"""Единый интерфейс backend: STUB-прототип и реальная модель из ДЗ 6+7.

Оба варианта проходят одни и те же тесты управления процессом и HTTP:
сервис знает только атрибуты loaded_id / runtime_sha256 / calls и метод
generate(prompt, max_new_tokens) -> dict. Реальный загрузчик подключается
без изменения HTTP-контракта.
"""
import json
import time
from pathlib import Path

import release


class BackendError(RuntimeError):
    """Артефакт не соответствует manifest: сервис не имеет права подняться."""


def _verify_files(hashes: dict, root: Path) -> None:
    if not hashes:
        raise BackendError("manifest без file_hashes: нечего проверять")
    for rel, expected in sorted(hashes.items()):
        path = Path(root) / rel
        if not path.is_file():
            raise BackendError(f"Файл артефакта отсутствует: {rel}")
        actual = release.file_sha256(path)
        if actual != expected:
            raise BackendError(f"Хеш файла {rel} не совпадает: {actual} против {expected}")


def verify_artifact_files(manifest: dict, root) -> None:
    """Пересчитать хеши файлов артефакта перед загрузкой.

    Раздел C: сервис не подменяет принятый manifest случайным конфигом —
    файлы должны совпадать с тем, что прошло допуск. manifest.file_hashes
    для адаптера включает веса и adapter_config; для базовой модели —
    конфиги и токенизатор её снапшота, веса закреплены ревизией.
    """
    _verify_files(manifest.get("file_hashes") or {}, root)


class StubBackend:
    """STUB ONLY: имитатор без модели, сети и пакетов. Не реальный ответ."""

    kind = "stub"

    def __init__(self, manifest: dict, workspace: Path):
        verify_artifact_files(manifest, workspace)
        self.manifest = manifest
        self.loaded_id = manifest["model_id"]
        self.runtime_sha256 = manifest["runtime_sha256"]
        self.calls = 0

    def load(self) -> None:
        time.sleep(0.01)  # имитация затрат на подъём

    def generate(self, prompt: str, max_new_tokens: int) -> dict:
        self.calls += 1
        started = time.perf_counter()
        time.sleep(0.01)
        return {"answer": f"STUB ONLY: {self.loaded_id}", "output_tokens": 1,
                "generation_seconds": time.perf_counter() - started}


class TransformersBackend:
    """Реальный загрузчик из ДЗ 6+7: Qwen3-0.6B (+ опционально адаптер PEFT).

    Условия совпадают с оценкой ДЗ 6+7: CPU, float32, точная ревизия базы,
    тот же шаблон чата и системная инструкция, do_sample=False.
    """

    kind = "transformers"

    def __init__(self, manifest: dict, workspace: Path, system_instruction: str = None):
        self.manifest = manifest
        self.loaded_id = manifest["model_id"]
        self.runtime_sha256 = manifest["runtime_sha256"]
        self.calls = 0
        runtime = manifest["runtime"]
        self.generation = dict(runtime.get("generation", {}))
        self.max_new_tokens_limit = int(self.generation.get("max_new_tokens", 32))
        self.system_instruction = system_instruction
        if self.system_instruction is not None:
            import hashlib
            actual = hashlib.sha256(system_instruction.encode("utf-8")).hexdigest()
            expected = self.generation.get("system_instruction_sha256")
            if expected and actual != expected:
                raise BackendError(f"Системная инструкция не совпадает с manifest: "
                                   f"{actual} против {expected}")

        uri = manifest["artifact_uri"]
        if uri.startswith("adapter-dir:"):
            # file_hashes кандидата — файлы адаптера; снапшот базы отдельной
            # ревизией уже закреплён, повторная проверка по нему неверна.
            self.repo, self.adapter_dir = _base_repo(manifest), Path(uri.split(":", 1)[1])
            _verify_files(manifest.get("file_hashes") or {}, self.adapter_dir)
            self.snapshot = _cached_snapshot(self.repo, runtime["base_revision"])
        elif uri.startswith("hf:"):
            self.repo, self.adapter_dir = uri[3:].split("@", 1)[0], None
            self.snapshot = _cached_snapshot(self.repo, runtime["base_revision"])
            verify_artifact_files(manifest, self.snapshot)
        else:
            raise BackendError(f"Неизвестная схема artifact_uri: {uri!r}")

        from transformers import AutoModelForCausalLM, AutoTokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(self.repo, revision=runtime["base_revision"])
        model = AutoModelForCausalLM.from_pretrained(
            self.repo, revision=runtime["base_revision"], dtype="float32")
        if self.adapter_dir is not None:
            from peft import PeftModel
            model = PeftModel.from_pretrained(model, str(self.adapter_dir))
            model = model.merge_and_unload()
        model.eval()
        self.model = model

    def load(self) -> None:
        # Тяжёлая работа сделана в __init__; здесь прогрев одним коротким
        # прогоном, чтобы первый реальный запрос не включал затраты подъёма.
        self.generate("Прогрев", 1)

    def generate(self, prompt: str, max_new_tokens: int) -> dict:
        import torch

        self.calls += 1
        limit = min(int(max_new_tokens), self.max_new_tokens_limit)
        messages = ([{"role": "system", "content": self.system_instruction}]
                    if self.system_instruction else [])
        messages.append({"role": "user", "content": prompt})
        encoded = self.tokenizer.apply_chat_template(
            messages, add_generation_prompt=True, return_tensors="pt",
            enable_thinking=False)
        # transformers 5.x возвращает BatchEncoding, а не тензор: берём input_ids.
        inputs = encoded.input_ids if not torch.is_tensor(encoded) else encoded
        started = time.perf_counter()
        with torch.no_grad():
            out = self.model.generate(
                inputs, max_new_tokens=limit,
                do_sample=bool(self.generation.get("do_sample", False)),
                pad_token_id=self.tokenizer.pad_token_id)
        text = self.tokenizer.decode(out[0][inputs.shape[1]:], skip_special_tokens=True)
        return {"answer": text.strip(),
                "output_tokens": int(out.shape[1] - inputs.shape[1]),
                "generation_seconds": time.perf_counter() - started}


def _base_repo(manifest: dict) -> str:
    base = manifest.get("base_model_repo")
    if base:
        return base
    raise BackendError("Для адаптера укажите manifest.base_model_repo")


def _cached_snapshot(repo: str, revision: str) -> Path:
    """Каталог снапшота базы в кеше HF: без сети, строго по ревизии."""
    from huggingface_hub import snapshot_download
    try:
        return Path(snapshot_download(repo_id=repo, revision=revision, local_files_only=True))
    except Exception as e:  # noqa: BLE001 — любая причина отсутствия кеша фатальна для загрузки
        raise BackendError(f"База {repo}@{revision[:12]} недоступна в кеше HF: {e}") from e


def load_manifest(path) -> dict:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    if manifest.get("runtime_sha256") != release.runtime_sha256(manifest.get("runtime", {})):
        raise BackendError("runtime_sha256 принятого manifest не сходится — файл испорчен")
    return manifest


def build_backend(spec: str, manifest: dict, workspace: Path, system_instruction=None):
    if spec == "stub":
        return StubBackend(manifest, workspace)
    if spec == "transformers":
        return TransformersBackend(manifest, workspace, system_instruction)
    raise ValueError(f"Неизвестный backend: {spec}")

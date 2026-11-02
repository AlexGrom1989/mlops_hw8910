"""Сборка РЕАЛЬНОГО выпуска из артефактов ДЗ 6+7 (без нового обучения).

Что делает:
  1. Строит manifest принятой baseline: точная ревизия базы из кеша HF,
     хеши файлов снапшота, runtime оценки (инструкция, шаблон, генерация).
  2. Строит manifest кандидата (адаптер my-qwen-adapter@1, run 0f060a1f…):
     хеши весов и adapter_config по закреплённому списку.
  3. Через gate.admit вызывает правило допуска ДЗ 6+7: кандидат против
     baseline — живой прогон quality_gate на исходных отчётах. Ожидаемый
     и фактический исход: ОТКАЗ (регрессия accuracy 0.2333 < 0.3000),
     отчёт отказа сохраняется, кандидат не публикуется.
  4. Публикует baseline в real/accepted.json (правило начального принятия).

Чтение готового training run — не повторное обучение: sweep из шести
запусков и полная оценка не повторяются, используются сохранённые
результаты ДЗ 6+7 (gate/reports/, копии с контрольными суммами).

Запуск:  .venv/bin/python -m gate.build_real_release
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import release
from gate import admit

ROOT = Path(__file__).resolve().parent.parent
REAL = ROOT / "real"
BASE_REPO = "Qwen/Qwen3-0.6B"
BASE_REVISION = "c1899de289a04d12100db370d81485cdf75e47ca"
# Закреплённый список файлов артефакта-адаптера (CONTRACTS.txt: веса + adapter_config).
ADAPTER_FILES = ["adapter_config.json", "adapter_model.safetensors"]
# Файлы базовой модели, входящие в file_hashes manifest. Веса базы закреплены
# точной ревизией (BASE_REVISION) и из кеша не перекачиваются.
BASE_SMALL_FILES = ["config.json", "generation_config.json", "tokenizer.json",
                    "tokenizer_config.json", "vocab.json", "merges.txt",
                    "special_tokens_map.json", "added_tokens.json", "chat_template.jinja"]


def snapshot_dir() -> Path:
    from huggingface_hub import snapshot_download
    return Path(snapshot_download(repo_id=BASE_REPO, revision=BASE_REVISION, local_files_only=True))


def hashes_of(root: Path, names) -> dict:
    return {name: release.file_sha256(root / name) for name in names if (root / name).is_file()}


def template_sha256(snapshot: Path) -> str:
    """Хеш шаблона чата: строка chat_template из конфига токенизатора.

    Именно эта строка подставляется apply_chat_template при оценке и в API,
    поэтому её хеш — часть runtime: смена шаблона меняет условия генерации.
    """
    config = json.loads((snapshot / "tokenizer_config.json").read_text(encoding="utf-8"))
    template = config.get("chat_template")
    if template is None and (snapshot / "chat_template.jinja").is_file():
        template = (snapshot / "chat_template.jinja").read_text(encoding="utf-8")
    if template is None:
        raise SystemExit("Шаблон чата не найден в снапшоте базы")
    return hashlib.sha256(template.encode("utf-8")).hexdigest()


def build_runtime(snapshot: Path, params: dict, base_revision: str = BASE_REVISION) -> dict:
    tokenizer_files = ["tokenizer.json", "tokenizer_config.json", "vocab.json",
                       "merges.txt", "special_tokens_map.json", "added_tokens.json",
                       "chat_template.jinja"]
    tokenizer_hashes = hashes_of(snapshot, tokenizer_files)
    return {
        "base_revision": base_revision,
        # Хеш токенизатора = канонический хеш словаря хешей его файлов.
        "tokenizer_sha256": release.digest(release.canonical(tokenizer_hashes)),
        "template_sha256": template_sha256(snapshot),
        "generation": {
            "system_instruction_sha256": hashlib.sha256(
                params["system"].encode("utf-8")).hexdigest(),
            "max_new_tokens": params["max_new_tokens"],
            "do_sample": False,
            "enable_thinking": params.get("enable_thinking", False),
        },
        "dtype": "float32",
    }


def build_baseline_manifest(snapshot: Path, baseline_eval: dict, params: dict) -> dict:
    runtime = build_runtime(snapshot, params)
    file_hashes = hashes_of(snapshot, BASE_SMALL_FILES)
    return {
        "schema_version": 1,
        "model_id": "baseline-qwen3-0.6b",
        "artifact_uri": f"hf:{BASE_REPO}@{BASE_REVISION}",
        "model_sha256": release.artifact_sha256(file_hashes),
        "protocol_sha256": baseline_eval["protocol_sha256"],
        "golden_sha256": baseline_eval["golden_sha256"],
        "runtime_sha256": release.runtime_sha256(runtime),
        "runtime": runtime,
        "file_hashes": file_hashes,
        "weights_pinned_by": "base_revision: веса базы закреплены точной ревизией HF, "
                             "file_hashes покрывает конфиги и токенизатор снапшота",
        "base_model_repo": BASE_REPO,
        "origin": {
            "kind": "base", "name": BASE_REPO, "revision": BASE_REVISION,
            "hw": "ДЗ 6+7: зафиксированная база оценки (docs/conclusion.md той работы)",
        },
    }


def build_candidate_manifest(adapter: Path, candidate_eval: dict, params: dict,
                             snapshot: Path) -> dict:
    runtime = build_runtime(snapshot, params)
    file_hashes = hashes_of(adapter, ADAPTER_FILES)
    if set(file_hashes) != set(ADAPTER_FILES):
        raise SystemExit(f"В каталоге адаптера нет файлов из закреплённого списка: "
                         f"{sorted(set(ADAPTER_FILES) - set(file_hashes))}")
    return {
        "schema_version": 1,
        "model_id": "candidate-my-qwen-adapter-1",
        "artifact_uri": f"adapter-dir:{adapter.resolve()}",
        "model_sha256": release.artifact_sha256(file_hashes),
        "protocol_sha256": candidate_eval["protocol_sha256"],
        "golden_sha256": candidate_eval["golden_sha256"],
        "runtime_sha256": release.runtime_sha256(runtime),
        "runtime": runtime,
        "file_hashes": file_hashes,
        "base_model_repo": BASE_REPO,
        "origin": {
            "kind": "adapter", "name": "my-qwen-adapter", "version": "1",
            "mlflow_run_id": "0f060a1fb67447948f2c4243d9abd311",
            "uri": "models:/my-qwen-adapter/1",
            "sweep_candidate": "all-r8-high (r=8, lr=3e-4, 7 проекций), ДЗ 6+7",
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--adapter-dir", type=Path, default=REAL / "adapter-candidate")
    parser.add_argument("--publish", action="store_true", default=True,
                        help="опубликовать baseline в real/accepted.json")
    args = parser.parse_args()

    REAL.mkdir(exist_ok=True)
    snapshot = snapshot_dir()
    baseline_eval, candidate_eval, params = admit.load_eval_reports()

    # Системная инструкция протокола — отдельным файлом: сервис проверит её хеш.
    instruction = REAL / "system-instruction.txt"
    instruction.write_text(params["system"], encoding="utf-8")

    baseline_manifest = build_baseline_manifest(snapshot, baseline_eval, params)
    candidate_manifest = build_candidate_manifest(args.adapter_dir, candidate_eval,
                                                  params, snapshot)
    (REAL / "baseline-manifest.json").write_text(
        json.dumps(baseline_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (REAL / "candidate-manifest.json").write_text(
        json.dumps(candidate_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    # --- Гейт: живой прогон правила ДЗ 6+7 и сохранённое решение.
    gate_outcome = admit.verify_saved_gate(baseline_eval, candidate_eval, params)
    candidate_report = admit.candidate_release_report(
        candidate_manifest, baseline_eval, candidate_eval, params, gate_outcome)
    (REAL / "candidate-release-report.json").write_text(
        json.dumps(candidate_report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"ГАТЕЙТ кандидата: passed={gate_outcome['passed']} "
          f"причина={gate_outcome.get('reason')}")

    # Кандидат не публикуется. Проверим, что публикация действительно закрыта:
    try:
        release.validate_release(candidate_manifest, candidate_report)
        raise SystemExit("ОШИБКА: отчёт отказа прошёл validate_release — барьер сломан")
    except ValueError as e:
        print(f"Барьер публикации кандидата работает: ValueError: {e}")

    # --- Baseline: правило начального принятия и публикация.
    baseline_report = admit.baseline_release_report(
        baseline_manifest, baseline_eval, params, gate_outcome)
    (REAL / "baseline-release-report.json").write_text(
        json.dumps(baseline_report, ensure_ascii=False, indent=2), encoding="utf-8")

    outcome = release.publish_release(
        baseline_manifest, baseline_report, REAL / "accepted.json",
        REAL / "releases.jsonl", attempts=REAL / "attempts.jsonl",
        decision={"rule": "initial-adoption", "hw": "8+9+10"})
    print(f"ПУБЛИКАЦИЯ baseline: {outcome['status']} release_id={outcome['release_id'][:12]}")

    # --- Конфиг стадии подготовки для flow --real (кеш по содержимому golden set).
    golden = REAL / "eval-golden.jsonl"
    if not golden.exists():
        src = Path("/home/alex/Desktop/mlops_bmstu/mlops26-hw6-7/hw67-broken/data/eval/golden.jsonl")
        if src.exists():
            golden.write_bytes(src.read_bytes())
    if golden.exists():
        prepare_config = {
            "tokenizer_revision": baseline_manifest["runtime"]["tokenizer_sha256"],
            "template_sha256": baseline_manifest["runtime"]["template_sha256"],
            "max_length": params["max_new_tokens"],
            "code_revision": "hw8910-prepare-v1",
            "note": "подготовка = проверка схемы golden set перед оценкой; ключ кеша "
                    "зависит от байтов golden и всех полей этого конфига",
        }
        (REAL / "prepare-config.json").write_text(
            json.dumps(prepare_config, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"golden set: {golden} ({len(golden.read_text().splitlines())} строк)")

    print("Готово: real/ содержит manifest'ы, отчёты допуска и принятый указатель")


if __name__ == "__main__":
    sys.exit(main())

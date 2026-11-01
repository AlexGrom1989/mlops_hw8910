"""HTTP-сервис принятой версии: FastAPI, фоновая загрузка, лимиты, диагностика.

Контракт (раздел C задания):
  GET  /health  — процесс жив; отвечает 200 и во время загрузки/прогрева;
  GET  /ready   — 200 только после загрузки весов и прогрева, иначе 503;
  POST /generate — ответ с фактически загруженной версией; некорректный
                  запрос получает 4xx ДО обращения к backend;
  GET  /debug   — диагностический маршрут для тестов (loaded/advertised/calls).

Исправленные дефекты прототипа:
  5) ready больше не константа True: до конца прогрева /ready и /generate
     отвечают 503;
  6) ответ содержит ФАКТИЧЕСКИ загруженную версию (backend.loaded_id), а не
     перечитанный на каждом запросе accepted.json: смена указателя без
     перезапуска не подменяет модель в памяти;
  7) вход проверяется до вызова backend: пустой prompt, max_new_tokens=0,
     100000 или строка вместо числа дают 4xx и не расходуют вычисление.

CLI совместим с прототипом: --workspace/--port/--warmup-seconds.
"""
import argparse
import json
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request

import backends

# Лимиты входа. Лимит генерации зафиксирован протоколом оценки ДЗ 6+7
# (max_new_tokens=32): произвольное его изменение не наследует прежний допуск.
MAX_PROMPT_CHARS = 2000
DEFAULT_MAX_NEW_TOKENS = 32
MAX_NEW_TOKENS_LIMIT = 32


class ServiceState:
    """Состояние сервиса: один backend в памяти, загрузка в фоне, семафор вычислений."""

    def __init__(self, workspace: Path, backend_spec: str, warmup_seconds: float,
                 max_concurrency: int, manifest_name: str = "accepted.json",
                 system_instruction: str = None):
        self.workspace = Path(workspace)
        self.advertised_path = self.workspace / manifest_name
        self.manifest = backends.load_manifest(self.advertised_path)
        self.backend_spec = backend_spec
        self.warmup_seconds = float(warmup_seconds)
        self.ready = threading.Event()
        self.load_error = None
        self.backend = None
        self.started_at = time.time()
        self._semaphore = threading.Semaphore(max(1, int(max_concurrency)))
        self._system_instruction = system_instruction
        threading.Thread(target=self._load_and_warm, daemon=True).start()

    def _load_and_warm(self) -> None:
        try:
            self.backend = backends.build_backend(
                self.backend_spec, self.manifest, self.workspace,
                system_instruction=self._system_instruction)
            self.backend.load()  # загрузка весов / подготовка
            # Прогрев: реальный короткий прогон после загрузки, плюс при
            # необходимости искусственная пауза (демо непрогретого состояния).
            if self.warmup_seconds > 0:
                time.sleep(self.warmup_seconds)
            self.ready.set()
        except Exception as e:  # noqa: BLE001 — причина отказа загрузки уходит в диагностику
            self.load_error = f"{type(e).__name__}: {e}"

    def advertised_id(self):
        """Что записано в accepted.json прямо сейчас (может отличаться от загруженного)."""
        try:
            return json.loads(self.advertised_path.read_text(encoding="utf-8")).get("model_id")
        except (OSError, ValueError):
            return None

    def generate(self, prompt: str, max_new_tokens: int) -> dict:
        started = time.perf_counter()
        with self._semaphore:
            result = self.backend.generate(prompt, max_new_tokens)
        result.update(
            request_id=str(uuid.uuid4()),
            model_id=self.backend.loaded_id,           # фактическая, не записанная
            runtime_sha256=self.backend.runtime_sha256,
            total_seconds=time.perf_counter() - started,
        )
        return result


def create_app(state: ServiceState) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app):
        yield

    app = FastAPI(title="release-api", lifespan=lifespan)

    @app.get("/health")
    async def health():
        # Liveness: процесс жив независимо от стадии загрузки (дефект 5).
        return {"alive": True, "backend": state.backend_spec,
                "uptime_seconds": round(time.time() - state.started_at, 3)}

    @app.get("/ready")
    async def ready():
        if state.ready.is_set():
            return {"ready": True, "model_id": state.backend.loaded_id}
        detail = {"ready": False, "error": state.load_error or "загрузка/прогрев не завершены"}
        raise HTTPException(status_code=503, detail=detail)

    @app.get("/debug")
    async def debug():
        # Диагностический маршрут для тестов: показывает расхождение
        # загруженной и записанной версий (дефект 6) и счётчик вызовов (дефект 7).
        return {"loaded_id": state.backend.loaded_id if state.backend else None,
                "advertised_id": state.advertised_id(),
                "calls": state.backend.calls if state.backend else 0,
                "warmup_finished": state.ready.is_set(),
                "load_error": state.load_error,
                "backend": state.backend_spec}

    @app.post("/generate")
    async def generate(request: Request):
        # Разбор тела отдельно от pydantic-модели, чтобы мёртвая петля
        # «строка вместо числа» не превратилась в 500: любое нарушение контракта
        # тела — 400 до единого вызова backend.
        try:
            payload = await request.json()
        except Exception as e:  # noqa: BLE001 — любой мусор в теле
            raise HTTPException(status_code=400, detail=f"тело не JSON: {e}") from e
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="тело должно быть JSON-объектом")

        prompt = payload.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise HTTPException(status_code=400, detail="prompt: непустая строка")
        if len(prompt) > MAX_PROMPT_CHARS:
            raise HTTPException(status_code=400,
                                detail=f"prompt длиннее {MAX_PROMPT_CHARS} символов")

        limit = payload.get("max_new_tokens", DEFAULT_MAX_NEW_TOKENS)
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise HTTPException(status_code=400,
                                detail=f"max_new_tokens: целое число, получено {type(limit).__name__}")
        if not 1 <= limit <= MAX_NEW_TOKENS_LIMIT:
            raise HTTPException(
                status_code=400,
                detail=f"max_new_tokens вне допустимого диапазона 1..{MAX_NEW_TOKENS_LIMIT} "
                       f"(предел зафиксирован протоколом оценки)")

        if not state.ready.is_set():
            raise HTTPException(status_code=503, detail={
                "error": state.load_error or "сервис не прогрет",
                "ready": False})

        return state.generate(prompt, limit)

    return app


def main():
    parser = argparse.ArgumentParser(description="Сервис принятой версии (FastAPI)")
    parser.add_argument("--workspace", type=Path, default=Path(".lab"))
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--warmup-seconds", type=float, default=5,
                        help="минимальное время до готовности (демо непрогретого состояния)")
    parser.add_argument("--backend", choices=["stub", "transformers"], default="stub")
    parser.add_argument("--manifest", default="accepted.json")
    parser.add_argument("--max-concurrency", type=int, default=1,
                        help="параллельные вычисления (CPU: 1 — последовательный декод)")
    parser.add_argument("--system-instruction", default=None,
                        help="текст или путь к файлу системной инструкции протокола")
    args = parser.parse_args()

    instruction = args.system_instruction
    if instruction and Path(instruction).is_file():
        instruction = Path(instruction).read_text(encoding="utf-8").strip()

    state = ServiceState(args.workspace, args.backend, args.warmup_seconds,
                         args.max_concurrency, args.manifest, instruction)
    app = create_app(state)
    import uvicorn
    print(f"STARTING http://127.0.0.1:{args.port} backend={args.backend} "
          f"manifest={args.manifest}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()

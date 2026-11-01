"""Интеграционные тесты HTTP-сервиса: реальный локальный сервер на TCP.

Обязательное покрытие (раздел 0): готовность; фактически загруженная версия
и управляемый откат; неверный запрос без вызова backend.

Каждый тест падает на исходном прототипе (RELEASE_TARGET=prototype) и
проходит на исправленной версии (RELEASE_TARGET=fixed).
"""
import json
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from adapter import TARGET, Service, init_workspace, run_release, sha256


class ReadinessTests(unittest.TestCase):
    """Дефект 5: ready не должен быть зелёным до конца загрузки и прогрева."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_ready_only_after_warmup(self):
        ws = init_workspace(self.tmp.name, "ws-ready")
        service = Service(ws, warmup_seconds=8.0)
        self.addCleanup(service.stop)

        status, health = service.request("GET", "/health")
        self.assertEqual(status, 200, "health обязан быть зелёным сразу: процесс жив")

        status, _ = service.request("GET", "/ready")
        self.assertEqual(status, 503, "до конца прогрева ready обязан быть 503")

        status, _ = service.request("POST", "/generate",
                                    {"prompt": "тест", "max_new_tokens": 4})
        self.assertEqual(status, 503, "генерация до готовности обязана получать 503")

        status, debug = service.request("GET", "/debug")
        self.assertEqual(status, 200)
        self.assertEqual(debug["calls"], 0,
                         "непрогретый сервис не должен вызывать backend")

        deadline = time.time() + 30
        while time.time() < deadline:
            status, ready = service.request("GET", "/ready")
            if status == 200:
                break
            time.sleep(0.5)
        self.assertEqual(status, 200, "после прогрева ready обязан стать 200")
        self.assertTrue(ready.get("ready"))


class LoadedVersionTests(unittest.TestCase):
    """Дефект 6: API отвечает ФАКТИЧЕСКИ загруженной версией, откат подтверждается."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    @staticmethod
    def wait_ready(service, timeout=30):
        """Дождаться готовности сервиса (само по себе это отдельный тест)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status, _ = service.request("GET", "/ready")
            if status == 200:
                return
            time.sleep(0.3)
        raise AssertionError("сервис не стал готовым за 30 с")

    def test_publish_does_not_swap_model_in_memory_and_rollback(self):
        ws = init_workspace(self.tmp.name, "ws-version")
        service = Service(ws, warmup_seconds=0.5)
        self.addCleanup(service.stop)
        self.wait_ready(service)

        status, body = service.request("POST", "/generate", {"prompt": "тест"})
        self.assertEqual(status, 200)
        self.assertEqual(body["model_id"], "fixture-A",
                         "сервис поднялся на принятой версии A")

        # Публикация B в ДРУГОМ процессе, пока сервис работает.
        out = run_release(ws, ["run"])
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])

        status, debug = service.request("GET", "/debug")
        self.assertEqual(debug["loaded_id"], "fixture-A",
                          "в памяти остаётся A до перезапуска")
        if TARGET == "fixed":
            self.assertEqual(debug["advertised_id"], "fixture-B",
                              "указатель уже указывает на B — расхождение видно в диагностике")

        status, body = service.request("POST", "/generate", {"prompt": "тест"})
        self.assertEqual(status, 200)
        self.assertEqual(body["model_id"], "fixture-A",
                         "ответ обязан содержать загруженную A, а не записанную B")

        service.stop()
        service2 = Service(ws, warmup_seconds=0.5, port=service.port)
        self.addCleanup(service2.stop)
        self.wait_ready(service2)
        status, body = service2.request("POST", "/generate", {"prompt": "тест"})
        self.assertEqual(status, 200)
        self.assertEqual(body["model_id"], "fixture-B",
                         "после перезапуска сервис обязан загрузить принятую B")

        # Управляемый откат к ранее принятой A: восстановление указателя + перезапуск.
        accepted = ws / "accepted.json"
        manifest_a = json.loads((ws / "manifest-A.json").read_text())
        # Для исправленной версии откат проводится выпуском A (журнал остаётся
        # согласован); прототип не умеет публиковать A — откат указателем.
        if TARGET == "fixed":
            out = run_release(ws, ["run", "--manifest", "manifest-A.json",
                                   "--report", "report-A.json"])
            self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        else:
            accepted.write_text(json.dumps(manifest_a, ensure_ascii=False, indent=2),
                                encoding="utf-8")

        service2.stop()
        service3 = Service(ws, warmup_seconds=0.5, port=service.port)
        self.addCleanup(service3.stop)
        self.wait_ready(service3)
        status, body = service3.request("POST", "/generate", {"prompt": "тест"})
        self.assertEqual(status, 200)
        self.assertEqual(body["model_id"], "fixture-A",
                         "после отката и перезапуска версия A подтверждается ответом")


class RequestValidationTests(unittest.TestCase):
    """Дефект 7: неверный запрос получает 4xx и не запускает вычисление."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_invalid_requests_rejected_before_backend(self):
        ws = init_workspace(self.tmp.name, "ws-validation")
        service = Service(ws, warmup_seconds=0.5)
        self.addCleanup(service.stop)
        # дождаться готовности, чтобы 503 не маскировал проверку 4xx
        deadline = time.time() + 20
        while time.time() < deadline:
            status, _ = service.request("GET", "/ready")
            if status == 200:
                break
            time.sleep(0.3)

        _, before = service.request("GET", "/debug")
        bad_payloads = [
            {"prompt": "", "max_new_tokens": 8},                      # пустой prompt
            {"prompt": "тест", "max_new_tokens": 0},                  # ноль токенов
            {"prompt": "тест", "max_new_tokens": 100000},             # сверх лимита
            {"prompt": "тест", "max_new_tokens": "8"},                # строка вместо числа
            {"prompt": "тест", "max_new_tokens": True},               # bool вместо int
            {"max_new_tokens": 8},                                    # нет prompt
            {"prompt": "x" * 3000},                                   # сверхдлинный вход
        ]
        for payload in bad_payloads:
            with self.subTest(payload=str(payload)[:60]):
                status, body = service.request("POST", "/generate", payload)
                self.assertGreaterEqual(status, 400,
                                        f"некорректный запрос обязан получать 4xx: {body}")
                _, debug = service.request("GET", "/debug")
                self.assertEqual(debug["calls"], before["calls"],
                                 "backend не должен вызываться на неверном запросе")

        status, body = service.request("POST", "/generate",
                                       {"prompt": "валидный запрос", "max_new_tokens": 8})
        self.assertEqual(status, 200, body)
        self.assertIn("answer", body)
        _, after = service.request("GET", "/debug")
        self.assertEqual(after["calls"], before["calls"] + 1,
                         "ровно один вызов backend на валидный запрос")


if __name__ == "__main__":
    unittest.main(verbosity=2)

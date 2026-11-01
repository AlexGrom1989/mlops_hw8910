"""Интеграционные тесты процесса выпуска: subprocess с отдельным рабочим каталогом.

Обязательное покрытие (раздел 0 задания): отказ и несовместимый отчёт;
сбой/восстановление; изменение входа; повтор выпуска; восстановление
согласованности указателя и журнала.

Каждый тест падает на исходном прототипе (RELEASE_TARGET=prototype) и
проходит на исправленной версии (RELEASE_TARGET=fixed).
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import adapter
from adapter import TARGET, init_workspace, release_events, run_release, sha256


class ReleaseProcessTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    # --- дефект 2: неполный и чужой отчёт не должны менять принятую версию ---

    def test_incomplete_and_foreign_report_rejected(self):
        """Неполный отчёт и отчёт чужой модели закрывают выпуск, A остаётся принятым."""
        ws = init_workspace(self.tmp.name, "ws-defect2")
        before = sha256(ws / "accepted.json")

        out = run_release(ws, ["run", "--report", "report-incomplete.json"])
        self.assertNotEqual(out.returncode, 0,
                            "неполный отчёт (status=running, passed='false' строкой) "
                            "обязан ломать выпуск")
        self.assertEqual(sha256(ws / "accepted.json"), before,
                         "указатель изменился после неполного отчёта")

        out = run_release(ws, ["run", "--report", "report-A.json"])
        self.assertNotEqual(out.returncode, 0,
                             "отчёт модели A для кандидата B — чужой отчёт")
        self.assertEqual(sha256(ws / "accepted.json"), before,
                         "указатель изменился после чужого отчёта")

    def test_rejected_report_preserves_accepted(self):
        """Честный отказ гейта (passed=false) сохраняет прежнюю версию."""
        ws = init_workspace(self.tmp.name, "ws-reject")
        before = sha256(ws / "accepted.json")
        out = run_release(ws, ["run", "--report", "report-reject.json"])
        self.assertNotEqual(out.returncode, 0)
        self.assertEqual(sha256(ws / "accepted.json"), before)

    # --- дефект 1 + восстановление: кеш подготовки по содержимому ---

    def test_crash_recovery_and_cache_invalidation(self):
        """Сбой после подготовки → повтор берёт кеш; изменение входа/конфига → пересчёт."""
        marker = f"Первый пример\nВторой пример\nуникальный маркер {id(self):x}\n"
        ws = init_workspace(self.tmp.name, "ws-cache", unique_input=marker)

        crash = run_release(ws, ["run", "--fail-after-prepare"])
        self.assertNotEqual(crash.returncode, 0, "инъекция сбоя обязана ломать запуск")
        self.assertEqual(sha256(ws / "accepted.json"), sha256(ws / "manifest-A.json"),
                         "сбой после подготовки не должен трогать принятую версию")

        recovered = run_release(ws, ["run"])
        self.assertEqual(recovered.returncode, 0, recovered.stderr[-2000:])
        combined = recovered.stdout + recovered.stderr
        self.assertIn("PREPARED", crash.stdout + crash.stderr,
                      "первый запуск обязан реально выполнить подготовку")
        self.assertTrue("CACHE_HIT" in combined or "Cached" in combined,
                        "повтор в новом процессе обязан взять сохранённый результат")

        # Изменение байтов входа по тому же пути — прежний кеш недействителен.
        (ws / "input.txt").write_text(marker + "изменившиеся данные\n", encoding="utf-8")
        out = run_release(ws, ["run"])
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        self.assertIn("PREPARED", out.stdout + out.stderr,
                      "изменённый вход обязан готовиться заново, а не брать кеш")

        # Изменение конфигурации подготовки — тоже недействительно.
        config = json.loads((ws / "config.json").read_text())
        config["tokenizer_revision"] = "r2-другая-ревизия"
        (ws / "config.json").write_text(json.dumps(config), encoding="utf-8")
        out = run_release(ws, ["run"])
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        self.assertIn("PREPARED", out.stdout + out.stderr,
                      "изменённый tokenizer_revision обязан менять ключ кеша")

    # --- дефект 4: повтор выпуска не создаёт вторую публикацию ---

    def test_repeat_release_single_event(self):
        """Два одинаковых успешных выпуска — одно событие фактического выпуска."""
        ws = init_workspace(self.tmp.name, "ws-repeat", unique_input=f"повтор {id(self):x}\n")
        first = run_release(ws, ["run"])
        self.assertEqual(first.returncode, 0, first.stderr[-2000:])
        after_first = release_events(ws)
        self.assertEqual(after_first.count("fixture-B"), 1, f"события: {after_first}")

        second = run_release(ws, ["run"])
        self.assertEqual(second.returncode, 0, second.stderr[-2000:])
        after_second = release_events(ws)
        self.assertEqual(after_second.count("fixture-B"), 1,
                         f"повтор выпуска размножил публикации: {after_second}")

        if TARGET == "fixed":
            # Попытки и события разделены: две попытки, одно событие.
            attempts = [json.loads(line) for line in
                        (ws / "attempts.jsonl").read_text().splitlines() if line.strip()]
            self.assertEqual(len(attempts), 2)
            self.assertEqual([a["action"] for a in attempts], ["publish", "noop"])

    # --- расширение дефекта 4: сбой между указателем и журналом ---

    @unittest.skipUnless(TARGET == "fixed", "точка сбоя есть только в исправленной версии")
    def test_crash_between_pointer_and_journal_recovers(self):
        """Сбой после замены указателя: повтор восстанавливает журнал без дубля."""
        ws = init_workspace(self.tmp.name, "ws-crash-pointer",
                            unique_input=f"краш {id(self):x}\n")
        crash = run_release(ws, ["run"], extra_env={"RELEASE_CRASH_AFTER_POINTER": "1"})
        self.assertNotEqual(crash.returncode, 0)

        pointer = json.loads((ws / "accepted.json").read_text())
        rid = pointer["release_id"]
        events = [json.loads(line) for line in
                  (ws / "releases.jsonl").read_text().splitlines()] \
            if (ws / "releases.jsonl").exists() else []
        self.assertFalse(any(e["release_id"] == rid for e in events),
                          "инъекция обязана оставить журнал без события")
        pointer_bytes = (ws / "accepted.json").read_bytes()

        out = run_release(ws, ["run"])
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        events = [json.loads(line) for line in
                  (ws / "releases.jsonl").read_text().splitlines()]
        matching = [e for e in events if e["release_id"] == rid]
        self.assertEqual(len(matching), 1, f"дублирование события: {matching}")
        self.assertTrue(matching[0].get("recovered"),
                        "восстановленное событие должно быть помечено")
        self.assertEqual((ws / "accepted.json").read_bytes(), pointer_bytes,
                         "восстановление не должно трогать уже записанный указатель")


if __name__ == "__main__":
    unittest.main(verbosity=2)

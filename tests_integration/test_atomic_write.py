"""Дефект 3: сбой записи не должен портить прежний указатель.

Красный/зелёный прогон по RELEASE_TARGET. Способ имитации сбоя зависит от
того, КАК версия пишет файл — в этом и есть исправление: прототип пишет
в целевой файл напрямую (обрыв записи оставляет в нём половину данных),
исправленная версия пишет во временный файл и меняет указатель одной
атомарной операцией os.replace.
"""
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from adapter import ROOT, TARGET


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def release_module():
    path = ROOT / "prototype" / "release.py" if TARGET == "prototype" else ROOT / "release.py"
    return load_module(path, f"release_{TARGET}")


def fixtures():
    """Минимальные согласованные manifest/report (валидны по контракту)."""
    import hashlib
    runtime = {"base_revision": "fixed-revision", "tokenizer_sha256": "a" * 64,
               "template_sha256": "b" * 64,
               "generation": {"max_new_tokens": 32, "do_sample": False},
               "dtype": "float32"}
    digest = hashlib.sha256(json.dumps(
        runtime, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    manifest = {"schema_version": 1, "model_id": "training-run-A",
                "model_sha256": "c" * 64, "protocol_sha256": "d" * 64,
                "golden_sha256": "e" * 64, "runtime_sha256": digest,
                "runtime": runtime, "artifact_uri": "models:/my-model/1"}
    report = {"status": "complete", "passed": True, "evaluation_run_id": "eval-A",
              "model_id": manifest["model_id"],
              **{k: manifest[k] for k in ["model_sha256", "protocol_sha256",
                                          "golden_sha256", "runtime_sha256"]},
              "metrics": {"accuracy": .8, "format_rate": 1.0}}
    return manifest, report


class AtomicWriteTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.target = self.root / "accepted.json"
        self.target.write_text('{"previous": true}\n', encoding="utf-8")
        self.before = self.target.read_bytes()
        self.manifest, self.report = fixtures()
        self.module = release_module()

    def test_failed_write_preserves_previous_bytes(self):
        """Обрыв записи посреди publish: прежние байты обязаны уцелеть."""
        if TARGET == "prototype":
            # Прототип пишет напрямую в целевой файл — рвём запись посреди данных.
            real_write = Path.write_text

            def torn_write(path, data, *args, **kwargs):
                if path.name == "accepted.json":
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(data[: len(data) // 2])
                        f.flush()
                    raise OSError("имитация обрыва записи посреди файла")
                return real_write(path, data, *args, **kwargs)

            with patch.object(Path, "write_text", torn_write):
                with self.assertRaises(OSError):
                    self.module.publish(self.manifest, self.report, self.target)
        else:
            # Исправленная версия пишет во временный файл — имитируем сбой на замене.
            with patch("os.replace", side_effect=OSError("диск недоступен")):
                with self.assertRaises(OSError):
                    self.module.publish(self.manifest, self.report, self.target)
            self.assertEqual(list(self.root.glob("*.tmp")), [],
                             "временный файл обязан удаляться после сбоя")
        self.assertEqual(self.target.read_bytes(), self.before,
                         "сбой записи обязан сохранять прежний указатель побайтно")


if __name__ == "__main__":
    unittest.main(verbosity=2)

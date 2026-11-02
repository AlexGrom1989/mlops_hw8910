"""Проверка «новый адаптер → загружается и отвечает»: кандидат ДЗ 6+7.

Гейт отказал кандидату по качеству, но артефакт обязан быть пригодным к
загрузке: берём real/candidate-manifest.json, поднимаем TransformersBackend
(база по пиннингу + адаптер PEFT), проверяем хеши файлов, системную
инструкцию и генерируем два ответа. Рядом — те же промпты через baseline,
чтобы видно было разницу поведения, а не только факт загрузки.

Запуск: .venv/bin/python scripts/check_adapter_load.py 2>&1 | tee logs/adapter-load.log
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import backends  # noqa: E402

WS = ROOT / "real"


def main() -> int:
    instruction = (WS / "system-instruction.txt").read_text(encoding="utf-8")
    prompts = [
        "Обслуживание хорошее, но пришлось ждать официанта минут двадцать.",
        "Ужасное отношение, больше не приду.",
    ]
    out = {"checked_at": time.strftime("%Y-%m-%d %H:%M:%S"), "runs": {}}
    for label, manifest_name in (("candidate", "candidate-manifest.json"),
                                 ("baseline", "baseline-manifest.json")):
        manifest = backends.load_manifest(WS / manifest_name)
        started = time.perf_counter()
        backend = backends.build_backend("transformers", manifest, str(WS),
                                         system_instruction=instruction)
        load_seconds = round(time.perf_counter() - started, 1)
        answers = []
        for text in prompts:
            t0 = time.perf_counter()
            try:
                r = backend.generate(text, 32)
                r["seconds"] = round(time.perf_counter() - t0, 1)
                r["prompt"] = text
                answers.append(r)
            except backends.BackendError as e:
                answers.append({"prompt": text, "error": str(e),
                                "seconds": round(time.perf_counter() - t0, 1)})
        out["runs"][label] = {
            "model_id": manifest["model_id"],
            "artifact_uri": manifest["artifact_uri"],
            "load_seconds": load_seconds,
            "answers": answers,
        }
        print(f"[{label}] model_id={manifest['model_id']} загрузка {load_seconds} с", flush=True)
        for a in answers:
            print(f"  промпт: {a['prompt']}\n  ответ:  {a.get('answer') or a.get('error')} "
                  f"({a['seconds']} с, {a.get('output_tokens')} ток.)", flush=True)
    (ROOT / "logs" / "adapter-load.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    cand = out["runs"]["candidate"]["answers"]
    loaded = all("answer" in a for a in cand)
    print("ИТОГ:", "адаптер кандидата загружается и генерирует" if loaded
          else "ОШИБКА загрузки адаптера", flush=True)
    return 0 if loaded else 1


if __name__ == "__main__":
    raise SystemExit(main())

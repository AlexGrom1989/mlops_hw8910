"""Обвязка допуска: связывает manifest с реальной оценкой ДЗ 6+7.

Правило допуска здесь не изобретается заново: для кандидата вызывается
quality_gate из ДЗ 6+7 (gate/quality_gate.py, побайтовая копия
evaluation/solution.py той работы) на исходных отчётах baseline/candidate-eval.
Один val_loss или рукописное passed=true оценкой не считаются.

Для принятой baseline действует правило начального принятия: полный
завершённый eval без ошибок генерации, format_rate 1.0, хеши протокола и
golden set совпадают с manifest. Полный quality_gate к baseline неприменим
по построению (не с чем сравнивать — он сравнивает кандидата с baseline);
отказ кандидата фиксируется в том же отчёте.

Используется и flow.py (--real), и gate/build_real_release.py.
"""
import hashlib
import json
import sys
from pathlib import Path

import release

GATE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(GATE_DIR))
import quality_gate  # noqa: E402 — ДЗ 6+7, score/quality_gate


def _sha256_file(path) -> str:
    return release.file_sha256(path)


def load_eval_reports():
    """Исходные отчёты ДЗ 6+7 + контроль целостности копий."""
    reports = GATE_DIR / "reports"
    sums = {}
    for line in (reports / "SHA256SUMS.txt").read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        sums[name.strip()] = digest
    for name, expected in sums.items():
        actual = _sha256_file(reports / name)
        if actual != expected:
            raise ValueError(f"Копия отчёта {name} изменена: {actual} против {expected}")
    baseline = json.loads((reports / "baseline-eval.json").read_text(encoding="utf-8"))
    candidate = json.loads((reports / "candidate-eval.json").read_text(encoding="utf-8"))
    params = json.loads((reports / "eval-params.json").read_text(encoding="utf-8"))
    return baseline, candidate, params


def verify_saved_gate(baseline, candidate, params) -> dict:
    """Вызвать quality_gate ДЗ 6+7 и сверить с сохранённым решением.

    Возвращает живой результат гейта; сохранённый quality-gate.json служит
    контролем, что решение не переписано задним числом.
    """
    try:
        result = quality_gate.quality_gate(baseline, candidate, params["critical_slices"])
        live = {"passed": True, "result": result}
    except ValueError as e:
        live = {"passed": False, "reason": str(e)}
    saved = json.loads((GATE_DIR / "reports" / "quality-gate.json").read_text(encoding="utf-8"))
    if live["passed"] != saved["passed"]:
        raise ValueError(f"Живой прогон гейта расходится с сохранённым решением: "
                         f"{live['passed']} против {saved['passed']}")
    return live


def candidate_release_report(manifest: dict, baseline, candidate, params, gate_outcome: dict) -> dict:
    """Отчёт выпуска кандидата: отказ сохраняется, а не выбрасывается."""
    overall = candidate["overall"]
    return {
        "model_id": manifest["model_id"],
        "model_sha256": manifest["model_sha256"],
        "protocol_sha256": manifest["protocol_sha256"],
        "golden_sha256": manifest["golden_sha256"],
        "runtime_sha256": manifest["runtime_sha256"],
        "status": "complete",
        "passed": gate_outcome["passed"],  # False: гейт ДЗ 6+7 отказал
        "gate_reason": gate_outcome.get("reason"),
        "evaluation_run_id": candidate["evaluation_run_id"],
        "metrics": {"accuracy": overall["accuracy"], "format_rate": overall["format_rate"]},
        "slices": {name: block["accuracy"] for name, block in candidate["slices"].items()},
        "behavioral": {"n": candidate["behavioral"]["overall"]["n"],
                       "correct": candidate["behavioral"]["overall"]["correct"]},
        "provenance": {
            "baseline_eval": "gate/reports/baseline-eval.json",
            "candidate_eval": "gate/reports/candidate-eval.json",
            "candidate_eval_sha256": _sha256_file(GATE_DIR / "reports" / "candidate-eval.json"),
            "saved_decision": "gate/reports/quality-gate.json",
            "saved_decision_sha256": _sha256_file(GATE_DIR / "reports" / "quality-gate.json"),
            "critical_slices": params["critical_slices"],
        },
        "synthetic": False,
    }


def baseline_release_report(manifest: dict, baseline, params, gate_outcome: dict) -> dict:
    """Отчёт принятия baseline: полнота и целостность, не подмена гейта.

    passed=true означает «отчёт полный, завершённый и связанный с этим
    manifest» (правило начального принятия). Полный quality_gate гонялся
    для кандидата против этой baseline и отказал — это зафиксировано ниже
    и в candidate-release-report.json. Пороги не ослаблялись.
    """
    overall = baseline["overall"]
    reasons = []
    if baseline.get("status") != "complete":
        reasons.append("оценка не завершена")
    if overall.get("errors"):
        reasons.append(f"{overall['errors']} ошибок генерации")
    if abs(float(overall["format_rate"]) - 1.0) > 1e-12:
        reasons.append(f"format_rate={overall['format_rate']}")
    if baseline.get("protocol_sha256") != manifest["protocol_sha256"]:
        reasons.append("протокол оценки не совпадает с manifest")
    if baseline.get("golden_sha256") != manifest["golden_sha256"]:
        reasons.append("golden set не совпадает с manifest")
    if baseline.get("model", {}).get("revision") != manifest["runtime"]["base_revision"]:
        reasons.append("ревизия базы в отчёте не совпадает с manifest")
    if reasons:
        raise ValueError("Baseline не проходит правило начального принятия: " + "; ".join(reasons))

    return {
        "model_id": manifest["model_id"],
        "model_sha256": manifest["model_sha256"],
        "protocol_sha256": manifest["protocol_sha256"],
        "golden_sha256": manifest["golden_sha256"],
        "runtime_sha256": manifest["runtime_sha256"],
        "status": "complete",
        "passed": True,
        "decision_basis": "initial-adoption: полный завершённый eval ДЗ 6+7 без ошибок "
                          "генерации, format_rate=1.0, хеши протокола/golden/ревизии "
                          "совпадают с manifest; полный quality_gate применён к кандидату "
                          "против этой baseline и отказал — пороги не менялись",
        "evaluation_run_id": baseline["evaluation_run_id"],
        "metrics": {"accuracy": overall["accuracy"], "format_rate": overall["format_rate"]},
        "slices": {name: block["accuracy"] for name, block in baseline["slices"].items()},
        "behavioral": {"n": baseline["behavioral"]["overall"]["n"],
                       "correct": baseline["behavioral"]["overall"]["correct"]},
        "gate": {
            "rule": "hw6+7 quality_gate(candidate vs baseline), критический срез rating_low",
            "passed": gate_outcome["passed"],
            "reason": gate_outcome.get("reason"),
            "candidate": "my-qwen-adapter@1 (run 0f060a1fb674…)",
            "saved_decision_sha256": _sha256_file(GATE_DIR / "reports" / "quality-gate.json"),
        },
        "provenance": {
            "baseline_eval": "gate/reports/baseline-eval.json",
            "baseline_eval_sha256": _sha256_file(GATE_DIR / "reports" / "baseline-eval.json"),
            "raw_answers": "gate/reports/baseline-outputs.json",
            "raw_answers_file_sha256": _sha256_file(GATE_DIR / "reports" / "baseline-outputs.json"),
            "raw_answers_sha256_as_recorded": baseline.get("outputs_sha256"),
            "system_instruction": "real/system-instruction.txt",
        },
        "synthetic": False,
    }


def admit_real(manifest_path: str, report_out_path: str, real_root: str) -> dict:
    """Путь допуска для flow --real: проверить всё и отдать решение.

    Для кандидата: живой прогон quality_gate → passed=False → отчёт с отказом
    сохраняется, validate_release отклоняет, публикация не происходит.
    Для baseline: правило начального принятия → отчёт связывается с manifest.
    """
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    baseline, candidate, params = load_eval_reports()
    gate_outcome = verify_saved_gate(baseline, candidate, params)

    if manifest["model_id"].startswith("candidate-"):
        report = candidate_release_report(manifest, baseline, candidate, params, gate_outcome)
    else:
        report = baseline_release_report(manifest, baseline, params, gate_outcome)
    out = Path(report_out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"GATE OUTCOME model_id={manifest['model_id']} passed={report['passed']} "
          f"reason={report.get('gate_reason') or report.get('decision_basis', '')[:80]}",
          flush=True)

    # validate_release — барьер публикации: кандидат с passed=False падает здесь.
    release.validate_release(manifest, json.loads(out.read_text(encoding="utf-8")))
    return {"passed": True, "report_path": str(out), "report_sha256": _sha256_file(out),
            "gate": gate_outcome}

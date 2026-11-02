"""Реализуйте две функции. Контракт и примеры: EVALUATION.md и tests/test_eval.py."""
import json
import math

# Поля, которые считаются и для всего набора, и для каждого среза.
METRIC_KEYS = ("n", "correct", "valid", "errors", "accuracy", "format_rate")


def _parse(raw, choices):
    """Разобрать ответ модели по строгому контракту.

    Валидным считается только JSON-объект с ЕДИНСТВЕННЫМ полем answer, чьё
    значение — строка из choices. Всё остальное — невалидный формат:
    лишние поля, число вместо строки, метка не из choices, обёртка в
    ```json, пустая строка, `null` (валидный JSON, но не объект).

    Возвращает (parsed, valid). parsed — сама метка либо None: это то,
    что уходит в per_item и позволяет глазами разобрать, что ответила модель.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None, False
    try:
        value = json.loads(raw)
    except (ValueError, TypeError):
        return None, False
    if not isinstance(value, dict) or set(value) != {"answer"}:
        return None, False
    answer = value["answer"]
    # bool — подкласс int, но и не str; отдельная проверка не нужна.
    if not isinstance(answer, str) or answer not in choices:
        return None, False
    return answer, True


def _aggregate(items):
    """Свести список результатов в блок метрик.

    Знаменатель — ВСЕ задания: пустые ответы, неверный формат и аварии
    генерации остаются в n. Иначе достаточно уронить генерацию на трудных
    вопросах, чтобы accuracy выросла.
    """
    n = len(items)
    correct = sum(1 for x in items if x["correct"])
    valid = sum(1 for x in items if x["valid"])
    errors = sum(1 for x in items if x["error"])
    return {"n": n, "correct": correct, "valid": valid, "errors": errors,
            "accuracy": correct / n if n else 0.0,
            "format_rate": valid / n if n else 0.0}


def score(rows, outputs):
    """Верните overall, slices и per_item. Не удаляйте плохие ответы из знаменателя.

    Строгий ответ: JSON с единственным строковым полем answer из choices.
    outputs: [{id, raw, error, latency_seconds}, ...].
    В overall и каждом срезе: n, correct, valid, errors, accuracy, format_rate.
    В per_item: id, raw, parsed, valid, correct, error, slices.
    Несовпадающие/дублирующиеся id и пустой набор должны вызвать ValueError.
    """
    if not rows or not outputs:
        raise ValueError("Пустой набор заданий или ответов")
    row_ids = [r["id"] for r in rows]
    out_ids = [o["id"] for o in outputs]
    if len(set(row_ids)) != len(row_ids):
        raise ValueError("В заданиях повторяются id")
    if len(set(out_ids)) != len(out_ids):
        raise ValueError("В ответах повторяются id")
    if set(row_ids) != set(out_ids):
        missing = sorted(set(row_ids) - set(out_ids))
        extra = sorted(set(out_ids) - set(row_ids))
        raise ValueError(f"Ответы не соответствуют заданиям: нет {missing[:5]}, лишние {extra[:5]}")

    # Сопоставление строго по id: порядок ответов не обязан совпадать с порядком
    # заданий, и полагаться на него нельзя.
    by_id = {o["id"]: o for o in outputs}
    per_item, by_slice = [], {}
    for row in rows:
        out = by_id[row["id"]]
        error = out.get("error") or None
        parsed, valid = (None, False) if error else _parse(out.get("raw"), row["choices"])
        item = {"id": row["id"], "raw": out.get("raw"), "parsed": parsed, "valid": valid,
                "correct": bool(valid and parsed == row["expected"]), "error": error,
                "slices": list(row["slices"])}
        per_item.append(item)
        for name in item["slices"]:
            by_slice.setdefault(name, []).append(item)

    return {"overall": _aggregate(per_item),
            "slices": {name: _aggregate(items) for name, items in sorted(by_slice.items())},
            "per_item": per_item}


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _check_report(role, report):
    """Отчёт пригоден к сравнению: он полный, про конкретную модель и непротиворечивый."""
    if not isinstance(report, dict):
        raise ValueError(f"{role}: отчёт не словарь")
    for key in ("overall", "slices", "per_item", "behavioral", "model", "status"):
        if key not in report:
            raise ValueError(f"{role}: в отчёте нет поля {key}")
    if report["status"] != "complete":
        raise ValueError(f"{role}: отчёт не завершён (status={report['status']!r})")
    if not report["model"]:
        raise ValueError(f"{role}: в отчёте не указана модель")

    blocks = [("overall", report["overall"])]
    blocks += [(f"срез {name}", block) for name, block in report["slices"].items()]
    blocks += [("behavioral", report["behavioral"]["overall"])]
    for title, block in blocks:
        if not isinstance(block, dict) or any(k not in block for k in METRIC_KEYS):
            raise ValueError(f"{role}: в блоке «{title}» не хватает метрик")
        if any(not _finite(block[k]) for k in METRIC_KEYS):
            raise ValueError(f"{role}: в блоке «{title}» метрика missing/NaN/inf")
        n = block["n"]
        if n <= 0:
            raise ValueError(f"{role}: блок «{title}» пуст")
        if not 0 <= block["correct"] <= block["valid"] <= n:
            raise ValueError(f"{role}: в блоке «{title}» counts противоречивы")
        # Метрики обязаны соответствовать counts: иначе достаточно переписать
        # одно число в json, чтобы пройти гейт.
        if abs(block["accuracy"] - block["correct"] / n) > 1e-9:
            raise ValueError(f"{role}: accuracy блока «{title}» не соответствует counts")
        if abs(block["format_rate"] - block["valid"] / n) > 1e-9:
            raise ValueError(f"{role}: format_rate блока «{title}» не соответствует counts")
    if report["overall"]["errors"]:
        raise ValueError(f"{role}: {report['overall']['errors']} ошибок генерации — результат неполный")
    if report["behavioral"]["overall"]["errors"]:
        raise ValueError(f"{role}: ошибки генерации в behavioral-кейсах")
    if len(report["per_item"]) != report["overall"]["n"]:
        raise ValueError(f"{role}: per_item не соответствует n")


def quality_gate(baseline, candidate, critical_slices):
    """Верните {passed: True, ...} или вызовите ValueError с причиной отказа.

    Проверьте совместимость протокола/набора/baseline, метрики и counts,
    все id и срезы, общую accuracy/format_rate, критические срезы и behavior.
    Missing, NaN, пустой набор и ошибки генерации означают отказ.
    Пороги задаются до измерения; учебный допуск регрессии равен нулю.
    """
    for role, report in (("baseline", baseline), ("candidate", candidate)):
        _check_report(role, report)

    # 1. Сравнивать можно только измерения одного протокола на одном наборе
    # и с одной точкой отсчёта. Иначе «улучшение» может означать смену данных.
    for key in ("protocol_sha256", "golden_sha256", "baseline_spec_sha256"):
        a, b = baseline.get(key), candidate.get(key)
        if not a or not b or a != b:
            raise ValueError(f"Несовместимые отчёты: {key} различается ({a!r} против {b!r})")

    # 2. Размер и состав набора.
    if baseline["overall"]["n"] < 50:
        raise ValueError(f"Golden set меньше 50 строк: {baseline['overall']['n']}")
    if len(baseline["slices"]) < 3:
        raise ValueError(f"Нужны минимум три среза, есть {len(baseline['slices'])}")
    if [x["id"] for x in baseline["per_item"]] != [x["id"] for x in candidate["per_item"]]:
        raise ValueError("Отчёты посчитаны на разных наборах заданий")
    if set(baseline["slices"]) != set(candidate["slices"]):
        raise ValueError("Состав срезов различается")
    for name, block in baseline["slices"].items():
        if candidate["slices"][name]["n"] != block["n"]:
            raise ValueError(f"Размер среза {name} различается: {block['n']} против {candidate['slices'][name]['n']}")

    # 3. Общие метрики и критические срезы. Допуск ухудшения равен нулю.
    for key in ("accuracy", "format_rate"):
        before, after = baseline["overall"][key], candidate["overall"][key]
        if after < before:
            raise ValueError(f"Регрессия по {key}: {after:.4f} против {before:.4f}")
    missing = [name for name in critical_slices if name not in baseline["slices"]]
    if missing:
        raise ValueError(f"Критический срез отсутствует в отчёте: {missing}")
    for name in critical_slices:
        before, after = baseline["slices"][name]["accuracy"], candidate["slices"][name]["accuracy"]
        if after < before:
            raise ValueError(f"Регрессия в критическом срезе {name}: {after:.4f} против {before:.4f}")

    # 4. Behavioral: не статистика, а обязательные требования. Проходят все.
    behavioral = candidate["behavioral"]["overall"]
    if behavioral["n"] < 3:
        raise ValueError(f"Behavioral-кейсов меньше трёх: {behavioral['n']}")
    if behavioral["correct"] != behavioral["n"]:
        failed = [x["id"] for x in candidate["behavioral"]["per_item"] if not x["correct"]]
        raise ValueError(f"Не прошли обязательные behavioral-кейсы: {failed}")

    return {
        "passed": True,
        "protocol_sha256": candidate["protocol_sha256"],
        "golden_sha256": candidate["golden_sha256"],
        "critical_slices": list(critical_slices),
        "n": candidate["overall"]["n"],
        "overall": {key: {"baseline": baseline["overall"][key], "candidate": candidate["overall"][key]}
                    for key in ("accuracy", "format_rate")},
        "critical": {name: {"baseline": baseline["slices"][name]["accuracy"],
                            "candidate": candidate["slices"][name]["accuracy"],
                            "n": baseline["slices"][name]["n"]}
                     for name in critical_slices},
        "behavioral": {"n": behavioral["n"], "correct": behavioral["correct"]},
    }

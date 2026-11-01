"""Исправленное ядро выпуска: ключ кеша, проверка отчёта, атомарная публикация.

Контракты — CONTRACTS.txt. Только стандартная библиотека: приложенные быстрые
тесты должны работать без Prefect, FastAPI и весов модели.

Разделение ответственности:
  cache_key / validate_release / publish  — контракты из CONTRACTS.txt;
  release_id / append_event / publish_release — журнал выпуска поверх контракта:
  попытки и события фактической публикации разделены, повтор выпуска
  идемпотентен, сбой между указателем и журналом устраняется повтором.
"""
import hashlib
import json
import os
import re
import tempfile
import time
from pathlib import Path

HEX64 = re.compile(r"^[0-9a-f]{64}$")
# Хеш-поля manifest, которые отчёт обязан повторить для связи с решением.
BINDING_KEYS = ("model_sha256", "protocol_sha256", "golden_sha256", "runtime_sha256")


def canonical(value) -> bytes:
    """Канонический JSON: единый вид байтов для хешей и сравнений.

    Формат зафиксирован CONTRACTS.txt: sort_keys, компактные разделители,
    UTF-8, NaN/Inf запрещены (allow_nan=False падает на нечисловых значениях).
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha256(path) -> str:
    """SHA-256 содержимого файла: путь и mtime в хеш не входят."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def runtime_sha256(runtime: dict) -> str:
    """Хеш фактических условий выполнения: считаем из канонического runtime."""
    return digest(canonical(runtime))


def artifact_sha256(file_hashes: dict) -> str:
    """Хеш артефакта по закреплённому списку файлов: {путь: sha256} → sha256."""
    return digest(canonical(file_hashes))


# ---------------------------------------------------------------- кеш подготовки

def cache_key(data_path, config: dict) -> str:
    """Ключ кеша стадии подготовки: содержимое входа + все поля конфигурации.

    Дефект 1 прототипа: ключ считался из пути файла, поэтому смена байтов
    input.txt или любого поля config не меняла ключ, и подготовка навсегда
    оставалась «уже выполненной». Здесь ключ зависит только от хеша байтов
    входа и канонического вида конфигурации: одинаковые байты в разных путях
    дают одинаковый ключ, порядок ключей словаря не важен.
    """
    if not isinstance(config, dict):
        raise ValueError("config должен быть словарем")
    return digest(canonical({"data_sha256": file_sha256(data_path), "config": config}))


# ---------------------------------------------------------------- проверка отчёта

def _nonempty_str(value) -> bool:
    return isinstance(value, str) and bool(value)


def _valid_digest(value) -> bool:
    return isinstance(value, str) and bool(HEX64.match(value))


def _valid_metric(value) -> bool:
    """Метрика — конечное число от 0 до 1. bool и строки запрещены:
    True — это 1, а '0.8' — строка; подмена числа строкой не допускается."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return 0.0 <= float(value) <= 1.0


def validate_release(manifest: dict, report: dict) -> None:
    """Проверить целостность и полноту решения о выпуске или поднять ValueError.

    Дефект 2 прототипа: проверялась только истинность report['passed'], поэтому
    неполный отчёт (status='running', passed='false' строкой) и чужой отчёт
    (модель A для кандидата B) проходили в выпуск. Здесь проверяется всё, что
    перечислено в CONTRACTS.txt: схема manifest, настоящий boolean PASS,
    конечные метрики, совпадение модели и всех четырёх хешей, пересчёт
    runtime_sha256 из фактического runtime.
    """
    if not isinstance(manifest, dict):
        raise ValueError("manifest не словарь")
    if not isinstance(report, dict):
        raise ValueError("report не словарь")

    # --- manifest: схема, идентификаторы, хеши.
    if manifest.get("schema_version") != 1 or isinstance(manifest.get("schema_version"), bool):
        raise ValueError(f"manifest.schema_version должен быть int 1, получено {manifest.get('schema_version')!r}")
    if not _nonempty_str(manifest.get("model_id")):
        raise ValueError("manifest.model_id пуст или не строка")
    if not _nonempty_str(manifest.get("artifact_uri")):
        raise ValueError("manifest.artifact_uri пуст или не строка")
    for key in BINDING_KEYS:
        if not _valid_digest(manifest.get(key)):
            raise ValueError(f"manifest.{key} не SHA-256 (64 строчных hex)")
    if not isinstance(manifest.get("runtime"), dict) or not manifest["runtime"]:
        raise ValueError("manifest.runtime должен быть непустым словарем")
    if manifest.get("runtime_sha256") != runtime_sha256(manifest["runtime"]):
        raise ValueError("runtime_sha256 не соответствует содержимому runtime")
    if "file_hashes" in manifest:
        hashes = manifest["file_hashes"]
        if not isinstance(hashes, dict) or not hashes:
            raise ValueError("manifest.file_hashes должен быть непустым словарем")
        bad = [p for p, h in hashes.items() if not _nonempty_str(p) or not _valid_digest(h)]
        if bad:
            raise ValueError(f"file_hashes содержит недопустимые записи: {bad[:3]}")

    # --- report: статус, настоящий PASS, метрики, связь с manifest.
    if report.get("status") != "complete":
        raise ValueError(f"Отчёт не завершён (status={report.get('status')!r})")
    # Именно bool True: строка 'true'/'false' и 1 не проходят.
    passed = report.get("passed")
    if not isinstance(passed, bool):
        raise ValueError(f"passed должен быть boolean, получено {type(passed).__name__}")
    if not passed:
        raise ValueError("Кандидат отклонён правилом допуска")
    if not _nonempty_str(report.get("evaluation_run_id")):
        raise ValueError("report.evaluation_run_id пуст или отсутствует")
    if report.get("model_id") != manifest["model_id"]:
        raise ValueError(f"Отчёт принадлежит другой модели: {report.get('model_id')!r} против {manifest['model_id']!r}")
    for key in BINDING_KEYS:
        if report.get(key) != manifest[key]:
            raise ValueError(f"Отчёт не совпадает с manifest по {key}: {report.get(key)!r} против {manifest[key]!r}")

    metrics = report.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("report.metrics должен быть словарем")
    for key in ("accuracy", "format_rate"):
        if key not in metrics:
            raise ValueError(f"В metrics нет {key}")
        value = metrics[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{key} должна быть числом, получено {type(value).__name__}")
        if not _finite(value) or not 0.0 <= float(value) <= 1.0:
            raise ValueError(f"{key} не конечное число в [0,1]: {value!r}")


def _finite(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == value and abs(value) != float("inf")


# ---------------------------------------------------------------- атомарная запись

def _dump_bytes(manifest: dict) -> bytes:
    """Байты указателя: детерминированные, повтор записи даёт те же байты."""
    return json.dumps(manifest, sort_keys=True, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"


def publish(manifest: dict, report: dict, target: Path) -> None:
    """Атомарно опубликовать указатель accepted.json или оставить прежние байты.

    Дефект 3 прототипа: файл перезаписывался напрямую, сбой посреди записи
    оставал бы половину файла. Здесь: сначала полная проверка, затем запись
    во временный файл рядом с целевым, flush+fsync и os.replace — одна
    атомарная операция файловой системы. Любая ошибка удаляет временный файл
    и оставляет целевой побайтно неизменным.
    """
    validate_release(manifest, report)
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = _dump_bytes(manifest)
    if target.exists() and target.read_bytes() == payload:
        return  # идемпотентность: повтор той же публикации не трогает файл
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=target.name + ".", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _fsync_dir(target.parent)


def _fsync_dir(directory: Path) -> None:
    """Синхронизировать каталог, чтобы замена имени пережила сбой питания."""
    try:
        fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass  # на файловых системах без fsync каталога это не ошибка записи


# ---------------------------------------------------------------- журнал выпуска

def release_id(manifest: dict, report: dict) -> str:
    """Идентификатор выпуска: модель + runtime + связанное решение допуска.

    Дефект 4 прототипа: записи журнала различались только model_id, поэтому
    повтор выпуска создавал событие-дубликат, а та же модель с другим runtime
    или после новой оценки считалась бы «уже опубликованной». Здесь в ключ
    входят модель, все связующие хеши и evaluation_run_id решения: новая
    оценка или другой runtime дают новый выпуск, а буквальный повтор — нет.
    """
    validate_release(manifest, report)
    return digest(canonical({
        "model_id": manifest["model_id"],
        "artifact_uri": manifest["artifact_uri"],
        "model_sha256": manifest["model_sha256"],
        "protocol_sha256": manifest["protocol_sha256"],
        "golden_sha256": manifest["golden_sha256"],
        "runtime_sha256": manifest["runtime_sha256"],
        "evaluation_run_id": report["evaluation_run_id"],
        "passed": report["passed"],
    }))


def read_events(journal: Path) -> list:
    """Прочитать события фактического выпуска (по одной JSON-строке на выпуск)."""
    journal = Path(journal)
    if not journal.exists():
        return []
    events = []
    for line in journal.read_text(encoding="utf-8").splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


def append_event(journal: Path, event: dict) -> None:
    """Дописать событие с fsync: строка журнала либо записана целиком, либо нет."""
    journal = Path(journal)
    journal.parent.mkdir(parents=True, exist_ok=True)
    with journal.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _target_release_id(target: Path):
    """release_id уже записанного указателя; None, если файл не читается как manifest."""
    try:
        manifest = json.loads(Path(target).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(manifest, dict):
        return None
    rid = manifest.get("release_id")
    return rid if isinstance(rid, str) else None


def publish_release(manifest: dict, report: dict, target: Path, journal: Path,
                    attempts: Path = None, decision: dict = None) -> dict:
    """Полный выпуск: проверка → указатель → журнал, идемпотентно и с восстановлением.

    В отличие от publish() из контракта, связывает указатель с журналом событий:
      - каждый запуск пишется в журнал попыток (attempts.jsonl);
      - событие фактического выпуска (releases.jsonl) создаётся один раз
        на release_id: повтор выпуска — NOOP, а не вторая публикация;
      - если прошлый запуск упал между заменой указателя и записью журнала,
        повтор восстанавливает согласованность одним событием с пометкой
        recovered=true (двойное событие не создаётся).

    Точка сбоя для теста восстановления включается переменной окружения
    RELEASE_CRASH_AFTER_POINTER=1: указатель уже заменён, журнал ещё не записан.
    """
    target, journal = Path(target), Path(journal)
    rid = release_id(manifest, report)  # включает validate_release: отказ не трогает файлы
    record = {
        "release_id": rid,
        "model_id": manifest["model_id"],
        "evaluation_run_id": report["evaluation_run_id"],
        "decision": decision or {},
    }

    events = read_events(journal)
    known = {e.get("release_id") for e in events}
    target_rid = _target_release_id(target)
    # Восстановление: указатель уже указывает на этот выпуск, а события нет —
    # прошлый запуск оборвался после замены указателя. Добиваем одно событие.
    recovered = target_rid == rid and rid not in known
    noop = rid in known and target_rid == rid
    action = "recover" if recovered else ("noop" if noop else "publish")
    # Одна запись попытки на запуск — включая запуск, который сейчас упадёт.
    _log_attempt(attempts, rid, manifest, action)

    if action == "noop":
        return {"status": "noop", "release_id": rid, "events": len(known)}
    if target_rid != rid:
        _atomic_pointer(manifest, report, target, rid)

    if os.environ.get("RELEASE_CRASH_AFTER_POINTER") == "1":
        # Управляемый сбой точно между обновлением указателя и журналом:
        # указатель записан, событие не записано, процесс падает.
        raise RuntimeError("Инъекция сбоя: указатель обновлён, журнал не записан")

    if rid in known:
        # Событие уже есть, а указатель вернулся к этому выпуску (откат):
        # указатель восстановлен, второго события не создаём.
        return {"status": "noop", "release_id": rid, "events": len(known),
                "pointer": "restored"}

    event = dict(record)
    event.update(timestamp=time.time(), recovered=recovered)
    append_event(journal, event)
    return {"status": "recovered" if recovered else "published", "release_id": rid,
            "events": len(known) + 1}


def _atomic_pointer(manifest: dict, report: dict, target: Path, rid: str) -> None:
    """publish() контракта + release_id в теле указателя для связи с журналом."""
    enriched = dict(manifest)
    enriched["release_id"] = rid
    publish(enriched, report, target)


def _log_attempt(attempts, rid, manifest, action) -> None:
    """Журнал попыток: пишется всегда, включая отказы и повторы."""
    if not attempts:
        return
    append_event(Path(attempts), {
        "timestamp": time.time(), "action": action, "release_id": rid,
        "model_id": manifest["model_id"],
    })

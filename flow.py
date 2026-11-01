"""Prefect-flow выпуска модели: подготовка → артефакт → допуск → публикация.

Заменяет последовательный прототип lab.py. Стадии — отдельные задачи Prefect;
между ними передаются только небольшие описания (URI, хеши, версии), веса
и тензоры результатами задач не являются.

Кеш подготовки: cache_key_fn + persist_result. Ключ считается из байтов
входа и полной конфигурации (release.cache_key), результат переживает
завершение процесса, поэтому повтор flow в НОВОМ процессе после сбоя
не повторяет подготовку. Изменение входа или конфигурации меняет ключ.

CLI совместим с прототипом lab.py (интерфейс — надмножество):
  python flow.py --workspace .lab init
  python flow.py --workspace .lab run --report report-B.json
  python flow.py --workspace .lab run --fail-after-prepare
  python flow.py --workspace .lab run --manifest manifest-A.json --report report-A.json
  # реальный выпуск из ДЗ 6+7 (готовые run и отчёты, без нового обучения):
  python flow.py --workspace real run --real --manifest candidate-manifest.json \
      --report candidate-release-report.json     # → отказ гейта, rc != 0
  python flow.py --workspace real run --real --manifest baseline-manifest.json \
      --report baseline-release-report.json      # → публикация/идемпотентный noop

Стадия получения артефакта читает ГОТОВЫЙ training-run (здесь — учебные
фикстуры STUB ONLY; в реальном выпуске — run из ДЗ 6+7, см. real/):
чтение результата не означает повторное обучение.

Сервер Prefect не обязан быть поднят заранее: при недоступном
PREFECT_API_URL используется эфемерный режим, но для межпроцессного кеша
нужен постоянный сервер (make prefect-up).
"""
import argparse
import json
import warnings
from pathlib import Path

import release
from prefect import flow, get_run_logger, task
from prefect.context import TaskRunContext

JOURNAL = "releases.jsonl"      # события фактического выпуска
ATTEMPTS = "attempts.jsonl"     # журнал попыток: пишется всегда, включая отказы


def _state_name(future) -> str:
    """Имя финального состояния задачи: Completed / Cached / Failed.

    Свойство state у Future помечено deprecated, но это единственный способ
    получить состояние в теле flow; предупреждение глушим точечно.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return future.state.name


# ---------------------------------------------------------------- инициализация стенда

def initialize(root: Path) -> None:
    """Учебные фикстуры STUB ONLY — те же байты, что создавал прототип."""
    if root.exists():
        raise FileExistsError("Рабочий каталог уже существует. Выберите новый --workspace.")
    root.mkdir(parents=True)
    runtime = {"base_revision": "fixture-only", "tokenizer_sha256": "a" * 64,
               "template_sha256": "b" * 64,
               "generation": {"max_new_tokens": 32, "do_sample": False}, "dtype": "float32"}
    for version in ["A", "B"]:
        artifact = root / f"model-{version}.txt"
        artifact.write_text(f"STUB ONLY — version {version}\n", encoding="utf-8")
        files = {artifact.name: release.digest(artifact.read_bytes())}
        m = {"schema_version": 1, "model_id": f"fixture-{version}",
             "artifact_uri": artifact.name, "model_sha256": release.digest(release.canonical(files)),
             "protocol_sha256": "d" * 64, "golden_sha256": "e" * 64,
             "runtime_sha256": release.runtime_sha256(runtime), "runtime": runtime,
             "file_hashes": files}
        report = {k: m[k] for k in ["model_id", "model_sha256", "protocol_sha256",
                                    "golden_sha256", "runtime_sha256"]}
        report.update(status="complete", passed=True,
                      evaluation_run_id=f"fixture-eval-{version}",
                      metrics={"accuracy": .8, "format_rate": 1.0}, synthetic=True)
        _write(root / f"manifest-{version}.json", m)
        _write(root / f"report-{version}.json", report)
    _write(root / "accepted.json", _read(root / "manifest-A.json"))
    report = _read(root / "report-B.json")
    report["status"] = "running"
    report["passed"] = "false"  # строка: прототип принимал её как «истину»
    _write(root / "report-incomplete.json", report)
    report = _read(root / "report-B.json")
    report["passed"] = False
    _write(root / "report-reject.json", report)
    _write(root / "config.json", {"tokenizer_revision": "r1", "template_sha256": "b" * 64,
                                  "max_length": 128, "code_revision": "prepare-v1"})
    (root / "input.txt").write_text("Первый пример\nВторой пример\n", encoding="utf-8")
    print("Создан синтетический стенд:", root)


def _read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------- задачи flow

def _prepare_cache_key(context: TaskRunContext, parameters: dict):
    """Ключ кеша Prefect = ключ release.cache_key: байты входа + весь config."""
    return release.cache_key(parameters["data_path"], parameters["config"])


@task(persist_result=True, cache_key_fn=_prepare_cache_key, refresh_cache=False)
def prepare_input(data_path: str, config: dict) -> dict:
    """Подготовка входа: проверка строк и сводка. Единственная кешируемая стадия.

    Реальная подготовка в ДЗ 8-10 — проверка схемы/токенизация данных;
    ключ учитывает ревизию токенизатора, шаблон, длину и версию кода.
    """
    logger = get_run_logger()
    payload = Path(data_path).read_bytes()
    rows = [line for line in payload.decode("utf-8").splitlines() if line.strip()]
    bad = [i + 1 for i, line in enumerate(rows) if not line.strip() or len(line) > 10000]
    if bad:
        raise ValueError(f"Строки не проходят проверку схемы: {bad[:5]}")
    key = release.cache_key(data_path, config)
    result = {"input_sha256": release.digest(payload), "rows": len(rows),
              "config": config, "cache_key": key}
    logger.info("PREPARED input_sha256=%s rows=%s", result["input_sha256"][:12], result["rows"])
    return result


@task
def obtain_artifact(workspace: str, manifest_name: str = "manifest-B.json") -> dict:
    """Получить готовый артефакт обучения: прочитать manifest и пересчитать хеши файлов.

    Чтение результата обучения, а не повторное обучение. Для учебного стенда
    файлы артефакта лежат в workspace; реальный baseline — снапшот HF-кеша по
    точной ревизии; кандидат — каталог адаптера из artifact_uri.
    """
    logger = get_run_logger()
    root = Path(workspace)
    manifest = _read(root / manifest_name)
    uri = manifest.get("artifact_uri", "")
    if uri.startswith("hf:"):
        import backends
        base = backends._cached_snapshot(manifest.get("base_model_repo") or uri[3:].split("@", 1)[0],
                                         manifest["runtime"]["base_revision"])
    elif uri.startswith("adapter-dir:"):
        base = Path(uri.split(":", 1)[1])
    else:
        base = root
    files = manifest.get("file_hashes") or {}
    for rel, expected in sorted(files.items()):
        path = base / rel
        if not path.is_file():
            raise FileNotFoundError(f"Файл артефакта отсутствует: {rel}")
        actual = release.file_sha256(path)
        if actual != expected:
            raise ValueError(f"Хеш файла {rel} не совпадает: {actual} против {expected}")
    logger.info("ARTIFACT model_id=%s files=%d", manifest["model_id"], len(files))
    return manifest


@task
def gate_check(manifest: dict, report_path: str) -> dict:
    """Допуск: validate_release связывает manifest и отчёт полной оценки.

    Для реального выпуска используется gate_check_real: правило качества
    ДЗ 6+7 вызывается живым прогоном на исходных отчётах — один val_loss
    не заменяет оценку поведения.
    """
    logger = get_run_logger()
    report = _read(Path(report_path))
    release.validate_release(manifest, report)  # ValueError = отказ выпуска
    logger.info("GATE PASS model_id=%s eval=%s", manifest["model_id"],
                report["evaluation_run_id"])
    return {"passed": True, "model_id": manifest["model_id"],
            "evaluation_run_id": report["evaluation_run_id"],
            "report_path": str(report_path)}


@task
def gate_check_real(workspace: str, manifest_name: str, report_name: str) -> dict:
    """Допуск реального выпуска: обвязка gate.admit вызывает правило ДЗ 6+7.

    Для кандидата качество_gate прогоняется на исходных baseline/candidate-eval
    из ДЗ 6+7; отказ сохраняется в отчёт и падает здесь же (ValueError из
    validate_release), не доходя до публикации.
    """
    logger = get_run_logger()
    from gate import admit
    root = Path(workspace)
    decision = admit.admit_real(str(root / manifest_name), str(root / report_name),
                                str(root))
    logger.info("GATE REAL model_id=%s passed=%s report=%s",
                decision["report_path"], decision["passed"], decision["report_sha256"][:12])
    return decision


@task
def publish_release_task(manifest: dict, decision: dict, workspace: str) -> dict:
    """Публикация: атомарный указатель + журнал событий, идемпотентно."""
    logger = get_run_logger()
    root = Path(workspace)
    report = _read(Path(decision["report_path"]))
    outcome = release.publish_release(
        manifest, report, root / "accepted.json", root / JOURNAL,
        attempts=root / ATTEMPTS, decision=decision)
    logger.info("RELEASE %s model_id=%s release_id=%s", outcome["status"],
                manifest["model_id"], outcome["release_id"][:12])
    return outcome


@flow(name="release-model")
def release_flow(workspace: str = ".lab", report: str = "report-B.json",
                 manifest_name: str = "manifest-B.json", fail_after_prepare: bool = False,
                 real: bool = False):
    root = Path(workspace)
    if real:
        data_path, config_path = root / "eval-golden.jsonl", root / "prepare-config.json"
    else:
        data_path, config_path = root / "input.txt", root / "config.json"
    config = _read(config_path)
    print(f"FLOW START workspace={root.resolve()} real={real}", flush=True)

    prepare = prepare_input.submit(str(data_path), config)
    prepared = prepare.result()
    print(f"TASK_STATE prepare_input {_state_name(prepare)} "
          f"input_sha256={prepared['input_sha256'][:12]}", flush=True)

    if fail_after_prepare:
        raise RuntimeError("Инъекция сбоя после подготовки")

    artifact = obtain_artifact.submit(str(root), manifest_name)
    manifest = artifact.result()
    print(f"TASK_STATE obtain_artifact {_state_name(artifact)} "
          f"model_id={manifest['model_id']}", flush=True)

    if real:
        gate = gate_check_real.submit(str(root), manifest_name, report)
        decision = gate.result()
        print(f"TASK_STATE gate_check_real {_state_name(gate)} "
              f"gate_passed={decision['gate']['passed']}", flush=True)
        report_path = decision["report_path"]
    else:
        gate = gate_check.submit(manifest, str(root / report))
        decision = gate.result()
        print(f"TASK_STATE gate_check {_state_name(gate)} passed={decision['passed']}",
              flush=True)
        report_path = str(root / report)

    publish = publish_release_task.submit(manifest, {**decision, "report_path": report_path},
                                          str(root))
    outcome = publish.result()
    print(f"TASK_STATE publish_release {_state_name(publish)} status={outcome['status']}",
          flush=True)
    print(f"PUBLISHED {manifest['model_id']} release_id={outcome['release_id'][:12]} "
          f"events={outcome['events']}", flush=True)
    return outcome


def main():
    parser = argparse.ArgumentParser(description="Процесс выпуска (Prefect)")
    parser.add_argument("--workspace", type=Path, default=Path(".lab"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--report", default="report-B.json")
    run_parser.add_argument("--manifest", default="manifest-B.json",
                            help="manifest кандидата или ранее принятой версии (откат)")
    run_parser.add_argument("--fail-after-prepare", action="store_true")
    run_parser.add_argument("--real", action="store_true",
                            help="реальные артефакты ДЗ 6+7 в workspace real/")
    args = parser.parse_args()
    if args.command == "init":
        initialize(args.workspace)
    else:
        release_flow(str(args.workspace), args.report, args.manifest,
                     args.fail_after_prepare, args.real)


if __name__ == "__main__":
    main()

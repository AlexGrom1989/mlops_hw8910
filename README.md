# ДЗ 8+9+10: расследование и исправление процесса выпуска

Исходный прототип с семью независимыми дефектами — коммит `6133956`
(копия сохранена в `prototype/` как мишень RED-прогона собственных тестов).
Расследование по шагам — `defects.csv`, полный отчёт — `REPORT.txt`,
сценарии инцидентов — `INCIDENTS.txt`, контракты — `CONTRACTS.txt`.

## Состав

| Файл | Роль |
|---|---|
| `release.py` | ядро выпуска: канонический JSON, ключ кеша, `validate_release`, атомарная публикация с `release_id` (только stdlib) |
| `flow.py` | Prefect-flow: prepare → artifact → gate → publish, отдельные задачи; кеш подготовки переживает процесс |
| `service_fastapi.py` | сервис: `/health` `/ready` `/generate`, готовность после прогрева, модель в памяти не подменяется указателем |
| `backends.py` | внедряемый backend: `stub` (учебные фикстуры) и `transformers` (реальная модель ДЗ 6+7) |
| `gate/` | правило качества ДЗ 6+7 (побайтовая копия) + исходные отчёты оценки с SHA256SUMS |
| `real/` | реальный выпуск из ДЗ 6+7: manifest-ы, отчёты, принятая baseline, замер раздела D |
| `tests/` | выданные 13 тестов (не менялись; отпечаток `09bd65e6b886`) |
| `tests_integration/` | собственные 9 интеграционных тестов; `RELEASE_TARGET=prototype` гоняет их против исходного кода |
| `prototype/` | исходные `lab.py`/`service.py`/`release.py` до исправлений |
| `scripts/` | демо процесса (`demo-process.sh`), опыт A→B→A (`demo-service.sh`), замер (`bench_real.py`), проверка адаптера (`check_adapter_load.py`) |
| `logs/` | доказательства: RED/GREEN прогоны, шаги демо, замер, quick-check красный/зелёный |

## Быстрый старт

```bash
# 1. ядро без окружения (Python 3.12, stdlib): 13 выданных тестов
make check

# 2. окружение проекта (Prefect, FastAPI; uv.lock закрепляет версии;
#    веса — отдельная группа model, для интеграционных тестов не нужна)
uv sync --frozen
make prefect-up          # постоянный сервер: кеш подготовки межпроцессный

# 3. собственные интеграционные тесты против ИСХОДНОГО прототипа (RED)
RELEASE_TARGET=prototype make check-integration   # → FAILED (failures=14)

# 4. они же против исправленного кода (GREEN)
make check-integration                            # → OK (9 тестов)
```

## Демо и замеры (логи в `logs/`)

```bash
scripts/demo-process.sh    # сбой после подготовки → кеш → noop → смена входа →
                           # сбой между указателем и журналом → восстановление → отказы
scripts/demo-service.sh    # A → публикация B при живом сервисе → перезапуск → откат к A
.venv/bin/python scripts/bench_real.py            # раздел D: 8 запросов × параллельность 1 и 2
.venv/bin/python scripts/check_adapter_load.py    # кандидат: загрузка адаптера + генерация
```

Реальный выпуск из ДЗ 6+7 (без нового обучения):

```bash
.venv/bin/python flow.py --workspace real run --real --manifest candidate-manifest.json \
    --report candidate-release-report.json    # гейт отказал, rc≠0
.venv/bin/python flow.py --workspace real run --real --manifest baseline-manifest.json \
    --report baseline-release-report.json     # принято, release_id 38ec6bf31cc0…
```

Веса (`*.safetensors`), окружение и кеши в Git не входят: целостность
артефакта восстанавливается проверкой `file_hashes` по manifest и пиннингом
ревизии базы; адаптер лежит в MLflow (run `0f060a1fb674…` той работы).

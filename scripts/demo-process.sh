#!/usr/bin/env bash
# Демонстрация процесса выпуска на учебном стенде (STUB ONLY): все сценарии
# раздела A. Каждый шаг пишет журнал в logs/process-*.log.
set -uo pipefail
cd "$(dirname "$0")/.."
export NO_PROXY=127.0.0.1,localhost,::1 no_proxy=127.0.0.1,localhost,::1
export PREFECT_HOME="$PWD/.prefect" PREFECT_API_URL=http://127.0.0.1:4200/api
PY=.venv/bin/python
WS=.lab-demo
mkdir -p logs

rm -rf "$WS"
$PY flow.py --workspace "$WS" init >/dev/null
# Уникальный вход: ключ кеша зависит от байтов, демо не должно ловить
# чужой кеш от прежних прогонов с тем же содержимым.
echo "уникальный маркер демо $(date +%s)" >> "$WS/input.txt"

run() {  # run <log> [аргументы...] → код возврата в журнал
  local log=$1; shift
  $PY flow.py --workspace "$WS" run "$@" >"logs/$log" 2>&1
  echo "rc=$?" >>"logs/$log"
  grep -aoE 'TASK_STATE [a-z_]+ [A-Za-z]+|PUBLISHED [a-zA-Z0-9-]+|PREPARED input_sha256=[0-9a-f]+|CACHE_HIT [0-9a-f]+' \
    "logs/$log" | tail -6 | sed 's/^/    /' >>"logs/$log"
}

echo "== 1. сбой после подготовки (подготовка выполнена и закеширована)"
run process-1-crash.log --fail-after-prepare

echo "== 2. повтор в новом процессе: подготовка из кеша, выпуск B"
run process-2-recovered.log

echo "== 3. повтор успешного выпуска: событие не дублируется"
run process-3-repeat.log

echo "== 4. изменение входа по тому же пути: кеш недействителен"
echo "изменившиеся данные $(date +%s)" >> "$WS/input.txt"
run process-4-input-changed.log

echo "== 5. сбой между указателем и журналом → восстановление без дубля"
# Выпускаем A (его ещё нет в журнале): указатель обновится, журнал не запишется.
# Если бы вместо A выпускался уже опубликованный B, действие было бы noop и
# точки инъекции мы бы не достигли.
RELEASE_CRASH_AFTER_POINTER=1 $PY flow.py --workspace "$WS" run \
  --manifest manifest-A.json --report report-A.json >logs/process-5-crash-pointer.log 2>&1
echo "rc=$?" >>logs/process-5-crash-pointer.log
{
  echo "указатель после сбоя: $(python3 -c 'import json;print(json.load(open("'"$WS"'/accepted.json"))["model_id"])')"
  echo "release_id в указателе: $(grep -aoE 'release_id=[0-9a-f]+' logs/process-5-crash-pointer.log | head -1)"
  echo "событий в журнале: $(wc -l < "$WS/releases.jsonl")"
} >>logs/process-5-crash-pointer.log
run process-6-recovered-journal.log --manifest manifest-A.json --report report-A.json

echo "== 6-7. отрицательные проверки: неполный и чужой отчёт, честный отказ"
BEFORE=$(sha256sum "$WS/accepted.json" | cut -d' ' -f1)
run process-6-incomplete-report.log --report report-incomplete.json
run process-7-foreign-report.log --report report-A.json
run process-8-reject.log --report report-reject.json
AFTER=$(sha256sum "$WS/accepted.json" | cut -d' ' -f1)
{
  echo "accepted.json до: ${BEFORE:0:24}"
  echo "accepted.json после: ${AFTER:0:24}"
  [ "$BEFORE" = "$AFTER" ] && echo "ИТОГ: указатель не изменился ни в одном отказе" \
                            || echo "ИТОГ: УКАЗАТЕЛЬ ИЗМЕНИЛСЯ — ОШИБКА"
} | tee logs/negative-checks-summary.log

echo "журнал событий выпуска:"
cat "$WS/releases.jsonl"
echo "журнал попыток:"
cat "$WS/attempts.jsonl"

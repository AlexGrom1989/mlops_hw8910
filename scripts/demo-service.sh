#!/usr/bin/env bash
# Опыт A → B → A на двух лёгких STUB-версиях (INCIDENTS.txt, сценарий 6):
# смена указателя не подменяет модель в памяти; после перезапуска загружается
# новая версия; управляемый откат возвращает A и подтверждается ответом.
set -uo pipefail
cd "$(dirname "$0")/.."
export NO_PROXY=127.0.0.1,localhost,::1 no_proxy=127.0.0.1,localhost,::1
export PREFECT_HOME="$PWD/.prefect" PREFECT_API_URL=http://127.0.0.1:4200/api
PY=.venv/bin/python
WS=.lab-demo-svc
PORT=8094
LOG=logs/service-a-b-a.log
mkdir -p logs

rm -rf "$WS"
$PY flow.py --workspace "$WS" init >/dev/null

start_service() {  # фоновый сервис; печатает STARTING после bind
  $PY service_fastapi.py --workspace "$WS" --port "$PORT" --warmup-seconds 1 \
    >>"$LOG" 2>&1 &
  SVC=$!
  for _ in $(seq 1 40); do curl -sf --noproxy '*' "http://127.0.0.1:$PORT/health" >/dev/null && break; sleep 0.3; done
  for _ in $(seq 1 40); do curl -sf --noproxy '*' "http://127.0.0.1:$PORT/ready" >/dev/null && break; sleep 0.3; done
}

stop_service() { kill "$SVC" 2>/dev/null; wait "$SVC" 2>/dev/null; }

gen() { curl -s --noproxy '*' -X POST "http://127.0.0.1:$PORT/generate" \
  -H 'Content-Type: application/json' -d '{"prompt":"тест"}'; }

{
  echo "== старт на принятой A"
} >"$LOG"
start_service
echo "ответ: $(gen)" | tee -a "$LOG"

echo "== публикация B в другом процессе, сервис работает" | tee -a "$LOG"
$PY flow.py --workspace "$WS" run >>"$LOG" 2>&1
echo "debug: $(curl -s --noproxy '*' http://127.0.0.1:$PORT/debug)" | tee -a "$LOG"
echo "ответ: $(gen)" | tee -a "$LOG"
stop_service

echo "== перезапуск: загружается B" | tee -a "$LOG"
start_service
echo "ответ: $(gen)" | tee -a "$LOG"
stop_service

echo "== управляемый откат к A (выпуск ранее принятой версии)" | tee -a "$LOG"
$PY flow.py --workspace "$WS" run --manifest manifest-A.json --report report-A.json >>"$LOG" 2>&1
start_service
echo "ответ: $(gen)" | tee -a "$LOG"
echo "debug: $(curl -s --noproxy '*' http://127.0.0.1:$PORT/debug)" | tee -a "$LOG"
stop_service

echo "== журнал выпусков стенда:" | tee -a "$LOG"
cat "$WS/releases.jsonl" | tee -a "$LOG"

PYTHON ?= python3
VENV ?= .venv/bin/python
# Системный прокси перехватывает localhost (урок ДЗ 6+7): исключаем локальные адресы.
export NO_PROXY := 127.0.0.1,localhost,::1
export no_proxy := 127.0.0.1,localhost,::1
# Prefect: домашний профиль проекта, чтобы кеш подготовки и база жили в репозитории каталога.
export PREFECT_HOME := $(CURDIR)/.prefect
export PREFECT_API_URL := http://127.0.0.1:4200/api

.PHONY: check check-integration check-all ci-local prefect-up prefect-down service stub clean

check:
	$(PYTHON) -m compileall -q release.py tests
	$(PYTHON) -m unittest discover -s tests -v

check-integration:
	$(VENV) -m compileall -q flow.py service_fastapi.py backends.py tests_integration
	$(VENV) -m unittest discover -s tests_integration -v

check-all: check check-integration

ci-local: check-all
	@echo "Локальный эквивалент CI: оба контура зелёные"

prefect-up:
	@mkdir -p $(PREFECT_HOME)
	@(.venv/bin/prefect server start --host 127.0.0.1 --port 4200 > .prefect-server.log 2>&1 &) || true
	@for i in $$(seq 1 180); do \
	  curl -sf http://127.0.0.1:4200/api/health >/dev/null 2>&1 && { echo "Prefect: http://127.0.0.1:4200 (UI /)"; exit 0; }; sleep 1; done; \
	  echo "Prefect не поднялся за 180 с, см. .prefect-server.log"; exit 1

prefect-down:
	@pkill -f "prefect server start" 2>/dev/null || true
	@echo "Prefect остановлен"

service:
	$(VENV) service_fastapi.py --workspace .lab --port 8091 --warmup-seconds 3

stub:
	$(VENV) service_fastapi.py --workspace .lab --port 8091 --warmup-seconds 3 --backend stub

clean:
	rm -rf .lab*/ __pycache__ tests/__pycache__ tests_integration/__pycache__ .prefect .prefect-server.log

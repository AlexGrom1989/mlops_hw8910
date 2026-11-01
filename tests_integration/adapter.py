"""Адаптер запуска: одни и те же тесты против прототипа и исправленной версии.

RELEASE_TARGET=prototype — исходный прототип из prototype/ (красный прогон);
RELEASE_TARGET=fixed — исправленный проект (зелёный прогон). CLI-интерфейс
запуска и HTTP-контракты версий совпадают, поэтому тестам не нужно знать,
какая версия под ними, кроме явно отмеченных fixed-only проверок.
"""
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGET = os.environ.get("RELEASE_TARGET", "fixed")

FLOW_ENV = {
    **os.environ,
    "NO_PROXY": "127.0.0.1,localhost,::1",
    "no_proxy": "127.0.0.1,localhost,::1",
    "PREFECT_HOME": str(ROOT / ".prefect"),
    "PREFECT_API_URL": "http://127.0.0.1:4200/api",
}

# Системный прокси перехватывает и localhost (урок ДЗ 6+7): все HTTP-обращения
# тестов идут через opener без прокси, иначе health-check гоняется в 10809.
OPENER = None


def opener():
    """urllib-opener без прокси: только прямые соединения с 127.0.0.1."""
    global OPENER
    if OPENER is None:
        import urllib.request
        OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return OPENER


def http_json(url, method="GET", payload=None, timeout=30):
    """Прямой HTTP-запрос к локальному сервису; возвращает (status, json)."""
    import urllib.error
    data = json.dumps(payload).encode() if payload is not None else None
    req = __import__("urllib.request", fromlist=["request"]).Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {})
    try:
        with opener().open(req, timeout=timeout) as r:
            body = r.read().decode()
            return r.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, {"raw": body}


_prefect_attempted = False


def ensure_prefect() -> None:
    """Поднять локальный сервер Prefect, если он не работает.

    Межпроцессный кеш подготовки живёт в базе сервера, поэтому для fixed-тестов
    нужен один постоянный сервер на всю сессию (эфемерный режим создаёт
    одноразовую базу и кеш между процессами теряется). Попытка запуска
    выполняется одна: повторные 180-секундные ожидания маскируют причину.
    """
    global _prefect_attempted
    if _prefect_attempted or TARGET != "fixed":
        return
    _prefect_attempted = True
    try:
        status, _ = http_json("http://127.0.0.1:4200/api/health", timeout=3)
        if status == 200:
            return
    except OSError:
        pass
    (ROOT / ".prefect").mkdir(exist_ok=True)
    log = open(ROOT / ".prefect-server.log", "ab")
    subprocess.Popen(
        [str(ROOT / ".venv/bin/prefect"), "server", "start",
         "--host", "127.0.0.1", "--port", "4200"],
        env=FLOW_ENV, stdout=log, stderr=log, start_new_session=True)
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            status, _ = http_json("http://127.0.0.1:4200/api/health", timeout=3)
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError("Prefect-сервер не поднялся за 180 с (см. .prefect-server.log); "
                       "запустите: make prefect-up")


def release_cmd(workspace: Path, args) -> list:
    """Команда запуска процесса выпуска с совместимым CLI."""
    script = "lab.py" if TARGET == "prototype" else "flow.py"
    base = ROOT / "prototype" if TARGET == "prototype" else ROOT
    return [sys.executable, str(base / script), "--workspace", str(workspace)] + list(args)


def run_release(workspace: Path, args, extra_env=None) -> subprocess.CompletedProcess:
    """Запустить процесс выпуска в ОТДЕЛЬНОМ процессе со своим рабочим каталогом."""
    ensure_prefect()
    env = dict(FLOW_ENV if TARGET == "fixed" else os.environ)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        release_cmd(workspace, args), capture_output=True, text=True,
        env=env, cwd=str(ROOT), timeout=300)


def service_cmd(workspace: Path, port: int, warmup_seconds: float) -> list:
    """Команда запуска HTTP-сервиса с совместимым CLI."""
    script = "service.py" if TARGET == "prototype" else "service_fastapi.py"
    base = ROOT / "prototype" if TARGET == "prototype" else ROOT
    return [sys.executable, str(base / script),
            "--workspace", str(workspace), "--port", str(port),
            "--warmup-seconds", str(warmup_seconds)]


class Service:
    """Реальный локальный HTTP-сервер в отдельном процессе (localhost, TCP)."""

    def __init__(self, workspace: Path, warmup_seconds: float = 3.0, port: int = None):
        ensure_prefect()  # сервису Prefect не нужен, но проверка дешёвая и единая
        self.port = port or free_port()
        self.proc = subprocess.Popen(
            service_cmd(workspace, self.port, warmup_seconds),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            env=FLOW_ENV if TARGET == "fixed" else os.environ, cwd=str(ROOT))
        self.base = f"http://127.0.0.1:{self.port}"
        self._wait_listening()

    def _wait_listening(self, timeout=30.0):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                status, _ = http_json(self.base + "/health", timeout=2)
                if status == 200:
                    return
            except OSError as e:
                last = e
            time.sleep(0.2)
        out = self.proc.stdout.read() if self.proc.stdout else ""
        raise RuntimeError(f"Сервис не поднялся на {self.base}: {last}\n{out}")

    def request(self, method, path, payload=None, timeout=60):
        """HTTP-запрос к РЕАЛЬНОМУ локальному серверу; возвращает (status, json)."""
        return http_json(self.base + path, method=method, payload=payload, timeout=timeout)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def release_events(workspace: Path) -> list:
    """События фактического выпуска в терминах целевой версии.

    Прототип пишет publications.jsonl без различения попыток и выпусков;
    исправленная версия — releases.jsonl (одно событие на release_id).
    """
    root = Path(workspace)
    if TARGET == "prototype":
        path = root / "publications.jsonl"
        if not path.exists():
            return []
        return [json.loads(line)["model_id"] for line in path.read_text().splitlines() if line.strip()]
    path = root / "releases.jsonl"
    if not path.exists():
        return []
    return [json.loads(line)["model_id"] for line in path.read_text().splitlines() if line.strip()]


def sha256(path) -> str:
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def init_workspace(tmpdir: str, name: str, unique_input: str = None) -> Path:
    """Свежий стенд с УНИКАЛЬНЫМ содержимым input.txt.

    Ключ кеша исправленной версии зависит от байтов входа, а не от пути, поэтому
    одинаковое содержимое в разных стендах — законный кеш-хит. Для проверки
    «подготовка выполняется» вход должен быть уникальным.
    """
    ws = Path(tmpdir) / name
    out = subprocess.run(
        release_cmd(ws, ["init"]), capture_output=True, text=True,
        env=os.environ, cwd=str(ROOT), timeout=60)
    assert out.returncode == 0, out.stderr
    if unique_input:
        (ws / "input.txt").write_text(unique_input, encoding="utf-8")
    return ws

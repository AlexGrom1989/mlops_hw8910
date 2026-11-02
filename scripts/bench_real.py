"""Замер раздела D: 8 одинаковых запросов при параллельности 1 и 2.

Сервис — реальная принятая baseline (Qwen3-0.6B, CPU, float32), промпты —
первые 8 заданий golden set ДЗ 6+7, лимит ответа один: 32 токена (предел
протокола оценки). Каждая попытка сохраняется целиком, ошибки не удаляются.

Запуск:
  .venv/bin/python scripts/bench_real.py --port 8095 [--skip-service]
  (--skip-service: сервис уже поднят, замерять готовый)
"""
import argparse
import concurrent.futures as cf
import json
import statistics
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
TIMEOUT_S = 180  # CPU-генерация 32 токенов может идти десятки секунд


def golden_prompts(n=8):
    rows = [json.loads(line) for line in
            (ROOT / "real" / "eval-golden.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()]
    return rows[:n]


def post(base, payload, timeout=TIMEOUT_S):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(base + "/generate", data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    started = time.perf_counter()
    try:
        with OPENER.open(req, timeout=timeout) as r:
            body = json.loads(r.read().decode())
            return {"status": r.status, "client_seconds": round(time.perf_counter() - started, 3),
                    **body}
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode())
        except ValueError:
            body = {}
        return {"status": e.code, "client_seconds": round(time.perf_counter() - started, 3),
                "error": body.get("detail") or str(e)}
    except Exception as e:  # noqa: BLE001 — таймауты и обрывы остаются в журнале
        return {"status": 0, "client_seconds": round(time.perf_counter() - started, 3),
                "error": f"{type(e).__name__}: {e}"}


def wait_ready(base, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with OPENER.open(base + "/ready", timeout=5) as r:
                if r.status == 200:
                    return
        except OSError:
            pass
        time.sleep(2)
    raise SystemExit("сервис не стал готовым — прогрев не завершён")


def run_batch(base, prompts, mode, workers):
    """Один пакет: те же 8 запросов; workers=1 — последовательно, 2 — параллельно."""
    started = time.perf_counter()
    records = []
    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(post, base, {"prompt": p["input"], "max_new_tokens": 32}): p
                   for p in prompts}
        for future in cf.as_completed(futures):
            prompt = futures[future]
            result = future.result()
            records.append({
                "id": f"{mode}-{len(records) + 1}",
                "mode": mode,
                "prompt_id": prompt["id"],
                "expected": prompt.get("expected"),
                "status": result.get("status"),
                "client_seconds": result.get("client_seconds"),
                "generation_seconds": result.get("generation_seconds"),
                "total_seconds": result.get("total_seconds"),
                "model_id": result.get("model_id"),
                "runtime_sha256": (result.get("runtime_sha256") or "")[:12],
                "output_tokens": result.get("output_tokens"),
                "answer": (result.get("answer") or result.get("error") or "")[:120],
                "request_id": result.get("request_id"),
            })
    return sorted(records, key=lambda r: r["id"]), round(time.perf_counter() - started, 3)


def summarize(records, wall_seconds, mode):
    ok = [r for r in records if r["status"] == 200]
    err = [r for r in records if r["status"] != 200]
    lat = [r["client_seconds"] for r in ok]
    gen = [r["generation_seconds"] for r in ok if r.get("generation_seconds")]
    return {
        "mode": mode, "n": len(records), "successes": len(ok), "errors": len(err),
        "batch_seconds": wall_seconds,
        "requests_per_second": round(len(ok) / wall_seconds, 3) if wall_seconds else None,
        "client_latency_seconds": {
            "mean": round(statistics.fmean(lat), 3) if lat else None,
            "max": round(max(lat), 3) if lat else None,
        },
        "generation_seconds": {
            "mean": round(statistics.fmean(gen), 3) if gen else None,
            "max": round(max(gen), 3) if gen else None,
        },
        "output_tokens": sorted({r["output_tokens"] for r in ok}),
        "model_id": next((r["model_id"] for r in ok), None),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8095)
    parser.add_argument("--skip-service", action="store_true")
    args = parser.parse_args()
    base = f"http://127.0.0.1:{args.port}"
    (ROOT / "real").mkdir(exist_ok=True)

    if not args.skip_service:
        import os
        import subprocess
        env = {**os.environ, "NO_PROXY": "127.0.0.1,localhost,::1",
               "no_proxy": "127.0.0.1,localhost,::1"}
        proc = subprocess.Popen(
            [str(ROOT / ".venv/bin/python"), "service_fastapi.py",
             "--workspace", "real", "--port", str(args.port),
             "--backend", "transformers", "--manifest", "accepted.json",
             "--system-instruction", "real/system-instruction.txt",
             "--max-concurrency", "1", "--warmup-seconds", "0"],
            cwd=str(ROOT), env=env,
            stdout=open(ROOT / "logs" / "service-real.log", "ab"), stderr=subprocess.STDOUT)
        print(f"сервис поднимается (pid {proc.pid}), ожидание готовности…", flush=True)
        try:
            wait_ready(base)
            prompts = golden_prompts(8)
            print("готов; прогрев завершён, запуск замеров", flush=True)
            _measure(base, prompts)
        finally:
            proc.terminate()
            proc.wait(timeout=30)
    else:
        prompts = golden_prompts(8)
        _measure(base, prompts)


def _measure(base, prompts):
    out_raw = ROOT / "real" / "bench-requests.jsonl"
    all_records = []
    results = {}
    for mode, workers in (("concurrency-1", 1), ("concurrency-2", 2)):
        records, wall = run_batch(base, prompts, mode, workers)
        all_records += records
        results[mode] = summarize(records, wall, mode)
        print(json.dumps(results[mode], ensure_ascii=False), flush=True)
    with out_raw.open("w", encoding="utf-8") as f:
        for r in all_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    summary = {
        "service": "http://127.0.0.1:8095, backend=transformers, max_concurrency=1",
        "model": results["concurrency-1"]["model_id"],
        "limit": "max_new_tokens=32 (предел протокола оценки ДЗ 6+7)",
        "prompts": "первые 8 заданий real/eval-golden.jsonl",
        "raw": "real/bench-requests.jsonl",
        **results,
    }
    (ROOT / "real" / "bench-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("сохранено: real/bench-requests.jsonl, real/bench-summary.json", flush=True)


if __name__ == "__main__":
    main()

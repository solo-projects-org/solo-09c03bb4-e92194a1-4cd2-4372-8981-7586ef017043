"""HTTP 层测试：起真实 uvicorn 服务（等价 compose 里的 web1/web2）。

两个 uvicorn 进程共享同一份 SQLite 账本与缓存目录，验证跨进程协调；
用真实 TCP + httpx 客户端，避开 ASGI 测试 transport 与 StreamingResponse
在 disconnect 语义上的不兼容。
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
from conftest import DAY_START  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def server_env(object_source, cache_paths):
    """启动一个 uvicorn Web 进程，返回 base_url。"""
    base_url, _ = object_source
    port = _free_port()
    env = dict(os.environ)
    env.update(
        SQLITE_PATH=cache_paths["sqlite"],
        CACHE_DIR=cache_paths["files"],
        OBJECT_SOURCE_URL=base_url,
        CACHE_LIMIT_BYTES=str(10 * 1024 * 1024),
        PROCESS_LABEL="web-1",
        PYTHONPATH=str(ROOT),
    )
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "web.app:app",
            "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
        ],
        cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    web_url = f"http://127.0.0.1:{port}"
    _wait_healthy(proc, web_url)
    yield web_url
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture()
def two_server_envs(object_source, cache_paths):
    """两个共享 SQLite 的 uvicorn 进程。"""
    base_url, _ = object_source
    procs, urls = [], []
    for label in ("web-1", "web-2"):
        port = _free_port()
        env = dict(os.environ)
        env.update(
            SQLITE_PATH=cache_paths["sqlite"],
            CACHE_DIR=cache_paths["files"],
            OBJECT_SOURCE_URL=base_url,
            CACHE_LIMIT_BYTES=str(10 * 1024 * 1024),
            PROCESS_LABEL=label,
            PYTHONPATH=str(ROOT),
        )
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "web.app:app",
                "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
            ],
            cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        procs.append(proc)
        urls.append(f"http://127.0.0.1:{port}")
    for proc, url in zip(procs, urls):
        _wait_healthy(proc, url)
    yield urls
    for proc in procs:
        proc.terminate()
    for proc in procs:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _wait_healthy(proc: subprocess.Popen, url: str, timeout: float = 25.0) -> None:
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            raise RuntimeError(f"Web 进程提前退出：\n{out}")
        try:
            r = httpx.get(f"{url}/api/health", timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError as exc:
            last_err = exc
        time.sleep(0.3)
    raise RuntimeError(f"Web 进程未在 {timeout}s 内就绪：{last_err}")


def test_query_csv_and_lease_release(server_env):
    with httpx.Client(base_url=server_env, timeout=30.0) as client:
        r = client.get("/api/query", params={
            "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60})
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        assert r.headers["x-cache-etag"].startswith('"')
        lines = r.text.strip().splitlines()
        # pyarrow 默认给列名加引号。
        assert lines[0] == '"utc_second","temperature_c","salinity_psu"'
        assert len(lines) == 2  # 表头 + 1 行

        snap = client.get(f"{server_env}/api/cache").json()
        assert snap["lease_count"] == 0  # 响应结束租用即释放
        assert any(e["state"] == "complete" for e in snap["entries"])


def test_invalid_window_rejected(server_env):
    r = httpx.get(f"{server_env}/api/query",
                  params={"key": "day1.parquet", "start": 100, "end": 100}, timeout=10)
    assert r.status_code == 400


def test_404_for_unknown_object(server_env):
    r = httpx.get(f"{server_env}/api/query",
                  params={"key": "nope.parquet", "start": DAY_START, "end": DAY_START + 60},
                  timeout=10)
    assert r.status_code == 404


def test_two_instances_share_one_download(two_server_envs):
    url1, url2 = two_server_envs
    with httpx.Client(timeout=30.0) as client:
        results = [
            client.get(f"{url1}/api/query", params={
                "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60}),
            client.get(f"{url2}/api/query", params={
                "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60}),
        ]
        for r in results:
            assert r.status_code == 200
        assert results[0].headers["x-cache-etag"] == results[1].headers["x-cache-etag"]
        snap = client.get(f"{url1}/api/cache").json()
        assert len([e for e in snap["entries"] if e["key"] == "day1.parquet"]) == 1
        assert snap["lease_count"] == 0
        # 两个进程各自留痕（owner 不同），但文件只有一份。
        labels = {r.headers["x-served-by"] for r in results}
        assert labels == {"web-1", "web-2"}


def test_503_when_cache_full_and_all_busy(object_source, cache_paths):
    base_url, _ = object_source
    port = _free_port()
    env = dict(os.environ)
    env.update(
        SQLITE_PATH=cache_paths["sqlite"],
        CACHE_DIR=cache_paths["files"],
        OBJECT_SOURCE_URL=base_url,
        CACHE_LIMIT_BYTES="25000",  # 够一份（约 19KiB），不够两份
        PROCESS_LABEL="web-full",
        PYTHONPATH=str(ROOT),
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "web.app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    web_url = f"http://127.0.0.1:{port}"
    try:
        _wait_healthy(proc, web_url)
        with httpx.Client(timeout=30.0) as client:
            # 先把 day1 拉进缓存。
            ok = client.get(f"{web_url}/api/query", params={
                "key": "day1.parquet", "start": DAY_START, "end": DAY_START + 60})
            assert ok.status_code == 200
            etag1 = ok.headers["x-cache-etag"]

            # 在测试进程内通过同一本账本持有 day1 的租用，模拟“有读者在用”。
            from cache.coordination import Coordinator
            holder = Coordinator(cache_paths["sqlite"], 25000)
            holder.acquire_lease("day1.parquet", etag1, "test-holder", "test-proc")
            try:
                busy = client.get(f"{web_url}/api/query", params={
                    "key": "day2.parquet", "start": DAY_START, "end": DAY_START + 60})
                assert busy.status_code == 503
                assert "忙" in busy.json()["detail"]
            finally:
                holder.release_lease("test-holder")

            # 读者退出后，LRU 淘汰 day1，day2 可正常下载。
            retry = client.get(f"{web_url}/api/query", params={
                "key": "day2.parquet", "start": DAY_START, "end": DAY_START + 60})
            assert retry.status_code == 200
            snap = client.get(f"{web_url}/api/cache").json()
            assert {e["key"] for e in snap["entries"]} == {"day2.parquet"}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

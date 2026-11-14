"""交付语义测试：失败不得伪装成附件，半份 CSV 绝不外泄。

覆盖三件事：
1. 日档缺列 → 明确的 JSON 错误（422），不是 200 附件里的“查询失败”；
2. 行组读取失败 → 明确报错（500），且报错前一个 CSV 字节都未发出；
3. 浏览器断开 → 本次临时输出被清理、lease 释放，同档其他读者不受影响。

这些用例在有缺陷的实现上会失败：
- 旧实现把 ``SchemaMismatch`` 变成 ``# 查询失败: ...`` 正文，并以 200 附件返回；
- 旧实现边读边发，行组出错时前半份 CSV 已经写给客户端。
"""

from __future__ import annotations

import os
import random
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
from conftest import DAY_START, DAY_SECONDS  # noqa: E402
from web.app import SPOOL_PREFIX  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def web_server(object_source, cache_paths, tmp_path):
    """一个真实 uvicorn Web 进程（与 test_webapi 同款最小启动）。"""
    base_url, _ = object_source
    port = _free_port()
    spool_root = tmp_path / "spool"
    spool_root.mkdir()
    env = dict(os.environ)
    env.update(
        SQLITE_PATH=cache_paths["sqlite"],
        CACHE_DIR=cache_paths["files"],
        OBJECT_SOURCE_URL=base_url,
        CACHE_LIMIT_BYTES=str(10 * 1024 * 1024),
        PROCESS_LABEL="web-delivery",
        QUERY_SPOOL_DIR=str(spool_root),
        PYTHONPATH=str(ROOT),
    )
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "web.app:app",
            "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
        ],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 25.0
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            raise RuntimeError(f"Web 进程提前退出：\n{out}")
        try:
            if httpx.get(f"{url}/api/health", timeout=2.0).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.3)
    else:
        raise RuntimeError("Web 进程未就绪")
    yield url, cache_paths, spool_root
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture()
def missing_column_source(object_source, tmp_path):
    """对象源里额外放一份缺 ``salinity_psu`` 的日档。"""
    base_url, store = object_source
    table = pa.table(
        {
            "utc_second": pa.array([DAY_START + i for i in range(60)], type=pa.int64()),
            "temperature_c": pa.array([20.0] * 60, type=pa.float64()),
        }
    )
    pq.write_table(table, os.path.join(store.data_dir, "day-missing.parquet"))
    store.refresh()
    return base_url, store


@pytest.fixture()
def corrupt_rowgroup_source(object_source, tmp_path):
    """对象源里放一份第 3 个行组数据页被破坏的日档。

    目标窗口覆盖所有行组，因此前面的批次必定已经产出部分 CSV——
    修复前，客户端会拿到“前半份 CSV + 200”。
    """
    base_url, store = object_source
    from scripts.generate_samples import build_day, write_parquet

    good = tmp_path / "good.parquet"
    write_parquet(build_day(DAY_START, 10, 7), good)
    md = pq.ParquetFile(good).metadata
    assert md.num_row_groups >= 3
    payload = bytearray(good.read_bytes())
    start = md.row_group(2).column(0).data_page_offset
    rng = random.Random(42)
    for offset in range(start + 8, start + 200):
        payload[offset] = rng.randrange(256)
    key = "day-corrupt.parquet"
    with open(os.path.join(store.data_dir, key), "wb") as fh:
        fh.write(payload)
    store.refresh()
    return key, md


def _spool_dirs(root: Path) -> set[str]:
    return {p.name for p in root.glob(f"{SPOOL_PREFIX}*")}


def test_missing_column_is_explicit_error_not_fake_attachment(
    web_server, missing_column_source
):
    url, _, spool_root = web_server
    with httpx.Client(base_url=url, timeout=30.0) as client:
        r = client.get(
            "/api/query",
            params={"key": "day-missing.parquet", "start": DAY_START, "end": DAY_START + 600},
        )
        assert r.status_code == 422, r.text
        assert r.headers["content-type"].startswith("application/json")
        assert "content-disposition" not in {k.lower() for k in r.headers}
        detail = r.json()["detail"]
        assert "salinity_psu" in detail
        assert "查询失败" not in r.text

        snap = client.get("/api/cache").json()
        assert snap["lease_count"] == 0
    assert _spool_dirs(spool_root) == set()


def test_rowgroup_read_failure_yields_no_csv_prefix(web_server, corrupt_rowgroup_source):
    url, _, spool_root = web_server
    key, _ = corrupt_rowgroup_source
    window_start, window_end = DAY_START, DAY_START + DAY_SECONDS
    with httpx.Client(base_url=url, timeout=30.0) as client:
        r = client.get(
            "/api/query",
            params={"key": key, "start": window_start, "end": window_end},
        )
        assert r.status_code == 500, r.text
        assert r.headers["content-type"].startswith("application/json")
        # 不能有任何 CSV 前缀伪装成附件。
        assert "content-disposition" not in {k.lower() for k in r.headers}
        assert "utc_second" not in r.text
        assert "未生成 CSV" in r.json()["detail"]

        # 失败后：无残留临时输出，无残留租用；共享缓存本身保留。
        snap = client.get("/api/cache").json()
        assert snap["lease_count"] == 0
        assert any(e["key"] == key and e["state"] == "complete" for e in snap["entries"])
    assert _spool_dirs(spool_root) == set()


def test_failure_does_not_disturb_concurrent_reader_same_day(
    web_server, corrupt_rowgroup_source
):
    """同一日档：一个读者因坏行组失败，另一个只读干净行组的读者照常成功。

    失败读者的 lease 释放/临时输出清理不能把共享缓存文件或对方租用带走。
    """
    from concurrent.futures import ThreadPoolExecutor

    url, _, spool_root = web_server
    key, md = corrupt_rowgroup_source
    bad_window = (DAY_START, DAY_START + DAY_SECONDS)  # 覆盖坏行组
    good_window = (DAY_START, DAY_START + 1000)  # 仅落在第一个行组

    def _fetch(window):
        with httpx.Client(base_url=url, timeout=30.0) as client:
            return client.get(
                "/api/query",
                params={"key": key, "start": window[0], "end": window[1]},
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        bad_future = pool.submit(_fetch, bad_window)
        good_future = pool.submit(_fetch, good_window)
        bad = bad_future.result()
        good = good_future.result()

    assert bad.status_code == 500
    assert good.status_code == 200
    assert good.headers["content-disposition"].startswith("attachment")
    assert int(good.headers["content-length"]) == len(good.content)
    assert good.content.startswith(b'"utc_second"')
    assert good.content.count(b"\n") == 101  # 表头 + [start, start+1000) 每 10s 一条

    with httpx.Client(base_url=url, timeout=10.0) as client:
        snap = client.get("/api/cache").json()
    assert snap["lease_count"] == 0
    assert any(e["key"] == key and e["state"] == "complete" for e in snap["entries"])
    deadline = time.time() + 5.0
    while time.time() < deadline and _spool_dirs(spool_root):
        time.sleep(0.1)
    assert not _spool_dirs(spool_root)


def test_disconnect_cleans_spool_and_releases_without_affecting_other_reader(
    web_server
):
    """浏览器中途断开：本次临时输出清理、lease 释放，其他读者继续成功。"""
    url, _, spool_root = web_server
    key = "day1.parquet"
    # 用一个大窗口（整天）让下载/生成有足够时间被中断。
    params = {"key": key, "start": DAY_START, "end": DAY_START + DAY_SECONDS}

    with httpx.stream("GET", f"{url}/api/query", params=params, timeout=30.0) as resp:
        assert resp.status_code == 200
        iterator = resp.iter_raw()
        first = next(iterator)
        assert b"utc_second" in first
        # 主动断连：关闭连接，不读完响应体。

    # 断开后服务端应清理本次 spool、释放 lease（可能需要一个轮询周期）。
    deadline = time.time() + 10.0
    while time.time() < deadline:
        with httpx.Client(timeout=10.0) as client:
            snap = client.get(f"{url}/api/cache").json()
        if snap["lease_count"] == 0 and not _spool_dirs(spool_root):
            break
        time.sleep(0.25)
    with httpx.Client(timeout=10.0) as client:
        snap = client.get(f"{url}/api/cache").json()
    assert snap["lease_count"] == 0
    assert not _spool_dirs(spool_root), "断开后仍残留本次查询临时输出"

    # 同档的下一个读者不受影响，仍可拿到完整、带长度声明的 CSV。
    with httpx.Client(base_url=url, timeout=30.0) as client:
        ok = client.get("/api/query", params={
            "key": key, "start": DAY_START, "end": DAY_START + 120})
        assert ok.status_code == 200
        assert ok.headers["content-disposition"].startswith("attachment")
        assert int(ok.headers["content-length"]) == len(ok.content)
        assert ok.content.startswith(b'"utc_second"')
        assert ok.content.count(b"\n") == 3  # 表头 + 2 行

    # 后台清理可能在响应体收完后才执行完，给一个短暂的收敛窗口。
    deadline = time.time() + 5.0
    while time.time() < deadline and _spool_dirs(spool_root):
        time.sleep(0.1)
    assert not _spool_dirs(spool_root)

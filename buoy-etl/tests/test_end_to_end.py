"""端到端：对象源 → 协调下载 → PyArrow 分批筛取 → CSV。

用 asyncio.run 驱动，避免额外的 pytest-asyncio 依赖。
"""

from __future__ import annotations

import asyncio
import io
import os
import time

import httpx
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest

from cache.coordination import CacheBusy, Coordinator, SingleFileTooLarge
from cache.manager import CacheManager, ETagMismatch
from web.query import stream_filtered_csv

from conftest import DAY_START


def _manager(base_url, cache_paths, limit):
    coord = Coordinator(cache_paths["sqlite"], limit)
    return CacheManager(coord, base_url, cache_paths["files"], wait_timeout=30)


def _collect_csv(path, start, end):
    return b"".join(stream_filtered_csv(path, start, end))


def _row_count(csv_bytes: bytes) -> int:
    table = pacsv.read_csv(io.BytesIO(csv_bytes))
    return table.num_rows


def test_head_etag_is_strong_and_stable(object_source, cache_paths):
    base_url, store = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag1, size1 = await mgr.head_etag("day1.parquet")
            etag2, size2 = await mgr.head_etag("day1.parquet")
            assert not etag1.startswith(("W/", "w/"))
            assert etag1 == etag2 and size1 == size2
            assert size1 == os.path.getsize(os.path.join(store.data_dir, "day1.parquet"))
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_full_flow_window_half_open(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            lease = await mgr.ensure("day1.parquet", etag, size)
            start, end = DAY_START, DAY_START + 3600  # 首小时
            csv_bytes = await asyncio.to_thread(_collect_csv, lease.path, start, end)
            await mgr.release(lease)
        finally:
            await mgr.close()

        # 60s 采样：窗口 [0, 3600) 应有 60 行（不含终点）。
        assert _row_count(csv_bytes) == 60
        table = pacsv.read_csv(io.BytesIO(csv_bytes))
        utc = table.column("utc_second").to_pylist()
        assert utc[0] == start
        assert utc[-1] < end
        assert min(utc) >= start and max(utc) < end
        assert table.column_names == ["utc_second", "temperature_c", "salinity_psu"]
    asyncio.run(scenario())


def test_empty_window_returns_header_only(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day2.parquet")
            lease = await mgr.ensure("day2.parquet", etag, size)
            # 落在 day1 范围的窗口对 day2 必为空。
            out = await asyncio.to_thread(
                _collect_csv, lease.path, DAY_START, DAY_START + 60
            )
            await mgr.release(lease)
        finally:
            await mgr.close()
        assert b"utc_second" in out
        assert _row_count(out) == 0
    asyncio.run(scenario())


def test_concurrent_same_version_downloads_once(object_source, cache_paths):
    base_url, store = object_source
    downloads = {"count": 0}
    original_stream = store.stream

    def counting_stream(key, write, flush):
        downloads["count"] += 1
        return original_stream(key, write, flush)

    store.stream = counting_stream
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            leases = await asyncio.gather(
                *[mgr.ensure("day1.parquet", etag, size) for _ in range(5)]
            )
            assert len(leases) == 5
            snap = await mgr.snapshot()
            assert sum(1 for e in snap["entries"] if e["key"] == "day1.parquet") == 1
            assert snap["lease_count"] == 5
            for lease in leases:
                await mgr.release(lease)
        finally:
            await mgr.close()
    asyncio.run(scenario())
    assert downloads["count"] == 1


def test_download_failure_deletes_residue_and_releases(object_source, cache_paths):
    base_url, store = object_source
    store.set_simulation("day1.parquet", delay=0.0, fail_after=512)
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            with pytest.raises((ConnectionAbortedError, httpx.HTTPError, ETagMismatch)):
                await mgr.ensure("day1.parquet", etag, size)
            snap = await mgr.snapshot()
            assert snap["total_bytes"] == 0 and snap["entries"] == []
            leftovers = [f for f in os.listdir(cache_paths["files"]) if f.endswith((".part", ".tmp"))]
            assert leftovers == []
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_etag_change_midflight_fails_and_recovers(object_source, cache_paths, object_dir):
    base_url, store = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag_v1, size_v1 = await mgr.head_etag("day1.parquet")
            # 模拟“查询固定版本后、GET 前源端更正”：替换对象内容。
            corrected = object_dir / "day1.corrected.parquet"
            payload = corrected.read_bytes()
            new_meta = store.correct("day1.parquet", payload)
            assert new_meta["etag"] != etag_v1

            with pytest.raises(ETagMismatch):
                await mgr.ensure("day1.parquet", etag_v1, size_v1)
            # 残件与预留都已清掉。
            snap = await mgr.snapshot()
            assert snap["entries"] == []

            # 新查询重新 HEAD 固定新版本，成功。
            etag_v2, size_v2 = await mgr.head_etag("day1.parquet")
            assert etag_v2 == new_meta["etag"]
            lease = await mgr.ensure("day1.parquet", etag_v2, size_v2)
            out = await asyncio.to_thread(
                _collect_csv, lease.path, DAY_START, DAY_START + 120
            )
            assert _row_count(out) == 2
            await mgr.release(lease)
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_cache_full_single_file_rejected(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 1024)  # 小于任何日档
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            with pytest.raises(SingleFileTooLarge):
                await mgr.ensure("day1.parquet", etag, size)
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_cache_full_busy_when_all_in_use(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        # 上限可放下一份（约 19 KiB），但放不下两份。
        mgr = _manager(base_url, cache_paths, 25_000)
        await mgr.start()
        try:
            etag1, size1 = await mgr.head_etag("day1.parquet")
            lease1 = await mgr.ensure("day1.parquet", etag1, size1)  # 占满且在用
            etag2, size2 = await mgr.head_etag("day2.parquet")
            with pytest.raises(CacheBusy):
                await mgr.ensure("day2.parquet", etag2, size2)
            # 释放读者后，LRU 淘汰 day1，day2 可下载。
            await mgr.release(lease1)
            lease2 = await mgr.ensure("day2.parquet", etag2, size2)
            snap = await mgr.snapshot()
            keys = {e["key"] for e in snap["entries"]}
            assert keys == {"day2.parquet"}
            await mgr.release(lease2)
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_slow_download_does_not_block_other_days(slow_object_source, object_dir, tmp_path):
    base_url, store = slow_object_source
    # day1 每块睡 0.3s，day2 正常。
    store.set_simulation("day1.parquet", delay=0.3, fail_after=0)
    paths = {
        "sqlite": str(tmp_path / "coord2" / "c.db"),
        "files": str(tmp_path / "files2"),
    }
    async def scenario():
        mgr = _manager(base_url, paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag1, size1 = await mgr.head_etag("day1.parquet")
            slow = asyncio.create_task(mgr.ensure("day1.parquet", etag1, size1))
            await asyncio.sleep(0.2)  # 让慢下载先跑起来
            etag2, size2 = await mgr.head_etag("day2.parquet")
            t0 = time.monotonic()
            lease2 = await mgr.ensure("day2.parquet", etag2, size2)
            elapsed = time.monotonic() - t0
            await lease2.release(mgr)
            assert elapsed < 5.0  # 未被慢日档堵住
            lease1 = await slow
            await lease1.release(mgr)
        finally:
            await mgr.close()
    asyncio.run(scenario())


def test_parquet_filter_pushdown_matches_full_scan(object_source, cache_paths):
    base_url, _ = object_source
    async def scenario():
        mgr = _manager(base_url, cache_paths, 10 * 1024 * 1024)
        await mgr.start()
        try:
            etag, size = await mgr.head_etag("day1.parquet")
            lease = await mgr.ensure("day1.parquet", etag, size)
            start, end = DAY_START + 7200, DAY_START + 10800
            out = await asyncio.to_thread(_collect_csv, lease.path, start, end)
            full = pq.read_table(lease.path)
            utc = full.column("utc_second")
            expect = full.filter(
                pa.compute.and_(
                    pa.compute.greater_equal(utc, pa.scalar(start)),
                    pa.compute.less(utc, pa.scalar(end)),
                )
            ).num_rows
            await mgr.release(lease)
            assert _row_count(out) == expect
        finally:
            await mgr.close()
    asyncio.run(scenario())

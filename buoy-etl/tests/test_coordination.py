"""SQLite 协调层的单元测试（容量、淘汰、忙、租用保护、崩溃回收）。"""

from __future__ import annotations

import pytest

from cache.coordination import (
    STATE_COMPLETE,
    CacheBusy,
    Coordinator,
    SingleFileTooLarge,
    VersionGone,
)


def make_coord(tmp_path, limit=1000):
    return Coordinator(str(tmp_path / "c.db"), limit, stale_download_ttl=0.05, lease_ttl=0.05)


def test_single_file_over_limit_rejected(tmp_path):
    coord = make_coord(tmp_path, limit=500)
    with pytest.raises(SingleFileTooLarge):
        coord.reserve("k", '"a"', 600, str(tmp_path / "a"), str(tmp_path / "a.tmp"), "owner-1")


def test_reserve_total_counts_download_against_limit(tmp_path):
    coord = make_coord(tmp_path, limit=1000)
    p = str(tmp_path / "a")
    assert coord.reserve("k1", '"a"', 600, p, p + ".tmp", "o1") == "reserved"
    # 剩余 400：更大的预留应失败（没有可淘汰对象）。
    with pytest.raises(CacheBusy):
        coord.reserve("k2", '"b"', 500, str(tmp_path / "b"), str(tmp_path / "b.tmp"), "o2")


def test_eviction_only_touches_unleased_complete_files_lru(tmp_path):
    coord = make_coord(tmp_path, limit=1000)
    # 两个完整文件各 400，共 800。
    for i, (k, etag) in enumerate([("k1", '"a"'), ("k2", '"b"')]):
        path = tmp_path / f"{i}.parquet"
        path.write_bytes(b"x" * 400)
        coord.reserve(k, etag, 400, str(path), str(path) + ".tmp", "o")
        coord.mark_complete(k, etag, "o")
    # k1 被租用，k2 空闲；需要 700 空间，只能淘汰 k2，仍不够 => CacheBusy。
    coord.acquire_lease("k1", '"a"', "lease-1", "reader")
    with pytest.raises(CacheBusy):
        coord.reserve("k3", '"c"', 700, str(tmp_path / "c"), str(tmp_path / "c.tmp"), "o2")
    assert coord.get_entry("k1", '"a"') is not None  # 在用文件保住了
    # 释放后再试：淘汰 k2（LRU 最旧）后 400+400=800 腾出，仍需 700 → 800 占用归零后可行。
    coord.release_lease("lease-1")
    out = coord.reserve("k3", '"c"', 700, str(tmp_path / "c"), str(tmp_path / "c.tmp"), "o2")
    assert out == "reserved"
    assert coord.get_entry("k1", '"a"') is None or coord.get_entry("k2", '"b"') is None
    assert not (tmp_path / "1.parquet").exists()  # 被淘汰文件已从磁盘删除


def test_eviction_succeeds_when_enough_space(tmp_path):
    coord = make_coord(tmp_path, limit=1000)
    path = tmp_path / "a.parquet"
    path.write_bytes(b"x" * 800)
    coord.reserve("k1", '"a"', 800, str(path), str(path) + ".tmp", "o")
    coord.mark_complete("k1", '"a"', "o")
    # 需要 500：淘汰 800 的 k1 后足够。
    assert coord.reserve("k2", '"b"', 500, str(tmp_path / "b"), str(tmp_path / "b.tmp"), "o2") == "reserved"
    assert coord.get_entry("k1", '"a"') is None


def test_same_key_same_etag_single_download(tmp_path):
    coord = make_coord(tmp_path)
    p = str(tmp_path / "a")
    first = coord.reserve("k", '"a"', 100, p, p + ".tmp", "o1")
    second = coord.reserve("k", '"a"', 100, p, p + ".tmp", "o2")
    assert first == "reserved"
    assert second == "downloading"  # 后来者不重复预留


def test_fail_download_removes_residue_and_releases(tmp_path):
    coord = make_coord(tmp_path)
    tmp = tmp_path / "a.tmp"
    tmp.write_bytes(b"partial")
    coord.reserve("k", '"a"', 100, str(tmp_path / "a"), str(tmp), "o1")
    coord.fail_download("k", '"a"', "o1", str(tmp))
    assert not tmp.exists()
    assert coord.get_entry("k", '"a"') is None
    # 释放后可以重新预留。
    assert coord.reserve("k", '"a"', 100, str(tmp_path / "a"), str(tmp), "o2") == "reserved"


def test_mark_complete_checks_length_and_owner(tmp_path):
    coord = make_coord(tmp_path)
    path = tmp_path / "a"
    coord.reserve("k", '"a"', 100, str(path), str(path) + ".tmp", "o1")
    path.write_bytes(b"short")
    with pytest.raises(VersionGone):
        coord.mark_complete("k", '"a"', "o1")
    with pytest.raises(VersionGone):
        coord.mark_complete("k", '"a"', "someone-else")
    path.write_bytes(b"x" * 100)
    coord.mark_complete("k", '"a"', "o1")
    assert coord.get_entry("k", '"a"').state == STATE_COMPLETE


def test_lease_blocks_eviction_then_release_allows(tmp_path):
    coord = make_coord(tmp_path, limit=500)
    path = tmp_path / "a"
    path.write_bytes(b"x" * 400)
    coord.reserve("k", '"a"', 400, str(path), str(path) + ".tmp", "o")
    coord.mark_complete("k", '"a"', "o")
    coord.acquire_lease("k", '"a"', "L1", "r")
    with pytest.raises(CacheBusy):
        coord.reserve("k2", '"b"', 400, str(tmp_path / "b"), str(tmp_path / "b.tmp"), "o2")
    coord.release_lease("L1")
    assert coord.reserve("k2", '"b"', 400, str(tmp_path / "b"), str(tmp_path / "b.tmp"), "o2") == "reserved"


def test_reap_clears_stale_download_and_lease(tmp_path):
    coord = make_coord(tmp_path, limit=2000)
    tmp = tmp_path / "a.tmp"
    tmp.write_bytes(b"z")
    coord.reserve("k", '"a"', 100, str(tmp_path / "a"), str(tmp), "dead-owner")
    path2 = tmp_path / "b"
    path2.write_bytes(b"x" * 100)
    coord.reserve("k2", '"b"', 100, str(path2), str(path2) + ".tmp", "o")
    coord.mark_complete("k2", '"b"', "o")
    coord.acquire_lease("k2", '"b"', "L", "r")
    # 等待超过 download/lease TTL 后再 reap，保证两者都过期。
    import time as _t
    _t.sleep(0.1)
    coord.reap()
    assert coord.get_entry("k", '"a"') is None
    assert not tmp.exists()
    assert coord.snapshot()["lease_count"] == 0

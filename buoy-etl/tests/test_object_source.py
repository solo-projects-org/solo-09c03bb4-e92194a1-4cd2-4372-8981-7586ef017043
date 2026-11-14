"""对象源：强 ETag、清单自动刷新、故障注入与源端更正。"""

from __future__ import annotations

import json
from urllib.request import Request, urlopen

import pytest

from scripts.generate_samples import build_day, write_parquet

from conftest import DAY_START, DAY_SECONDS


@pytest.fixture()
def http_source(object_source):
    base_url, store = object_source
    return base_url, store


def _get_json(url: str):
    with urlopen(url, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode())


def test_catalog_includes_manifest_fields(tmp_path):
    from object_source.server import ObjectStore

    d = tmp_path / "objs"
    d.mkdir()
    write_parquet(build_day(DAY_START, 60, 1), d / "d.parquet")
    (d / "manifest.json").write_text(json.dumps({
        "d.parquet": {"day_start_utc": DAY_START, "day_end_utc": DAY_START + DAY_SECONDS}
    }))
    store = ObjectStore(str(d))
    item = store.catalog()[0]
    assert item["key"] == "d.parquet"
    assert item["day_start_utc"] == DAY_START
    assert item["etag"].startswith('"') and not item["etag"].startswith(("W/", 'w/'))


def test_head_exposes_strong_etag_and_length(http_source):
    base_url, _ = http_source
    req = Request(f"{base_url}/objects/day1.parquet", method="HEAD")
    with urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        etag = resp.headers["ETag"]
        assert etag.startswith('"') and not etag.startswith(("W/", "w/"))
        assert int(resp.headers["Content-Length"]) > 0


def test_auto_refresh_picks_up_new_file(http_source, object_dir):
    base_url, store = http_source
    before = {x["key"] for x in store.catalog()}
    assert "newday.parquet" not in before
    write_parquet(build_day(DAY_START + 2 * DAY_SECONDS, 60, 7), object_dir / "newday.parquet")
    # 不重启进程，下一次 catalog 调用应自动发现新文件。
    _, catalog = _get_json(f"{base_url}/objects")
    assert any(x["key"] == "newday.parquet" for x in catalog)
    # 新对象可直接 HEAD/GET。
    req = Request(f"{base_url}/objects/newday.parquet", method="HEAD")
    with urlopen(req, timeout=5) as resp:
        assert resp.status == 200


def test_correction_changes_etag(http_source, object_dir):
    base_url, store = http_source
    old_etag = store.meta_for("day1.parquet")["etag"]
    payload = (object_dir / "day1.corrected.parquet").read_bytes()
    new_meta = store.correct("day1.parquet", payload)
    assert new_meta["etag"] != old_etag
    # 通过 HTTP 读到的 ETag 已更新。
    req = Request(f"{base_url}/objects/day1.parquet", method="HEAD")
    with urlopen(req, timeout=5) as resp:
        assert resp.headers["ETag"] == new_meta["etag"]


def test_simulation_fail_after_aborts_transfer(http_source):
    base_url, store = http_source
    store.set_simulation("day1.parquet", delay=0.0, fail_after=256)
    try:
        with pytest.raises(Exception):  # 连接被源端中途切断
            urlopen(f"{base_url}/objects/day1.parquet", timeout=5).read()
    finally:
        store.clear_simulation("day1.parquet")
    # 清除注入后恢复完整可读。
    with urlopen(f"{base_url}/objects/day1.parquet", timeout=5) as resp:
        data = resp.read()
    assert len(data) == store.meta_for("day1.parquet")["size"]


def test_404_for_missing_object(http_source):
    base_url, _ = http_source
    from urllib.error import HTTPError
    with pytest.raises(HTTPError) as exc:
        urlopen(f"{base_url}/objects/missing.parquet", timeout=5)
    assert exc.value.code == 404

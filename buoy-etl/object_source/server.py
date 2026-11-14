"""本地 HTTP 对象源（岸站对象库模拟）。

仅用 Python 标准库实现：
- ``GET  /objects``           日档清单（key / 强 ETag / 全长 / 当日 UTC 秒范围）
- ``HEAD /objects/{key}``     强 ETag（SHA-256，无 ``W/`` 前缀）+ Content-Length
- ``GET  /objects/{key}``     流式取档，支持演示用延迟/中途失败注入
- ``POST /admin/correct/{key}``  用新 Parquet 原子替换对象（源端更正 → 新 ETag）
- ``POST /admin/simulate``    注入慢下载/中途失败，``DELETE`` 取消注入

ETag 按文件内容 SHA-256 计算并缓存；文件被替换后自动重算，因此更正前后
版本可明确区分，供 Web 端做版本固定与 ETag 失配检测。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [object-source] %(message)s"
)
log = logging.getLogger("object-source")

DATA_DIR = os.environ.get("DATA_DIR", "/data/objects")
PORT = int(os.environ.get("PORT", "8080"))
CHUNK_BYTES = 256 * 1024


class ObjectStore:
    # 自动刷新的最小间隔：避免每次请求都 stat 目录。
    REFRESH_THROTTLE_SECONDS = 1.0

    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self._lock = threading.RLock()
        # key -> {"etag": str, "size": int, "mtime": float}
        self._meta: dict[str, dict] = {}
        # key -> {"delay": float, "fail_after": int}
        self._sim: dict[str, dict] = {}
        self._manifest: dict[str, dict] = {}
        self._last_refresh = 0.0
        self._dir_signature: tuple | None = None
        self.refresh()

    # -------------------------------------------------------------元数据
    def maybe_refresh(self) -> None:
        """目录签名（文件清单+大小+mtime）变化时才重扫，平时只做一次 stat。"""
        try:
            entries = tuple(
                sorted(
                    (name, os.stat(os.path.join(self.data_dir, name)).st_mtime_ns)
                    for name in os.listdir(self.data_dir)
                    if name.endswith(".parquet")
                )
            )
        except FileNotFoundError:
            entries = ()
        if entries != self._dir_signature:
            self.refresh()

    def refresh(self) -> None:
        with self._lock:
            self._last_refresh = time.monotonic()
            manifest_path = os.path.join(self.data_dir, "manifest.json")
            if os.path.exists(manifest_path):
                with open(manifest_path, encoding="utf-8") as fh:
                    self._manifest = json.load(fh)
            if not os.path.isdir(self.data_dir):
                self._dir_signature = ()
                return
            present = set()
            for name in sorted(os.listdir(self.data_dir)):
                if not name.endswith(".parquet"):
                    continue
                present.add(name)
                full = os.path.join(self.data_dir, name)
                st = os.stat(full)
                cached = self._meta.get(name)
                if cached is None or cached["mtime"] != st.st_mtime or cached["size"] != st.st_size:
                    etag = self._hash_file(full)
                    self._meta[name] = {
                        "etag": etag,
                        "size": st.st_size,
                        "mtime": st.st_mtime,
                    }
                    log.info("登记/刷新对象 %s etag=%s size=%d", name, etag, st.st_size)
            for gone in set(self._meta) - present:
                del self._meta[gone]
            try:
                self._dir_signature = tuple(
                    sorted(
                        (name, os.stat(os.path.join(self.data_dir, name)).st_mtime_ns)
                        for name in present
                    )
                )
            except FileNotFoundError:
                self._dir_signature = ()

    @staticmethod
    def _hash_file(path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for block in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(block)
        return '"' + h.hexdigest() + '"'

    def catalog(self) -> list[dict]:
        self.maybe_refresh()
        with self._lock:
            out = []
            for key, meta in sorted(self._meta.items()):
                item = {
                    "key": key,
                    "etag": meta["etag"],
                    "size": meta["size"],
                }
                item.update(self._manifest.get(key, {}))
                out.append(item)
            return out

    def meta_for(self, key: str) -> dict | None:
        self.maybe_refresh()
        with self._lock:
            return self._meta.get(key)

    def set_simulation(self, key: str, delay: float, fail_after: int) -> None:
        with self._lock:
            self._sim[key] = {"delay": max(0.0, delay), "fail_after": max(0, fail_after)}

    def clear_simulation(self, key: str) -> None:
        with self._lock:
            self._sim.pop(key, None)

    def get_simulation(self, key: str) -> dict:
        with self._lock:
            return dict(self._sim.get(key, {"delay": 0.0, "fail_after": 0}))

    def correct(self, key: str, payload: bytes) -> dict:
        """原子替换对象内容，返回新版本元数据。"""
        if not key.endswith(".parquet") or "/" in key or ".." in key:
            raise ValueError("非法对象名")
        with self._lock:
            tmp = os.path.join(self.data_dir, f".{key}.new-{os.getpid()}")
            with open(tmp, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            final = os.path.join(self.data_dir, key)
            os.replace(tmp, final)
            self.refresh()
            log.info("源端更正 %s -> %s", key, self._meta[key]["etag"])
            return dict(self._meta[key])

    # -------------------------------------------------------------取档
    def stream(self, key: str, write, flush) -> None:
        """``write(bytes)`` 发送数据；模拟失败时抛 :class:`ConnectionAbortedError`。"""
        meta = self.meta_for(key)
        if meta is None:
            raise FileNotFoundError(key)
        sim = self.get_simulation(key)
        sent = 0
        with open(os.path.join(self.data_dir, key), "rb") as fh:
            while True:
                block = fh.read(CHUNK_BYTES)
                if not block:
                    break
                if sim["fail_after"] and sent + len(block) > sim["fail_after"]:
                    cut = max(0, sim["fail_after"] - sent)
                    if cut:
                        write(block[:cut])
                        flush()
                        sent += cut
                    log.warning("模拟中断：%s 已发送 %d 字节后断开", key, sent)
                    raise ConnectionAbortedError("simulated mid-transfer failure")
                write(block)
                flush()
                sent += len(block)
                if sim["delay"]:
                    time.sleep(sim["delay"])


STORE = ObjectStore(DATA_DIR)


class Handler(BaseHTTPRequestHandler):
    server_version = "BuoyObjectSource/1.0"

    def log_message(self, fmt: str, *args) -> None:
        log.info("%s - %s", self.address_string(), fmt % args)

    # -------------------------------------------------------------路由
    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/healthz":
            self._json(200, {"ok": True})
        elif path == "/objects":
            self._json(200, STORE.catalog())
        elif path.startswith("/objects/"):
            self._get_object(path[len("/objects/"):])
        else:
            self._json(404, {"detail": "not found"})

    def do_HEAD(self) -> None:
        path = urlparse(self.path).path
        if not path.startswith("/objects/"):
            self._json(404, {"detail": "not found"})
            return
        meta = STORE.meta_for(path[len("/objects/"):])
        if meta is None:
            self._json(404, {"detail": "对象不存在"})
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("ETag", meta["etag"])  # 强 ETag：无 W/ 前缀
        self.send_header("Content-Length", str(meta["size"]))
        self.send_header("Content-Type", "application/vnd.apache.parquet")
        self.end_headers()

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/admin/simulate":
            body = self._read_json()
            if body is None:
                return
            try:
                STORE.set_simulation(
                    body["key"], float(body.get("delay_per_chunk", 0.0)),
                    int(body.get("fail_after_bytes", body.get("fail_after", 0))),
                )
            except (KeyError, ValueError) as exc:
                self._json(400, {"detail": f"参数错误：{exc}"})
                return
            self._json(200, {"ok": True, "simulation": STORE.get_simulation(body["key"])})
        elif path.startswith("/admin/correct/"):
            key = path[len("/admin/correct/"):]
            length = int(self.headers.get("Content-Length", "0"))
            payload = self.rfile.read(length) if length else b""
            try:
                meta = STORE.correct(key, payload)
            except ValueError as exc:
                self._json(400, {"detail": str(exc)})
                return
            self._json(200, {"ok": True, "meta": meta})
        else:
            self._json(404, {"detail": "not found"})

    def do_DELETE(self) -> None:
        path = urlparse(self.path).path
        if path.startswith("/admin/simulate/"):
            key = path[len("/admin/simulate/"):]
            STORE.clear_simulation(key)
            self._json(200, {"ok": True})
        else:
            self._json(404, {"detail": "not found"})

    # -------------------------------------------------------------实现
    def _get_object(self, key: str) -> None:
        meta = STORE.meta_for(key)
        if meta is None:
            self._json(404, {"detail": "对象不存在"})
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("ETag", meta["etag"])
        self.send_header("Content-Length", str(meta["size"]))
        self.send_header("Content-Type", "application/vnd.apache.parquet")
        self.end_headers()
        try:
            STORE.stream(key, self.wfile.write, self.wfile.flush)
        except (ConnectionAbortedError, BrokenPipeError, ConnectionResetError) as exc:
            log.warning("取档中断 %s：%s", key, exc)
            # 直接关闭连接，让客户端把它当成传输失败处理。
            self.close_connection = True

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"detail": f"JSON 解析失败：{exc}"})
            return None

    def _json(self, status: int, payload) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log.info("对象源监听 0.0.0.0:%d，数据目录 %s", PORT, DATA_DIR)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

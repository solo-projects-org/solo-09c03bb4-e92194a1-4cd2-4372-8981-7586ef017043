"""缓存管理器：把对象源、SQLite 账本和文件落盘粘起来。

关键承诺
========
- 每次查询先取源端强 ETag 并固定该版本；取档时 ETag 不匹配即失败。
- 同档同版全集群只下载一次：先到者 ``reserve`` 成功负责下载，后来者
  在条件变量上等待，不产生重复流量。
- 下载按源端 ``Content-Length`` 全长预留空间；失败先删残件再释放预留。
- 慢源/慢下载只阻塞同一 (key, etag) 的等待者，其他日档的查询照常推进
  （每个等待在独立任务里，事件环不被独占）。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import tempfile
import time
import uuid
from dataclasses import dataclass

import httpx

from .coordination import (
    STATE_COMPLETE,
    Coordinator,
    VersionGone,
)

logger = logging.getLogger("buoy.cache")


class ETagMismatch(RuntimeError):
    """下载流中的 ETag 与查询开始时固定的版本不一致。"""


class DownloadAttemptsExhausted(RuntimeError):
    """多次等待后下载仍未完成（下载者崩溃或持续失败）。"""


@dataclass
class Lease:
    lease_id: str
    key: str
    etag: str
    path: str

    async def release(self, manager: "CacheManager") -> None:
        await manager.release(self)


class CacheManager:
    def __init__(
        self,
        coordinator: Coordinator,
        object_base_url: str,
        cache_dir: str,
        *,
        owner: str | None = None,
        wait_timeout: float = 120.0,
        poll_interval: float = 0.25,
    ) -> None:
        self.coord = coordinator
        self.object_base_url = object_base_url.rstrip("/")
        self.cache_dir = cache_dir
        self.owner = owner or f"{os.uname().nodename}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.wait_timeout = wait_timeout
        self.poll_interval = poll_interval
        self._client: httpx.AsyncClient | None = None
        # 同进程等待者挂在这个事件上；跨进程等待者靠轮询 SQLite。
        # 不在这里实例化（构造通常发生在事件循环之外，asyncio 锁会绑定
        # 首次使用时的循环），首次真正需要时懒创建。
        self._entry_changed: asyncio.Condition | None = None
        os.makedirs(cache_dir, exist_ok=True)

    def _condition(self) -> asyncio.Condition:
        cond = self._entry_changed
        if cond is None:
            cond = asyncio.Condition()
            self._entry_changed = cond
        return cond

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self.object_base_url,
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=10.0),
            follow_redirects=True,
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    # -------------------------------------------------------------元数据
    async def head_etag(self, key: str) -> tuple[str, int]:
        """HEAD 取强 ETag 与全长。强 ETag 形如 ``"..."``（无 ``W/`` 前缀）。"""
        assert self._client is not None
        resp = await self._client.head(f"/objects/{key}")
        if resp.status_code == 404:
            raise FileNotFoundError(f"对象不存在：{key}")
        resp.raise_for_status()
        etag = resp.headers.get("etag", "")
        if not etag:
            raise RuntimeError(f"源端未返回 ETag：{key}")
        if etag.startswith(("W/", "w/")):
            raise RuntimeError(f"源端仅提供弱 ETag，无法固定版本：{etag}")
        length = int(resp.headers.get("content-length", "0"))
        if length <= 0:
            raise RuntimeError(f"源端未返回有效 Content-Length：{key}")
        return etag, length

    # -------------------------------------------------------------取档主流程
    async def ensure(self, key: str, etag: str, size: int) -> Lease:
        """保证 (key, etag) 完整落盘并返回租用句柄。"""
        safe_name = key.replace("/", "_")
        path = os.path.join(self.cache_dir, f"{safe_name}__{_etag_tag(etag)}.parquet")
        tmp_path = f"{path}.{os.getpid()}.tmp"

        outcome = await asyncio.to_thread(
            self.coord.reserve, key, etag, size, path, tmp_path, self.owner
        )
        if outcome == "complete":
            return await self._lease(key, etag, path)
        if outcome == "downloading":
            await self._wait_for_peer(key, etag)
            return await self._lease(key, etag, path)

        # outcome == "reserved"：本进程本任务负责下载。
        try:
            await self._download(key, etag, size, path, tmp_path)
        except Exception:
            await asyncio.to_thread(
                self.coord.fail_download, key, etag, self.owner, tmp_path
            )
            await self._notify()
            raise
        await asyncio.to_thread(self.coord.mark_complete, key, etag, self.owner)
        await self._notify()
        return await self._lease(key, etag, path)

    async def _download(
        self, key: str, etag: str, size: int, path: str, tmp_path: str
    ) -> None:
        assert self._client is not None
        last_beat = 0.0
        written = 0
        # 同目录临时文件 + fsync + rename，保证读者只可能看到完整文件。
        fd, real_tmp = tempfile.mkstemp(prefix=os.path.basename(path), suffix=".part",
                                        dir=os.path.dirname(path))
        os.close(fd)
        try:
            async with self._client.stream("GET", f"/objects/{key}") as resp:
                if resp.status_code == 404:
                    raise FileNotFoundError(f"下载时对象已消失：{key}")
                resp.raise_for_status()
                got_etag = resp.headers.get("etag", "")
                if got_etag != etag:
                    raise ETagMismatch(
                        f"取档 ETag {got_etag!r} 与固定版本 {etag!r} 不一致"
                    )
                got_len = int(resp.headers.get("content-length", "0"))
                if got_len and got_len != size:
                    raise ETagMismatch(
                        f"源端长度 {got_len} 与 HEAD 全长 {size} 不一致，对象已变更"
                    )
                with open(real_tmp, "wb") as fh:
                    async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
                        fh.write(chunk)
                        written += len(chunk)
                        now = time.monotonic()
                        if now - last_beat >= 2.0:
                            await asyncio.to_thread(
                                self.coord.heartbeat_download,
                                key, etag, self.owner,
                            )
                            last_beat = now
                    fh.flush()
                    os.fsync(fh.fileno())
            if written != size:
                raise VersionGone(f"实际下载 {written} 字节 != 预留 {size}")
            os.replace(real_tmp, path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(real_tmp)

    async def _wait_for_peer(self, key: str, etag: str) -> None:
        """等待别的持有者完成下载。本地事件优先，跨进程靠 SQLite 轮询。"""
        deadline = time.monotonic() + self.wait_timeout
        while True:
            entry = await asyncio.to_thread(self.coord.get_entry, key, etag)
            if entry is None:
                # 下载者失败已释放预留：由当前调用方重试一次预留。
                raise VersionGone(f"版本下载被放弃：{key} {etag}")
            if entry.state == STATE_COMPLETE:
                return
            if time.monotonic() > deadline:
                raise DownloadAttemptsExhausted(
                    f"等待 {key}@{etag} 下载超时（{self.wait_timeout}s）"
                )
            cond = self._condition()
            try:
                async with cond:
                    await asyncio.wait_for(cond.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass

    async def _notify(self) -> None:
        cond = self._condition()
        async with cond:
            cond.notify_all()

    async def _lease(self, key: str, etag: str, path: str) -> Lease:
        lease_id = uuid.uuid4().hex
        await asyncio.to_thread(
            self.coord.acquire_lease, key, etag, lease_id, self.owner
        )
        return Lease(lease_id=lease_id, key=key, etag=etag, path=path)

    async def release(self, lease: Lease) -> None:
        await asyncio.to_thread(self.coord.release_lease, lease.lease_id)

    async def snapshot(self) -> dict:
        return await asyncio.to_thread(self.coord.snapshot)

    async def reap(self) -> None:
        await asyncio.to_thread(self.coord.reap)


def _etag_tag(etag: str) -> str:
    return etag.strip('"').replace("/", "_")

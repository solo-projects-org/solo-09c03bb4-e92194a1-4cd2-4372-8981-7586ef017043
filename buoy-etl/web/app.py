"""FastAPI 应用：选日档 + UTC 秒时间窗，流式下载质控 CSV。"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from cache import (
    CacheBusy,
    CacheManager,
    Coordinator,
    SingleFileTooLarge,
    VersionGone,
)
from cache.manager import DownloadAttemptsExhausted, ETagMismatch
from web.query import SchemaMismatch, stream_filtered_csv

logger = logging.getLogger("buoy.web")

CACHE_LIMIT_DEFAULT = 64 * 1024 * 1024  # 64 MiB：下载预留 + 完整缓存合计
LEASE_HEARTBEAT_SECONDS = 10.0
REAP_INTERVAL_SECONDS = 10.0
# 断开检测的 receive 轮询周期：既不长期占用 receive，又能及时发现浏览器断开。
DISCONNECT_POLL_SECONDS = 5.0


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def create_app(
    *,
    sqlite_path: str | None = None,
    cache_dir: str | None = None,
    object_source_url: str | None = None,
    limit_bytes: int | None = None,
    process_label: str | None = None,
) -> FastAPI:
    sqlite_path = sqlite_path or _env("SQLITE_PATH", "/data/cache/coordination/cache.db")
    cache_dir = cache_dir or _env("CACHE_DIR", "/data/cache/files")
    object_source_url = object_source_url or _env(
        "OBJECT_SOURCE_URL", "http://object-source:8080"
    )
    limit_bytes = (
        limit_bytes if limit_bytes is not None
        else int(_env("CACHE_LIMIT_BYTES", str(CACHE_LIMIT_DEFAULT)))
    )
    process_label = process_label or _env("PROCESS_LABEL", f"pid-{os.getpid()}")

    coord = Coordinator(sqlite_path, limit_bytes)
    manager = CacheManager(coord, object_source_url, cache_dir)

    app = FastAPI(title="浮标温盐观测提取", version="1.0.0")
    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
    app.state.manager = manager
    app.state.process_label = process_label
    app.state.limit_bytes = limit_bytes

    @app.on_event("startup")
    async def _startup() -> None:
        await manager.start()
        app.state.reaper_task = asyncio.create_task(_reaper_loop(manager))
        logger.info("web 进程 %s 已启动，缓存上限 %d 字节", process_label, limit_bytes)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        app.state.reaper_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await app.state.reaper_task
        await manager.close()

    async def _reaper_loop(mgr: CacheManager) -> None:
        while True:
            try:
                await mgr.reap()
            except Exception:  # 兜底：reaper 不能死
                logger.exception("reaper 轮询失败")
            await asyncio.sleep(REAP_INTERVAL_SECONDS)

    # ----------------------------------------------------------页面/目录
    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "process_label": process_label,
                "limit_mib": limit_bytes / (1024 * 1024),
            },
        )

    @app.get("/api/catalog")
    async def catalog() -> JSONResponse:
        """代理对象源的日档清单（含当前 ETag 与全长）。"""
        assert manager._client is not None
        resp = await manager._client.get("/objects")
        resp.raise_for_status()
        return JSONResponse(resp.json())

    @app.get("/api/cache")
    async def cache_status() -> JSONResponse:
        snap = await manager.snapshot()
        snap["served_by"] = process_label
        return JSONResponse(snap)

    # ----------------------------------------------------------查询主接口
    @app.get("/api/query")
    async def query(
        request: Request,
        key: str,
        start: int,
        end: int,
    ) -> StreamingResponse:
        if end <= start:
            return JSONResponse(
                {"detail": "时间窗非法：要求 start < end（含起点、不含终点）"},
                status_code=400,
            )

        # 1) 先取源端强 ETag，固定本次查询版本。
        try:
            etag, size = await manager.head_etag(key)
        except FileNotFoundError:
            return JSONResponse({"detail": f"对象不存在：{key}"}, status_code=404)
        except httpx.HTTPError as exc:
            return JSONResponse(
                {"detail": f"对象源不可达，无法固定版本：{exc}"}, status_code=502
            )

        # 2) 确保该版本完整落盘（同档同版全集群只下载一次）。
        try:
            lease = await manager.ensure(key, etag, size)
        except FileNotFoundError:
            return JSONResponse({"detail": f"下载时对象消失：{key}"}, status_code=404)
        except SingleFileTooLarge as exc:
            return JSONResponse({"detail": str(exc)}, status_code=413)
        except CacheBusy as exc:
            return JSONResponse(
                {"detail": f"缓存忙：{exc}，所有完整文件均有读者占用，请稍后重试"},
                status_code=503,
            )
        except ETagMismatch as exc:
            return JSONResponse(
                {"detail": f"源端更正导致版本固定失败：{exc}"}, status_code=409
            )
        except (VersionGone, DownloadAttemptsExhausted) as exc:
            return JSONResponse({"detail": f"取档失败：{exc}"}, status_code=409)
        except httpx.HTTPError as exc:
            # 连接中断、源端中途断开等传输层失败；残件与预留已在 ensure 内清理。
            return JSONResponse(
                {"detail": f"从对象源取档失败（连接已清理，可重试）：{exc}"},
                status_code=502,
            )

        filename = f"{Path(key).stem}_{start}_{end}.csv"
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Cache-ETag": etag,
            "X-Served-By": process_label,
        }

        async def generate():
            hb_task = asyncio.create_task(_lease_heartbeat(manager, lease.lease_id))
            disconnect_poller = asyncio.create_task(
                _watch_disconnect(request, DISCONNECT_POLL_SECONDS)
            )
            try:
                # 阻塞式 PyArrow 迭代放到工作线程，每个批次回到事件环，
                # 慢读取不会堵住同进程处理其他日档的请求。
                iterator = iter(stream_filtered_csv(lease.path, start, end))
                while True:
                    if disconnect_poller.done() and disconnect_poller.result():
                        logger.info("客户端断开，提前结束查询 %s", lease.lease_id)
                        break
                    # 注意：不能让 StopIteration 穿过 asyncio.to_thread ——
                    # 它无法作为异常传回 future，会损坏 async generator。
                    chunk = await asyncio.to_thread(_next_csv_chunk, iterator)
                    if chunk is None:
                        break
                    yield chunk
            except SchemaMismatch as exc:
                yield _error_csv(str(exc))
            finally:
                hb_task.cancel()
                disconnect_poller.cancel()
                for task in (hb_task, disconnect_poller):
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                # 查询结束或浏览器断开：释放本次占用，其他读者不受影响。
                await manager.release(lease)
                logger.info("已释放 lease %s（%s@%s）", lease.lease_id, key, etag)

        return StreamingResponse(
            generate(),
            media_type="text/csv; charset=utf-8",
            headers=headers,
        )

    @app.get("/api/health")
    async def health() -> dict:
        return {"ok": True, "served_by": process_label, "ts": time.time()}

    return app


async def _watch_disconnect(request: Request, poll_seconds: float) -> bool:
    """周期性尝试读取 disconnect 事件。

    用 ``wait_for`` 包住 ``receive``：真实 uvicorn 下断开会立即返回
    ``http.disconnect``；测试用 ASGI transport 不发该事件时，超时只意味着
    “本轮未断开”，不会永久挂起。
    """
    while True:
        try:
            message = await asyncio.wait_for(request.receive(), timeout=poll_seconds)
        except asyncio.TimeoutError:
            continue
        if message.get("type") == "http.disconnect":
            return True


async def _lease_heartbeat(manager: CacheManager, lease_id: str) -> None:
    while True:
        await asyncio.sleep(LEASE_HEARTBEAT_SECONDS)
        ok = await asyncio.to_thread(manager.coord.heartbeat_lease, lease_id)
        if not ok:
            logger.warning("lease %s 已不存在，停止心跳", lease_id)
            return


def _next_csv_chunk(iterator):
    """工作线程入口：把 StopIteration 转成 None 哨兵，避免穿透 to_thread。"""
    try:
        return next(iterator)
    except StopIteration:
        return None


def _error_csv(message: str) -> bytes:
    return f"# 查询失败: {message}\n".encode("utf-8")


# uvicorn 入口：环境（容器 compose）就绪时自动建应用；
# 测试或库式导入若默认路径不可用则降级为 None，由 create_app() 显式构造。
try:
    app = create_app()
except OSError:
    app = None

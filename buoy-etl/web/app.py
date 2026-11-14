"""FastAPI 应用：选日档 + UTC 秒时间窗，完整生成后交付质控 CSV。

交付语义
========
- 响应头一旦发出就无法改状态码，因此查询先按固定强 ETag 取档，再把整个
  时间窗的 CSV 写入本请求专属临时文件；**只有全部批次读取并编码成功**才
  返回 200 附件（带 ``Content-Length`` 与强 ETag 响应头）。
- 缺列、行组读取/编码失败等任何中途错误都在发出响应头之前变成明确的
  JSON 错误（422/500），不会把半份 CSV 或伪装成附件的“查询失败”交给用户。
- 本请求的临时输出与 lease 在失败、浏览器断开、交付结束时统一清理；
  其他读者各自的 lease 与共享缓存不受影响。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path

import anyio
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask

from cache import (
    CacheBusy,
    CacheManager,
    Coordinator,
    SingleFileTooLarge,
    VersionGone,
)
from cache.manager import DownloadAttemptsExhausted, ETagMismatch
from web.query import QueryAborted, SchemaMismatch, stream_filtered_csv

logger = logging.getLogger("buoy.web")

CACHE_LIMIT_DEFAULT = 64 * 1024 * 1024  # 64 MiB：下载预留 + 完整缓存合计
LEASE_HEARTBEAT_SECONDS = 10.0
REAP_INTERVAL_SECONDS = 10.0
# 断开检测的 receive 轮询周期：既不长期占用 receive，又能及时发现浏览器断开。
DISCONNECT_POLL_SECONDS = 1.0
# 读临时文件回送客户端的块大小：低内存、平缓背压。
DELIVERY_CHUNK_BYTES = 64 * 1024
# 每次查询的临时输出目录前缀；正常结束/失败/断开都会整目录删除。
SPOOL_PREFIX = "buoy-query-"
# 中止工作线程后等待其退出的上限（线程内阻塞的磁盘读取无法强杀）。
ABORT_GRACE_SECONDS = 2.0
# 崩溃残留下来的 spool 目录，超过该年龄才在启动时清扫（避免误删同机活跃查询）。
SPOOL_STALE_SECONDS = 24 * 3600


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def create_app(
    *,
    sqlite_path: str | None = None,
    cache_dir: str | None = None,
    object_source_url: str | None = None,
    limit_bytes: int | None = None,
    process_label: str | None = None,
    spool_root: str | None = None,
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
    spool_root = spool_root or _env("QUERY_SPOOL_DIR", tempfile.gettempdir())
    os.makedirs(spool_root, exist_ok=True)

    coord = Coordinator(sqlite_path, limit_bytes)
    manager = CacheManager(coord, object_source_url, cache_dir)

    app = FastAPI(title="浮标温盐观测提取", version="1.0.0")
    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
    app.state.manager = manager
    app.state.process_label = process_label
    app.state.limit_bytes = limit_bytes
    app.state.spool_root = spool_root

    @app.on_event("startup")
    async def _startup() -> None:
        await manager.start()
        _sweep_stale_spools(spool_root)
        app.state.reaper_task = asyncio.create_task(_reaper_loop(manager))
        logger.info("web 进程 %s 已启动，缓存上限 %d 字节", process_label, limit_bytes)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        app.state.reaper_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await app.state.reaper_task
        await manager.close()

    def _release_lease(lease) -> None:
        try:
            manager.coord.release_lease(lease.lease_id)
        except Exception:  # 兜底：释放失败不能阻断错误返回
            logger.exception("释放 lease %s 失败", lease.lease_id)

    async def _best_effort_release(lease) -> None:
        try:
            await asyncio.to_thread(_release_lease, lease)
        except Exception:  # pragma: no cover - 释放失败只记日志，不覆盖交付/错误
            pass

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
    async def query(request: Request, key: str, start: int, end: int) -> Response:
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

        # 取完档后再看客户端是否已经离开：断开时连整窗读取都不必做。
        if await request.is_disconnected():
            await _best_effort_release(lease)
            return JSONResponse({"detail": "客户端已断开，查询已中止"}, status_code=499)

        filename = f"{Path(key).stem}_{start}_{end}.csv"
        spool_dir = tempfile.mkdtemp(prefix=SPOOL_PREFIX, dir=spool_root)
        spool_path = os.path.join(spool_dir, filename)

        # 3) 在发出响应头之前，把整个时间窗完整读写到本请求专属临时文件。
        #    任一批次失败都不会留下“200 附件”的半份 CSV。
        abort_flag = threading.Event()
        hb_task = asyncio.create_task(_lease_heartbeat(manager, lease.lease_id))
        disconnect_poller = asyncio.create_task(
            _watch_disconnect(request, DISCONNECT_POLL_SECONDS)
        )
        worker = asyncio.create_task(
            asyncio.to_thread(
                _spool_window_csv, lease.path, start, end, spool_path, abort_flag
            )
        )
        try:
            await _await_generation(worker, disconnect_poller, abort_flag)
            total_bytes = worker.result()
        except _ClientGone:
            await _stop_worker(worker, abort_flag)
            _cleanup_spool(spool_dir)
            logger.info("客户端断开，已中止并清理查询 %s（%s@%s）", lease.lease_id, key, etag)
            # 客户端已断开，状态码只用于日志/代理语义；不会交付任何字节。
            return JSONResponse({"detail": "客户端已断开，查询已中止"}, status_code=499)
        except QueryAborted as exc:
            _cleanup_spool(spool_dir)
            logger.info("查询 %s 被中止：%s", lease.lease_id, exc)
            return JSONResponse({"detail": str(exc)}, status_code=499)
        except SchemaMismatch as exc:
            _cleanup_spool(spool_dir)
            logger.warning("日档 %s 缺列，拒绝交付：%s", key, exc)
            return JSONResponse(
                {"detail": f"日档结构不符合约定，未生成 CSV：{exc}"},
                status_code=422,
            )
        except asyncio.CancelledError:
            # 服务关闭等外部取消：通知工作线程退出、删临时输出，并继续向上取消。
            # 这里不 await（取消中的任务再次被取消会立刻抛错），
            # 工作线程会在下一个批次边界看到 abort_flag 后自行退出。
            abort_flag.set()
            worker.cancel()
            _cleanup_spool(spool_dir)
            raise
        except Exception as exc:
            _cleanup_spool(spool_dir)
            logger.exception("生成 CSV 失败 %s（%s@%s）", lease.lease_id, key, etag)
            return JSONResponse(
                {"detail": f"读取或编码窗口数据失败，未生成 CSV（可重试）：{exc}"},
                status_code=500,
            )
        finally:
            hb_task.cancel()
            disconnect_poller.cancel()
            for task in (hb_task, disconnect_poller):
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            # 缓存文件的读取已结束（成功、失败或断开），释放本次占用；
            # 其他读者的 lease 与共享缓存完全不受影响。
            await _best_effort_release(lease)
            logger.info("已释放 lease %s（%s@%s）", lease.lease_id, key, etag)

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(total_bytes),
            "ETag": etag,
            "X-Cache-ETag": etag,
            "X-Served-By": process_label,
            "Cache-Control": "no-store",
        }

        async def deliver():
            # 完整结果已落临时文件；按固定小块回送，内存占用与窗口大小无关。
            try:
                async with await anyio.open_file(spool_path, "rb") as fh:
                    while True:
                        chunk = await fh.read(DELIVERY_CHUNK_BYTES)
                        if not chunk:
                            break
                        yield chunk
            finally:
                # 正常交付结束或浏览器中途断开都清理本次临时输出。
                _cleanup_spool(spool_dir)

        return StreamingResponse(
            deliver(),
            media_type="text/csv; charset=utf-8",
            headers=headers,
            background=BackgroundTask(_cleanup_spool, spool_dir),
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


class _ClientGone(Exception):
    """工作线程读取期间发现客户端已断开。"""


def _spool_window_csv(
    src_path: str,
    start: int,
    end: int,
    dest_path: str,
    abort_flag: threading.Event,
) -> int:
    """工作线程：把整个 ``[start, end)`` 窗口完整写成一个 CSV 文件。

    只有所有批次都成功写入才返回总字节数；任何读取/编码异常都会向上抛出，
    并删除不完整的临时输出，调用方据此返回错误而不是“半份 CSV”。
    """
    written = 0
    try:
        with open(dest_path, "wb") as out:
            for chunk in stream_filtered_csv(
                src_path, start, end, should_abort=abort_flag.is_set
            ):
                out.write(chunk)
                written += len(chunk)
            out.flush()
            os.fsync(out.fileno())
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(dest_path)
        raise
    return written


async def _await_generation(
    worker: "asyncio.Task[int]",
    disconnect_poller: "asyncio.Task[bool]",
    abort_flag: threading.Event,
) -> None:
    """等待完整生成完成；浏览器提前断开时中止工作线程并抛 :class:`_ClientGone`。"""
    while True:
        done, _ = await asyncio.wait(
            {worker, disconnect_poller},
            timeout=DISCONNECT_POLL_SECONDS,
            return_when=asyncio.FIRST_COMPLETED,
        )
        # 先看生成结果：即使同一轮里发现断开，也由调用方决定缓存/错误处理，
        # 不能把已经成功的结果误报成“客户端断开”。
        if worker in done:
            return
        if disconnect_poller in done:
            abort_flag.set()
            raise _ClientGone()
        if disconnect_poller.done():
            # 轮询任务被取消（服务关闭）：按客户端消失处理。
            abort_flag.set()
            raise _ClientGone()


async def _stop_worker(
    task: "asyncio.Task[object]", abort_flag: threading.Event
) -> None:
    """让正在生成的工作线程尽快退出，并等待其结束。

    线程里的阻塞读取无法强杀：``abort_flag`` 会让它在下一个批次边界抛
    :class:`QueryAborted`。等待有上限（受卡住的磁盘读取由后续批次检查兜底），
    无论线程是否已退出，其结果/异常都被消费，避免留下未处理的任务异常。
    """
    abort_flag.set()
    task.add_done_callback(_consume_task_result)
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=ABORT_GRACE_SECONDS)
    except asyncio.TimeoutError:
        # 线程仍卡在阻塞 IO：它在下一个批次边界看到 abort_flag 后自行退出，
        # 结果会由 done 回调消费。
        pass
    except asyncio.CancelledError:
        # 外层请求本身被取消：继续向上传播，不把取消吞成普通失败。
        raise
    except Exception:
        # 工作线程的 QueryAborted/读取异常：由调用方按错误路径处理。
        pass


def _consume_task_result(task: "asyncio.Task[object]") -> None:
    """任务结束回调：取走异常，避免 asyncio 打印 “exception was never retrieved”。"""
    if task.cancelled():
        return
    with contextlib.suppress(BaseException):
        task.exception()


def _cleanup_spool(spool_dir: str) -> None:
    """删除本次查询的临时输出目录；重复调用安全。"""
    shutil.rmtree(spool_dir, ignore_errors=True)


def _sweep_stale_spools(spool_root: str, max_age: float = SPOOL_STALE_SECONDS) -> None:
    """启动时清扫本进程崩溃后遗留的旧临时目录（正常请求都会自行删除）。"""
    cutoff = time.time() - max_age
    try:
        entries = os.scandir(spool_root)
    except OSError:
        return
    with entries:
        for entry in entries:
            if not entry.name.startswith(SPOOL_PREFIX) or not entry.is_dir():
                continue
            try:
                if entry.stat().st_mtime < cutoff:
                    shutil.rmtree(entry.path, ignore_errors=True)
            except OSError:
                continue


# uvicorn 入口：环境（容器 compose）就绪时自动建应用；
# 测试或库式导入若默认路径不可用则降级为 None，由 create_app() 显式构造。
try:
    app = create_app()
except OSError:
    app = None

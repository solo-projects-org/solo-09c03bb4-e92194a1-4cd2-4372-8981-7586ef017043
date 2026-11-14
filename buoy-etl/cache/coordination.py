"""SQLite 协调层：两个 Web 进程共享同一份缓存账本。

设计要点
--------
- 只记录账本，不碰业务语义；所有写操作走 ``BEGIN IMMEDIATE``，配合 WAL 与
  busy_timeout 实现多进程互斥。
- 容量口径：``cache_entries.size`` 之和 = 下载预留 + 完整缓存，共享同一上限。
- 空间不足时只淘汰「无人租用（无 lease）的完整文件」，按 LRU 顺序；
  一个都淘汰不了就抛 :class:`CacheBusy`（调用方返回 503 忙）。
- 下载行带 owner + heartbeat，进程崩溃后由任意存活进程的 reaper 清理残件。
- lease（查询占用）同样带 heartbeat，浏览器断开而进程未及时释放时可兜底回收。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import closing
from dataclasses import dataclass

STATE_DOWNLOADING = "downloading"
STATE_COMPLETE = "complete"


class CacheBusy(Exception):
    """空间不足且没有可淘汰的完整文件（全在用）。"""


class SingleFileTooLarge(Exception):
    """单档全长超过缓存上限，直接拒绝。"""


class VersionGone(Exception):
    """指定 (key, etag) 已不存在或尚未完整，无法租用。"""


@dataclass(frozen=True)
class Entry:
    key: str
    etag: str
    state: str
    path: str
    size: int
    owner: str | None
    heartbeat: float | None
    last_used_at: float
    leases: int


class Coordinator:
    """同步 SQLite 协调器；协程通过 ``anyio.to_thread`` 调用。"""

    def __init__(
        self,
        db_path: str,
        limit_bytes: int,
        *,
        stale_download_ttl: float = 30.0,
        lease_ttl: float = 30.0,
    ) -> None:
        self.db_path = db_path
        self.limit_bytes = int(limit_bytes)
        self.stale_download_ttl = stale_download_ttl
        self.lease_ttl = lease_ttl
        self._fs_lock = threading.Lock()  # 同进程内串行化文件 unlink，避免竞态
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with closing(self._connect()) as conn:
            self._init_schema(conn)

    # ------------------------------------------------------------------基础
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=15.0,
            isolation_level=None,  # 显式事务
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    @staticmethod
    def _init_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS cache_entries (
                key          TEXT    NOT NULL,
                etag         TEXT    NOT NULL,
                state        TEXT    NOT NULL CHECK (state IN ('downloading','complete')),
                path         TEXT    NOT NULL,
                tmp_path     TEXT,
                size         INTEGER NOT NULL,
                owner        TEXT,
                heartbeat    REAL,
                created_at   REAL    NOT NULL,
                last_used_at REAL    NOT NULL,
                PRIMARY KEY (key, etag)
            );
            CREATE TABLE IF NOT EXISTS cache_leases (
                lease_id   TEXT PRIMARY KEY,
                key        TEXT NOT NULL,
                etag       TEXT NOT NULL,
                owner      TEXT NOT NULL,
                created_at REAL NOT NULL,
                heartbeat  REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_leases_entry
                ON cache_leases(key, etag);
            """
        )

    @staticmethod
    def _rollback_safely(conn: sqlite3.Connection) -> None:
        """回滚；若事务已结束（如前面显式回滚过）则忽略。"""
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass

    # -------------------------------------------------------------预留/下载
    def reserve(
        self,
        key: str,
        etag: str,
        size: int,
        path: str,
        tmp_path: str,
        owner: str,
    ) -> str:
        """为 (key, etag) 预留空间。

        返回值：
        - ``"complete"``    ：已完整，直接租用即可；
        - ``"downloading"`` ：别的持有者正在下载，等待即可；
        - ``"reserved"``    ：本次调用成功预留，调用方负责下载。
        """
        if size > self.limit_bytes:
            raise SingleFileTooLarge(
                f"对象 {key} 全长 {size} 字节超过单档上限 {self.limit_bytes} 字节"
            )
        now = time.time()
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT state, owner FROM cache_entries WHERE key=? AND etag=?",
                    (key, etag),
                ).fetchone()
                if row is not None:
                    if row["state"] == STATE_COMPLETE:
                        return "complete"
                    conn.execute(
                        "UPDATE cache_entries SET heartbeat=? WHERE key=? AND etag=?",
                        (now, key, etag),
                    )
                    return "downloading"

                if not self._ensure_capacity(conn, size):
                    self._rollback_safely(conn)
                    raise CacheBusy(
                        f"缓存已满：预留 {size} 字节，可淘汰的完整文件为 0"
                    )
                conn.execute(
                    """
                    INSERT INTO cache_entries
                        (key, etag, state, path, tmp_path, size, owner,
                         heartbeat, created_at, last_used_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        key,
                        etag,
                        STATE_DOWNLOADING,
                        path,
                        tmp_path,
                        size,
                        owner,
                        now,
                        now,
                        now,
                    ),
                )
                conn.execute("COMMIT")
                return "reserved"
            except Exception:
                self._rollback_safely(conn)
                raise

    def _ensure_capacity(self, conn: sqlite3.Connection, need: int) -> bool:
        """在当前事务内淘汰无租用的完整文件，直到腾出 ``need`` 字节。"""
        total = conn.execute("SELECT COALESCE(SUM(size),0) FROM cache_entries").fetchone()[0]
        if total + need <= self.limit_bytes:
            return True
        victims = conn.execute(
            """
            SELECT e.key, e.etag, e.path, e.size
            FROM cache_entries e
            WHERE e.state = 'complete'
              AND NOT EXISTS (
                  SELECT 1 FROM cache_leases l
                  WHERE l.key = e.key AND l.etag = e.etag
              )
            ORDER BY e.last_used_at ASC
            """
        ).fetchall()
        freed = 0
        for victim in victims:
            # 事务内 unlink：SQLite 写锁只阻塞其他写者，淘汰量通常很小。
            self._safe_unlink(victim["path"])
            conn.execute(
                "DELETE FROM cache_entries WHERE key=? AND etag=?",
                (victim["key"], victim["etag"]),
            )
            freed += victim["size"]
            if total - freed + need <= self.limit_bytes:
                return True
        return False

    @staticmethod
    def _safe_unlink(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def heartbeat_download(self, key: str, etag: str, owner: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE cache_entries SET heartbeat=? WHERE key=? AND etag=? AND owner=?",
                (time.time(), key, etag, owner),
            )

    def mark_complete(self, key: str, etag: str, owner: str) -> None:
        """下载落盘后转正：校验行归属与实际文件长度。"""
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM cache_entries WHERE key=? AND etag=?",
                    (key, etag),
                ).fetchone()
                if row is None:
                    self._rollback_safely(conn)
                    raise VersionGone(f"下载行已被回收：{key} {etag}")
                if row["state"] == STATE_COMPLETE:
                    self._rollback_safely(conn)
                    return
                if row["owner"] != owner:
                    self._rollback_safely(conn)
                    raise VersionGone(f"下载行归属他人：{row['owner']} != {owner}")
                actual = os.path.getsize(row["path"])
                if actual != row["size"]:
                    self._rollback_safely(conn)
                    raise VersionGone(
                        f"落盘长度 {actual} 与预留 {row['size']} 不一致"
                    )
                conn.execute(
                    """
                    UPDATE cache_entries
                    SET state='complete', tmp_path=NULL, last_used_at=?
                    WHERE key=? AND etag=?
                    """,
                    (time.time(), key, etag),
                )
                conn.execute("COMMIT")
            except Exception:
                self._rollback_safely(conn)
                raise

    def fail_download(self, key: str, etag: str, owner: str, tmp_path: str) -> None:
        """下载失败：先删残件，再释放预留。"""
        with self._fs_lock:
            self._safe_unlink(tmp_path)
            with closing(self._connect()) as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute(
                        "DELETE FROM cache_entries "
                        "WHERE key=? AND etag=? AND owner=? AND state='downloading'",
                        (key, etag, owner),
                    )
                    conn.execute("COMMIT")
                except Exception:
                    self._rollback_safely(conn)
                    raise

    # ----------------------------------------------------------------租用
    def acquire_lease(
        self, key: str, etag: str, lease_id: str, owner: str
    ) -> None:
        now = time.time()
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT state FROM cache_entries WHERE key=? AND etag=?",
                    (key, etag),
                ).fetchone()
                if row is None or row["state"] != STATE_COMPLETE:
                    self._rollback_safely(conn)
                    raise VersionGone(f"版本不可租用：{key} {etag}")
                conn.execute(
                    """
                    INSERT INTO cache_leases
                        (lease_id, key, etag, owner, created_at, heartbeat)
                    VALUES (?,?,?,?,?,?)
                    """,
                    (lease_id, key, etag, owner, now, now),
                )
                conn.execute(
                    "UPDATE cache_entries SET last_used_at=? WHERE key=? AND etag=?",
                    (now, key, etag),
                )
                conn.execute("COMMIT")
            except Exception:
                self._rollback_safely(conn)
                raise

    def release_lease(self, lease_id: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute("DELETE FROM cache_leases WHERE lease_id=?", (lease_id,))

    def heartbeat_lease(self, lease_id: str) -> bool:
        with closing(self._connect()) as conn:
            cur = conn.execute(
                "UPDATE cache_leases SET heartbeat=? WHERE lease_id=?",
                (time.time(), lease_id),
            )
            return cur.rowcount > 0

    # -------------------------------------------------------------维护/查询
    def reap(self) -> None:
        """清理僵死下载行（删残件）与过期 lease。任意存活进程都可执行。"""
        now = time.time()
        with self._fs_lock, closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                stale_dl = conn.execute(
                    "SELECT key, etag, tmp_path, path FROM cache_entries "
                    "WHERE state='downloading' AND heartbeat < ?",
                    (now - self.stale_download_ttl,),
                ).fetchall()
                for row in stale_dl:
                    if row["tmp_path"]:
                        self._safe_unlink(row["tmp_path"])
                    self._safe_unlink(row["path"])
                    conn.execute(
                        "DELETE FROM cache_entries WHERE key=? AND etag=?",
                        (row["key"], row["etag"]),
                    )
                conn.execute(
                    "DELETE FROM cache_leases WHERE heartbeat < ?",
                    (now - self.lease_ttl,),
                )
                conn.execute("COMMIT")
            except Exception:
                self._rollback_safely(conn)
                raise

    def get_entry(self, key: str, etag: str) -> Entry | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT e.*,
                       (SELECT COUNT(*) FROM cache_leases l
                         WHERE l.key=e.key AND l.etag=e.etag) AS leases
                FROM cache_entries e
                WHERE e.key=? AND e.etag=?
                """,
                (key, etag),
            ).fetchone()
            return self._to_entry(row) if row else None

    def snapshot(self) -> dict:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT e.*,
                       (SELECT COUNT(*) FROM cache_leases l
                         WHERE l.key=e.key AND l.etag=e.etag) AS leases
                FROM cache_entries e
                ORDER BY e.last_used_at DESC
                """
            ).fetchall()
            total = conn.execute(
                "SELECT COALESCE(SUM(size),0) FROM cache_entries"
            ).fetchone()[0]
            lease_count = conn.execute(
                "SELECT COUNT(*) FROM cache_leases"
            ).fetchone()[0]
        return {
            "limit_bytes": self.limit_bytes,
            "total_bytes": total,
            "lease_count": lease_count,
            "entries": [self._to_entry(r).__dict__ for r in rows],
        }

    @staticmethod
    def _to_entry(row: sqlite3.Row) -> Entry:
        return Entry(
            key=row["key"],
            etag=row["etag"],
            state=row["state"],
            path=row["path"],
            size=row["size"],
            owner=row["owner"],
            heartbeat=row["heartbeat"],
            last_used_at=row["last_used_at"],
            leases=row["leases"],
        )

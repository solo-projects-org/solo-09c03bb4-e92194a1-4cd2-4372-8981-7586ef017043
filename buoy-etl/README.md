# 浮标温盐观测提取平台（岸站对象库 → 离线质控）

海洋数据员从岸站对象库选取**一份日 Parquet**和一个 **UTC 秒时间窗**，
平台按版本固定取档、跨进程共享磁盘缓存，并以 **PyArrow 分批筛取 + 流式 CSV**
把窗口内的温度/盐度观测交给离线质控。

- 时间窗语义：**`[start, end)`，含起点、不含终点**。
- 日档固定三列：`utc_second`（UTC 秒，int64）、`temperature_c`、`salinity_psu`。
- 两个 Web 进程通过 **SQLite（WAL + 显式写事务）** 协调同一份共享磁盘缓存。
- 缓存口径：**下载预留 + 完整缓存合计上限 64 MiB**，单档超上限直接拒绝。

## 架构

```
                 ┌──────────────────────────────┐
 浏览器  ───────▶ │ nginx :8090（轮询，关闭缓冲）  │
                 └───────┬───────────────┬──────┘
                         ▼               ▼
                    web1 :8001       web2 :8002        FastAPI + Uvicorn
                  ┌───────────┐    ┌───────────┐
                  │ CacheMgr  │    │ CacheMgr  │      每进程独立 owner
                  └─────┬─────┘    └─────┬─────┘
                        └──────┬────────┘
                               ▼
                 SQLite 账本 cache.db（WAL，共享卷）
                 + 缓存文件目录（共享卷，64 MiB 合计上限）
                               │
                               ▼
                    object-source :8080（本地 HTTP 对象源）
                    强 ETag（SHA-256）+ Content-Length
```

### 关键承诺与实现位置

| 需求 | 实现 |
| --- | --- |
| 先取源端强 ETag 并固定该版 | `cache/manager.py::head_etag` 拒绝 `W/` 弱 ETag；查询入口先 HEAD 再取档 |
| 取档 ETag 不匹配即失败 | `cache/manager.py::_download` 校验 GET 响应 ETag 与长度，失配抛 `ETagMismatch` → HTTP 409 |
| 同档同版只下载一次 | `cache/coordination.py::reserve` 原子预留；后来者得到 `downloading` 并在条件变量/SQLite 轮询上等待 |
| 按源端全长预留，合计 64 MiB | `cache_entries.size` 在建档时即计入总量；`SingleFileTooLarge` → HTTP 413 |
| 空间不足只淘汰无人使用的完整文件 | `_ensure_capacity` 仅选无 lease 的 `complete` 行，按 `last_used_at` LRU；全在用抛 `CacheBusy` → HTTP 503 |
| 查询结束/断开释放占用，其他读者照常 | 每次查询一个 lease（`cache_leases` 表），生成器 `finally` 必释放；带心跳，僵死 lease 由 reaper 回收 |
| 慢源/慢下载不堵其他日档 | 阻塞 IO 全部 `asyncio.to_thread`；等待按 (key, etag) 隔离；PyArrow 批次逐块回到事件环 |
| 下载失败先删残件再释放预留 | `fail_download` 先 `unlink` 临时文件再删下载行；临时文件同目录 `fsync + rename`，读者只见完整文件 |
| 崩溃兜底 | 下载行/lease 带 owner + heartbeat；任意存活进程的 reaper 清理僵死行与残件 |
| PyArrow 分批筛取 + 流式 CSV | `web/query.py` 用 Dataset scanner 做行组统计下推，逐批 `write_csv` 产出字节块 |

## 一键启动（Docker Compose）

```bash
cd buoy-etl
docker compose up --build
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 网页（经 nginx 轮询两个 Web 进程） | http://localhost:8090 | 选日档、填 UTC 秒窗口、流式下载、实时缓存面板 |
| Web 进程直连 | http://localhost:8001 / http://localhost:8002 | 响应头 `X-Served-By` 标识实际实例 |
| 本地对象源 | http://localhost:8080/objects | 日档清单（ETag、全长、当日 UTC 范围） |

演示“缓存满”可用小容量启动：

```bash
CACHE_LIMIT_BYTES=4194304 docker compose up --build
```

## 本地无容器开发/测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest pytest-timeout pyyaml

# 1. 生成温盐样例（默认 4 天，10 秒采样，含缺失值与 corrected 素材）
python scripts/generate_samples.py --out ./sample_data/objects --days 4

# 2. 起对象源
DATA_DIR=./sample_data/objects PORT=8080 python -m object_source.server

# 3. 起两个 Web 进程（另开终端，换端口/标签，共用同一 SQLite 与缓存目录）
SQLITE_PATH=./sample_data/cache/c.db CACHE_DIR=./sample_data/cache/files \
OBJECT_SOURCE_URL=http://127.0.0.1:8080 PROCESS_LABEL=web-1 \
python -m uvicorn web.app:app --port 8001

# 测试（30 个用例：协调层、端到端、HTTP、对象源）
python -m pytest --timeout=120
```

## 三个演示场景

### 1. 并发查询 → 全集群只下载一次

```bash
KEY=buoy-st-ts-1735776000.parquet
for i in $(seq 1 6); do
  curl "http://localhost:8090/api/query?key=$KEY&start=1735776000&end=1735776300" -o c$i.csv &
done; wait
curl -s http://localhost:8001/api/cache | jq '.entries[] | {key,state,leases}'
# 对象源访问日志中该 key 只有 1 次 GET；6 份 CSV 内容一致
```

### 2. 缓存满 → 全在用提示忙，读者退出后 LRU 淘汰

```bash
# 用小容量实例（单档约 19 KiB，上限 25 KiB 只够一份）
CACHE_LIMIT_BYTES=25000 SQLITE_PATH=/tmp/s/c.db CACHE_DIR=/tmp/s/files \
python -m uvicorn web.app:app --port 8012 &

curl "http://localhost:8012/api/query?key=buoy-st-ts-1735689600.parquet&start=1735689600&end=1735689660"
# 用 Coordinator 持有第一档 lease 期间请求第二档 → HTTP 503「缓存忙…请稍后重试」
# 释放 lease 后再请求第二档 → 200，第一档被 LRU 淘汰
```

### 3. 源端更正 → 版本固定与 ETag 失配

```bash
KEY=buoy-st-ts-1735689600.parquet
# 用 corrected 素材原子替换对象（产生新 ETag）
python scripts/correct_object.py --key $KEY --source http://localhost:8080 \
  --data-dir ./sample_data/objects
# 新查询先 HEAD 固定新版，缓存内同 key 新旧 (key, etag) 共存
curl -D - "http://localhost:8090/api/query?key=$KEY&start=1735689600&end=1735689660" -o new.csv
```

### 附加：慢源、失败、断连

```bash
# 慢下载（每个 256 KiB 块后休眠），不影响其他日档查询
python scripts/simulate_source.py --key $KEY --delay 0.5
# 发送 1024 字节后中途断开 → API 返回 502，缓存目录无残件、预留已释放
python scripts/simulate_source.py --key $KEY --fail-after 1024
python scripts/simulate_source.py --key $KEY --clear   # 清除注入
```

## 目录结构

```
buoy-etl/
├── cache/
│   ├── coordination.py   # SQLite 账本：预留/淘汰/lease/reaper（跨进程互斥）
│   └── manager.py        # ETag 固定、下载协调、等待与失败清理
├── web/
│   ├── app.py            # FastAPI：查询接口、流式响应、断开检测、503/409/502 映射
│   ├── query.py          # PyArrow scanner 分批筛取 [start,end) + CSV 流式输出
│   └── templates/index.html  # 可操作网页（选档/窗口/下载/缓存面板）
├── object_source/server.py   # 纯标准库 HTTP 对象源（强 ETag、注入、原子更正）
├── scripts/              # 样例生成、更正、故障注入
├── nginx/nginx.conf      # 轮询 + 关闭代理缓冲（保证流式）
├── tests/                # 30 个 pytest 用例
├── Dockerfile / docker-compose.yml
└── requirements.txt
```

## 设计取舍

- **预留即占额**：下载行在创建时就按源端 `Content-Length` 计入总量，杜绝“下到一半把缓存撑爆”。
- **临时文件同目录 `fsync + os.replace`**：缓存目录里永远不存在半成品可读文件。
- **协调只依赖 SQLite**：不引入 Redis 等额外组件；WAL + `BEGIN IMMEDIATE` + `busy_timeout`
  保证两个（及更多）Web 进程互斥，写事务只在预留/转正/淘汰/租用等短操作内持有。
- **心跳 + reaper 而非分布式锁服务**：进程崩溃后，残件与占用由任意存活进程在 10 秒级回收。
- **HTTP 语义**：413 单档超限、503 缓存忙、409 版本失配/下载被放弃、502 源端传输失败、404 对象不存在。

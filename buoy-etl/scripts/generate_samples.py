"""生成浮标温盐日 Parquet 样例。

每份日档固定三列：
- ``utc_second``    ：UTC 秒（int64，单调递增、不重复）
- ``temperature_c`` ：海表温度（摄氏度，含少量缺失）
- ``salinity_psu``  ：实用盐度（PSU，含少量缺失）

同时写 ``manifest.json`` 记录每份日档的当日 UTC 秒范围，供网页预填演示窗口。
另生成一份 ``*.corrected.parquet`` 作为源端更正演示素材。
"""

from __future__ import annotations

import argparse
import json
import math
import os

import pyarrow as pa
import pyarrow.parquet as pq

DAY_SECONDS = 86_400
COLUMNS = ["utc_second", "temperature_c", "salinity_psu"]


def build_day(day_start_utc: int, step_seconds: int, seed: int) -> pa.Table:
    count = DAY_SECONDS // step_seconds
    utc = [day_start_utc + i * step_seconds for i in range(count)]

    temps, sals = [], []
    for i in range(count):
        phase = 2 * math.pi * (i * step_seconds) / DAY_SECONDS
        # 日变化 + 仪器噪声；每 211 个点留一个缺失，模拟真实浮标。
        t = 18.0 + 2.6 * math.sin(phase - 1.1) + 0.15 * math.sin(seed + i * 0.37)
        s = 34.7 + 0.25 * math.cos(phase) + 0.05 * math.cos(seed + i * 0.21)
        temps.append(None if i % 211 == 37 else round(t, 3))
        sals.append(None if i % 173 == 91 else round(s, 3))

    return pa.table(
        {
            "utc_second": pa.array(utc, type=pa.int64()),
            "temperature_c": pa.array(temps, type=pa.float64()),
            "salinity_psu": pa.array(sals, type=pa.float64()),
        }
    )


def write_parquet(table: pa.Table, path: str) -> None:
    pq.write_table(
        table,
        path,
        compression="snappy",
        # 显式行组大小，方便演示 PyArrow 行组过滤下推。
        row_group_size=2048,
        use_dictionary=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="生成温盐日 Parquet 样例")
    parser.add_argument("--out", default="/data/objects", help="输出目录")
    parser.add_argument("--days", type=int, default=4, help="生成天数（默认 4）")
    parser.add_argument(
        "--day-start",
        type=int,
        default=1735689600,  # 2025-01-01T00:00:00Z
        help="首日 00:00 UTC 秒",
    )
    parser.add_argument("--step", type=int, default=10, help="采样间隔秒（默认 10s）")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    manifest: dict[str, dict] = {}

    for d in range(args.days):
        day_start = args.day_start + d * DAY_SECONDS
        key = f"buoy-st-ts-{day_start}.parquet"
        table = build_day(day_start, args.step, seed=d + 1)
        path = os.path.join(args.out, key)
        write_parquet(table, path)
        manifest[key] = {
            "day_start_utc": day_start,
            "day_end_utc": day_start + DAY_SECONDS,
            "step_seconds": args.step,
            "rows": table.num_rows,
        }
        # 更正素材：同一日档、带漂移偏差的新版本，文件名带 .corrected。
        corrected = f"buoy-st-ts-{day_start}.corrected.parquet"
        ctable = build_day(day_start, args.step, seed=100 + d)
        write_parquet(ctable, os.path.join(args.out, corrected))
        print(f"wrote {key} ({table.num_rows} rows) + corrected 素材")

    with open(os.path.join(args.out, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    print(f"manifest -> {os.path.join(args.out, 'manifest.json')}")


if __name__ == "__main__":
    main()

"""Parquet 分批筛取与 CSV 流式输出。

日档固定三列：``utc_second``（UTC 秒，int64）、``temperature_c``、``salinity_psu``。
时间窗 ``[start, end)``：含起点、不含终点。筛选下推到 Parquet 行组，
逐批产出 CSV 字节，避免把整档读进内存。
"""

from __future__ import annotations

from io import BytesIO
from typing import Iterator

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq

REQUIRED_COLUMNS = ("utc_second", "temperature_c", "salinity_psu")


class SchemaMismatch(RuntimeError):
    pass


def _check_schema(pf: pq.ParquetFile) -> None:
    names = set(pf.schema_arrow.names)
    missing = [c for c in REQUIRED_COLUMNS if c not in names]
    if missing:
        raise SchemaMismatch(f"Parquet 缺少必需列：{', '.join(missing)}")


def stream_filtered_csv(
    path: str,
    start_utc_second: int,
    end_utc_second: int,
    batch_size: int = 65_536,
) -> Iterator[bytes]:
    """逐批筛取 ``[start, end)`` 窗口并产出 CSV 字节块。"""
    if end_utc_second <= start_utc_second:
        raise ValueError("时间窗必须满足 start < end（含起点、不含终点）")

    pf = pq.ParquetFile(path)
    _check_schema(pf)

    # 通过 Dataset scanner 做过滤下推：行组统计不重叠时整块跳过。
    expression = ds.field("utc_second") >= start_utc_second
    expression = expression & (ds.field("utc_second") < end_utc_second)
    dataset = ds.dataset(path, format="parquet")
    scanner = dataset.scanner(
        columns=list(REQUIRED_COLUMNS),
        filter=expression,
        batch_size=batch_size,
        use_threads=True,
    )

    header_written = False
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        # 行组过滤后仍可能有落在窗口外的行，逐批再做一次精确布尔过滤。
        mask = pa.compute.and_(
            pa.compute.greater_equal(batch.column("utc_second"), pa.scalar(start_utc_second)),
            pa.compute.less(batch.column("utc_second"), pa.scalar(end_utc_second)),
        )
        batch = batch.filter(mask)
        if batch.num_rows == 0:
            continue
        buf = BytesIO()
        pacsv.write_csv(
            batch,
            buf,
            write_options=pacsv.WriteOptions(include_header=not header_written),
        )
        header_written = True
        yield buf.getvalue()

    if not header_written:
        # 空结果也要给客户端一份带表头的 CSV。
        schema = pa.schema([pf.schema_arrow.field(c) for c in REQUIRED_COLUMNS])
        empty = pa.Table.from_batches([], schema=schema)
        buf = BytesIO()
        pacsv.write_csv(empty, buf)
        yield buf.getvalue()

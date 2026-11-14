"""把 *.corrected.parquet 推给对象源做原子更正，并可清理模拟注入。

用法：
  python scripts/correct_object.py --key buoy-st-ts-1735689600.parquet
  python scripts/clear_simulation.py --key ...   （见 clear_simulation.py）
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="触发对象源源端更正")
    parser.add_argument("--source", default=os.environ.get("OBJECT_SOURCE_URL", "http://localhost:8080"))
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data/objects"))
    parser.add_argument("--key", required=True, help="目标对象 key，如 buoy-st-ts-1735689600.parquet")
    args = parser.parse_args()

    corrected_path = os.path.join(args.data_dir, args.key.replace(".parquet", ".corrected.parquet"))
    if not os.path.exists(corrected_path):
        print(f"找不到更正素材：{corrected_path}", file=sys.stderr)
        return 2
    with open(corrected_path, "rb") as fh:
        payload = fh.read()

    req = urllib.request.Request(
        f"{args.source.rstrip('/')}/admin/correct/{args.key}",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/vnd.apache.parquet"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(resp.read().decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

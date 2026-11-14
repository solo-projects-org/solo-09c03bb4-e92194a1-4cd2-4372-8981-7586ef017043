"""向对象源注入慢下载/中途失败，或清除注入（演示用，纯标准库）。"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="对象源故障/延迟注入")
    parser.add_argument("--source", default=os.environ.get("OBJECT_SOURCE_URL", "http://localhost:8080"))
    parser.add_argument("--key", required=True)
    parser.add_argument("--delay", type=float, default=0.0, help="每个 256KiB 块后的休眠秒数")
    parser.add_argument("--fail-after", type=int, default=0, help="发送多少字节后中途断开；0 表示不断")
    parser.add_argument("--clear", action="store_true", help="清除该 key 的注入")
    args = parser.parse_args()

    base = args.source.rstrip("/")
    if args.clear:
        req = urllib.request.Request(f"{base}/admin/simulate/{args.key}", method="DELETE")
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(resp.read().decode())
        return 0

    body = json.dumps(
        {"key": args.key, "delay_per_chunk": args.delay, "fail_after_bytes": args.fail_after}
    ).encode()
    req = urllib.request.Request(
        f"{base}/admin/simulate",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        print(resp.read().decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

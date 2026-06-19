#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class SinkHandler(BaseHTTPRequestHandler):
    request_count = 0
    fail_first = 0
    delay_ms = 0
    output_path: Path

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        if type(self).delay_ms > 0:
            time.sleep(type(self).delay_ms / 1000)
        type(self).request_count += 1
        record = {
            "path": self.path,
            "count": type(self).request_count,
            "body": body,
        }
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("a", encoding="utf-8") as out:
            out.write(json.dumps(record, ensure_ascii=False) + "\n")

        if type(self).request_count <= type(self).fail_first:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b'{"ok":false,"retry":true}')
            return

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, _format: str, *_args: object) -> None:
        return


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19090)
    parser.add_argument("--out", default="/tmp/seastar-log-agent-fake-sink.ndjson")
    parser.add_argument("--fail-first", type=int, default=0)
    parser.add_argument("--delay-ms", type=int, default=0)
    args = parser.parse_args()

    SinkHandler.output_path = Path(args.out)
    SinkHandler.fail_first = args.fail_first
    SinkHandler.delay_ms = args.delay_ms
    server = ThreadingHTTPServer((args.host, args.port), SinkHandler)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Deterministic, bounded-memory HTTP Range fixture; no media or credentials needed."""
import argparse
import json
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def serve(args):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            start, end, code = 0, args.size - 1, args.status
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", self.headers.get("Range", ""))
            if match and not args.ignore_range and code == 200:
                start = int(match[1])
                end = min(int(match[2]) if match[2] else end, end)
                code = 206 if start < args.size else 416
            if args.delay_ms:
                time.sleep(args.delay_ms / 1000)
            self.send_response(code)
            self.send_header("ETag", '"deterministic-v1"')
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Type", "video/x-matroska")
            if code == 416:
                self.send_header("Content-Range", f"bytes */{args.size}")
            elif code == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{args.size}")
            length = end - start + 1 if code in (200, 206) else 0
            self.send_header("Content-Length", str(length))
            self.end_headers()
            sent = 0
            try:
                while sent < length:
                    n = min(65536, length - sent)
                    if args.truncate:
                        n = min(n, args.truncate - sent)
                        if n <= 0:
                            self.close_connection = True
                            break
                    self.wfile.write(bytes(((start + sent + i) % 251 for i in range(n))))
                    sent += n
            except (BrokenPipeError, ConnectionResetError):
                pass
            print(json.dumps(dict(status=code, start=start, requested=length, sent=sent)), flush=True)

    ThreadingHTTPServer((args.bind, args.port), Handler).serve_forever()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bind", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--size", type=int, default=5 * 1024**3 + 177)
    p.add_argument("--delay-ms", type=int, default=0)
    p.add_argument("--truncate", type=int, default=0)
    p.add_argument("--status", type=int, default=200)
    p.add_argument("--ignore-range", action="store_true")
    serve(p.parse_args())

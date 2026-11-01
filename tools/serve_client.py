"""Simple HTTP server to host the Conversa AI Live Microphone Client."""

import argparse
import http.server
import socketserver
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CLIENT_DIR = BASE_DIR / "client"


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(CLIENT_DIR), **kwargs)

    def log_message(self, format, *args):
        # Concise logging
        sys.stdout.write(f"[ClientServer] {self.address_string()} - {format % args}\n")
        sys.stdout.flush()


def run_server(port: int = 3000):
    handler = QuietHandler
    socketserver.TCPServer.allow_reuse_address = True
    try:
        with socketserver.TCPServer(("", port), handler) as httpd:
            print(f"Conversa AI Client running at: http://localhost:{port}")
            print(f"Serving files from: {CLIENT_DIR}")
            print("Press Ctrl+C to stop.")
            sys.stdout.flush()
            httpd.serve_forever()
    except OSError as e:
        print(f"Error starting server on port {port}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serve Conversa AI Web Client")
    parser.add_argument("--port", type=int, default=3000, help="HTTP port (default: 3000)")
    args = parser.parse_args()
    run_server(args.port)

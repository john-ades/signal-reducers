"""Run the normal CLI against live models with an explicitly loaded .env key."""

from __future__ import annotations

import asyncio
import sys

from _live_support import load_live_key

from signal_reducers.cli import execute, parser


def main():
    try:
        load_live_key()
        arguments = parser().parse_args(sys.argv[1:])
        code = asyncio.run(execute(arguments))
    except KeyboardInterrupt:
        code = 130
    except Exception as exc:
        # Do not print exceptions that could contain credential material.
        print({"error": getattr(exc, "code", type(exc).__name__)}, file=sys.stderr)
        code = 1
    raise SystemExit(code)


if __name__ == "__main__":
    main()

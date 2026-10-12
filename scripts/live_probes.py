"""Explicit live capability probes with an isolated Redis and private report."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from _live_support import load_live_key

from signal_reducers import cli
from signal_reducers.config import Config
from signal_reducers.demo import dedicated_redis, write_private
from signal_reducers.providers import OpenRouterClient


async def run(output: Path) -> int:
    source = load_live_key()
    clients = []

    def create_client(config):
        client = OpenRouterClient(config)
        clients.append(client)
        return client

    with dedicated_redis() as redis_url:
        result = await cli.readiness(
            Config(mode="live", redis_url=redis_url),
            probe=True,
            client_factory=create_client,
        )
    result.update(
        {
            "credential_source": source,
            "credentials_logged": False,
            "request_counts": [dict(client.counters) for client in clients],
            "calls": [call for client in clients for call in client.calls],
        }
    )
    write_private(output, result)
    print(
        {
            "report": str(output),
            "ready": result["ready"],
            "model_error": result.get("model_error"),
            "request_counts": result["request_counts"],
        }
    )
    return 0 if result["ready"] else 1


def main():
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument(
        "--output", type=Path, default=Path("outputs/live/probes.json")
    )
    try:
        code = asyncio.run(run(arguments.parse_args().output))
    except KeyboardInterrupt:
        code = 130
    except Exception as exc:
        print({"error": getattr(exc, "code", type(exc).__name__)}, file=sys.stderr)
        code = 1
    raise SystemExit(code)


if __name__ == "__main__":
    main()

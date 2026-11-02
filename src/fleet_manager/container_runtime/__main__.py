"""Run the isolated container gateway or delegate native maintenance commands."""

import asyncio
import sys

from fleet_manager.cli import peer_cli
from fleet_manager.container_runtime.runtime import healthcheck, run


def main():
    if sys.argv[1:] == ["--healthcheck"]:
        raise SystemExit(0 if asyncio.run(healthcheck()) else 1)
    if len(sys.argv) > 1:
        peer_cli.main()
    else:
        asyncio.run(run())


if __name__ == "__main__":
    main()

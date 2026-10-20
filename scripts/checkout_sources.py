#!/usr/bin/env python3
"""Create missing sibling checkouts at the committed integration pins."""

import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    sources = json.loads((ROOT / "sources.lock.json").read_text())
    for name, source in sources.items():
        destination = ROOT.parent / name
        if destination.exists():
            raise SystemExit(f"Refusing to overwrite existing checkout: {destination}")
        subprocess.run(["git", "init", str(destination)], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(destination),
                "remote",
                "add",
                "origin",
                f"https://github.com/{source['repository']}.git",
            ],
            check=True,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(destination),
                "fetch",
                "--depth",
                "1",
                "origin",
                source["revision"],
            ],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(destination), "checkout", "--detach", "FETCH_HEAD"],
            check=True,
        )


if __name__ == "__main__":
    main()

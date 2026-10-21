"""Native test-process orchestration shared by the matrix and controller tests."""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
LANGUAGES = ("python", "typescript", "rust")


def source_snapshot(checkout):
    """Record tracked and untracked inputs, including the newly authored peers."""
    checkout = Path(checkout)
    names = (
        subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            cwd=checkout,
        )
        .decode()
        .split("\0")
    )
    hashes = {
        name: hashlib.sha256((checkout / name).read_bytes()).hexdigest()
        for name in sorted(set(names))
        if name and (checkout / name).is_file()
    }
    return {
        "revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=checkout, text=True
        ).strip(),
        "status": subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=checkout, text=True
        ),
        "files": hashes,
    }


def peer_command(language):
    if language == "python":
        return [sys.executable, str(ROOT / "agents/python/action_peer.py")]
    if language == "typescript":
        return ["node", str(ROOT / "agents/typescript/action-peer.ts")]
    if language == "rust":
        return [str(ROOT / "agents/rust/target/debug/action_peer")]
    raise ValueError(language)


class Peer:
    def __init__(self, language, config, directory):
        self.language, self.config = language, config
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = asyncio.Lock()
        self.process = None
        self.ready = None
        self.closed = False

    async def start(self):
        self.stderr = (self.directory / "stderr.log").open("w")
        self.transcript = (self.directory / "stdio.jsonl").open("w")
        config = {**self.config, "specRoot": str(WORKSPACE / "deckr/contract/v1")}
        (self.directory / "config.json").write_text(json.dumps(config, indent=2))
        self.process = await asyncio.create_subprocess_exec(
            *peer_command(self.language),
            json.dumps(config),
            cwd=ROOT,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=self.stderr,
            limit=8 * 1024 * 1024,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        try:
            self.ready = await self.receive(timeout=45)
            assert self.ready.get("ready") is True, self.ready
        except BaseException:
            await self.stop(check=False)
            raise
        return self

    async def receive(self, timeout=20):
        line = await asyncio.wait_for(self.process.stdout.readline(), timeout)
        if not line:
            await self.process.wait()
            raise AssertionError(
                f"{self.language} peer exited ({self.process.returncode}); "
                f"{self.directory / 'stderr.log'}"
            )
        value = json.loads(line)
        self.transcript.write(json.dumps({"response": value}) + "\n")
        self.transcript.flush()
        return value

    async def command(self, op, *, allow_error=False, **values):
        async with self.lock:
            command = {"op": op, **values}
            self.transcript.write(json.dumps({"command": command}) + "\n")
            self.transcript.flush()
            self.process.stdin.write((json.dumps(command) + "\n").encode())
            await self.process.stdin.drain()
            result = await self.receive()
            if not allow_error:
                assert "error" not in result, (self.language, op, result)
            return result

    async def wait(self, predicate, *, timeout=15):
        async with asyncio.timeout(timeout):
            while True:
                state = await self.command("status")
                assert not state["errors"], state["errors"]
                if predicate(state):
                    return state
                await asyncio.sleep(0.02)

    async def delivered(self, message_id, timeout=15):
        state = await self.wait(
            lambda s: any(m["messageId"] == message_id for m in s["received"]),
            timeout=timeout,
        )
        return next(m for m in state["received"] if m["messageId"] == message_id)

    async def stop(self, *, check=True):
        if self.process is None or self.closed:
            return
        failure = None
        try:
            if self.process.returncode is None:
                await self.command("stop")
                self.process.stdin.close()
                await asyncio.wait_for(self.process.wait(), 15)
            if self.process.returncode != 0:
                failure = AssertionError(
                    f"{self.language} cleanup exited {self.process.returncode}"
                )
        except BaseException as exc:
            failure = exc
            if self.process.returncode is None:
                self.process.kill()
                await self.process.wait()
        finally:
            self.closed = True
            self.stderr.close()
            self.transcript.close()
        if failure is not None and check:
            raise failure


async def build_rust(directory):
    log = Path(directory) / "build.log"
    with log.open("w") as output:
        process = await asyncio.create_subprocess_exec(
            "mise",
            "exec",
            "--",
            "cargo",
            "build",
            "--locked",
            "--manifest-path",
            str(ROOT / "agents/rust/Cargo.toml"),
            "--bin",
            "action_peer",
            cwd=WORKSPACE / "deckr-core-rust",
            stdout=output,
            stderr=asyncio.subprocess.STDOUT,
        )
        code = await process.wait()
    assert code == 0, f"Rust peer build failed: {log}"

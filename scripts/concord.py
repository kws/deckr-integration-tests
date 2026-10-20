#!/usr/bin/env python3
"""Check three-language Concord lifecycles against an isolated native broker."""

import argparse
import json
import os
from pathlib import Path
import queue
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid

from smoke import ROOT, source_manifest, write_junit

WORKSPACE = ROOT.parent
LANGUAGES = ("rust", "python", "typescript")


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Peer:
    def __init__(self, name, command, url, prefix, directory):
        self.name = name
        self.output = queue.Queue()
        self.log = open(directory / f"{name}.log", "w+")
        self.process = subprocess.Popen(
            [*command, url, prefix],
            cwd=WORKSPACE / f"deckr-core-{name}",
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            text=True,
            bufsize=1,
        )

        def read():
            for line in self.process.stdout:
                self.output.put(line)
            self.output.put(None)

        threading.Thread(target=read, daemon=True).start()

    def receive(self, timeout=15):
        try:
            line = self.output.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(
                f"{self.name} did not respond within {timeout}s"
            ) from None
        if line is None:
            raise AssertionError(f"{self.name} exited with {self.process.poll()}")
        response = json.loads(line)
        assert "error" not in response, (self.name, response)
        return response

    def command(self, operation, **kwargs):
        self.process.stdin.write(json.dumps({"op": operation, **kwargs}) + "\n")
        self.process.stdin.flush()
        return self.receive()

    def stop(self):
        if self.process.poll() is None:
            try:
                self.command("stop")
                self.process.wait(timeout=10)
            except Exception:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()


def wait_for(peers, predicate, description, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        states = {name: peer.command("status") for name, peer in peers.items()}
        if predicate(states):
            return states
        time.sleep(0.1)
    raise AssertionError(f"Timed out: {description}: {json.dumps(states)}")


def selected(states, contract_id):
    return [
        next(
            (
                item
                for item in states[name]["contracts"]
                if item["contractId"] == contract_id
            ),
            {},
        )
        for name in LANGUAGES
    ]


def run(args, directory, checks):
    def passed(description):
        checks.append(description)
        print(f"PASS: {description}", flush=True)

    if not args.skip_build:
        cargo = shutil.which("cargo") or str(Path.home() / ".cargo/bin/cargo")
        with (directory / "build.log").open("w") as build_log:
            subprocess.run(
                [
                    cargo,
                    "build",
                    "--locked",
                    "--features",
                    "nats",
                    "--example",
                    "interop",
                ],
                cwd=WORKSPACE / "deckr-core-rust",
                stdout=build_log,
                stderr=subprocess.STDOUT,
                check=True,
            )
    broker_port = port()
    monitor_port = port()
    url = f"nats://127.0.0.1:{broker_port}"
    prefix = "concord_" + uuid.uuid4().hex
    broker = None
    broker_log = open(directory / "nats.log", "a")
    peers = {}

    def start_broker():
        return subprocess.Popen(
            [
                args.nats_server,
                "-js",
                "-a",
                "127.0.0.1",
                "-p",
                str(broker_port),
                "-m",
                str(monitor_port),
                "-sd",
                str(directory / "store"),
            ],
            stdout=broker_log,
            stderr=subprocess.STDOUT,
        )

    def stop_broker():
        nonlocal broker
        if broker and broker.poll() is None:
            broker.terminate()
            broker.wait(timeout=10)
        broker = None

    try:
        commands = {
            "rust": [str(WORKSPACE / "deckr-core-rust/target/debug/examples/interop")],
            "python": ["uv", "run", "--no-sync", "python", "scripts/interop_peer.py"],
            "typescript": ["node", "scripts/interop-peer.ts"],
        }
        for name, command in commands.items():
            peers[name] = Peer(name, command, url, prefix, directory)
        # The broker is deliberately absent. Every implementation must retry.
        time.sleep(1)
        assert all(peer.process.poll() is None for peer in peers.values()), (
            "initial connection failed instead of retrying"
        )
        broker = start_broker()
        for peer in peers.values():
            assert peer.receive() == {"ready": True}
        passed("all three peers retry initial connection until the broker starts")

        terms = {
            "float": 1.0,
            "integer": 9007199254740993,
            "negativeZero": -0.0,
            "nested": {"languageIndependent": True},
        }
        previous_ids = set()
        for owner in ("typescript", "rust", "python"):
            # Close old lifetimes before any new participant can see them.
            for peer in peers.values():
                peer.command("mode", mode="owner")
            for name, peer in peers.items():
                if name != owner:
                    peer.command("mode", mode="participant")
            contract_id = peers[owner].command("propose", terms=terms)["contractId"]
            assert contract_id not in previous_ids
            previous_ids.add(contract_id)
            states = wait_for(
                peers,
                lambda states: all(
                    item.get("valid") and item.get("authority")
                    for item in selected(states, contract_id)
                ),
                f"{owner} proposal becomes valid at every peer",
            )
            records = selected(states, contract_id)
            hashes = {item["termsHash"] for item in records} | {
                item["calculatedHash"] for item in records
            }
            assert len(hashes) == 1 and None not in hashes, records
            assert all(item["generation"] == 1 for item in records)
            counts = peers["typescript"].command("counts")
            if counts != {"contracts": 3, "tokens": 3}:
                print(json.dumps(peers["typescript"].command("consumers")), flush=True)
            assert counts == {"contracts": 3, "tokens": 3}, counts
            passed(
                f"{owner} proposal is attached by all languages with identical lossless numeric terms hashes"
            )
            if owner == "typescript":
                if not args.skip_renewal:
                    wait_for(
                        peers,
                        lambda states: all(
                            item.get("authority") and item.get("refreshSeq", 0) >= 2
                            for item in selected(states, contract_id)
                        ),
                        "automatic renewal in all three languages",
                        timeout=100,
                    )
                assert peers["typescript"].command("counts") == {
                    "contracts": 3,
                    "tokens": 3,
                }
                passed(
                    "consumer counts remain constant (renewal check skipped)"
                    if args.skip_renewal
                    else "all three automatic renewals preserve authority; consumer counts remain constant"
                )
                stop_broker()
                wait_for(
                    peers,
                    lambda states: all(
                        not state["current"]
                        and not any(item["authority"] for item in state["contracts"])
                        for state in states.values()
                    ),
                    "outage revokes every peer's authority",
                    timeout=20,
                )
                passed("broker loss revokes authority in all languages")
                broker = start_broker()
                states = wait_for(
                    peers,
                    lambda states: all(state["current"] for state in states.values()),
                    "shared sources recover",
                    timeout=30,
                )
                assert not any(
                    item["authority"]
                    for state in states.values()
                    for item in state["contracts"]
                ), states
                passed("sources recover without resurrecting old authority")
            peers[owner].command("mode", mode="owner")
            wait_for(
                peers,
                lambda states: all(
                    not item.get("authority") for item in selected(states, contract_id)
                ),
                "owner exit revokes peer authority",
            )
        for peer in peers.values():
            peer.command("mode", mode="owner")
        assert peers["typescript"].command("counts") == {"contracts": 3, "tokens": 3}
        passed(
            "owner exits revoke participants; all rounds keep one shared consumer per bucket and process"
        )
    finally:
        # Keep the broker available while clients delete their owned consumers.
        for peer in peers.values():
            peer.stop()
        stop_broker()
        broker_log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--nats-server",
        default=os.environ.get("NATS_SERVER_BINARY") or shutil.which("nats-server"),
        required=not (
            os.environ.get("NATS_SERVER_BINARY") or shutil.which("nats-server")
        ),
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Reuse the existing Rust interop binary",
    )
    parser.add_argument(
        "--skip-renewal",
        action="store_true",
        help="Quick smoke run; omits the automatic renewal assertion",
    )
    parser.add_argument(
        "--logs",
        type=Path,
        help="New evidence directory (default: artifacts/concord-<run-id>)",
    )
    parser.add_argument("--allow-source-changes", action="store_true")
    args = parser.parse_args()
    if not __debug__:
        parser.error("Run without -O: Concord assertions must be enabled")
    run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
    directory = (args.logs or ROOT / "artifacts" / f"concord-{run_id}").resolve()
    directory.mkdir(parents=True, exist_ok=False)
    print(f"Evidence: {directory}", flush=True)
    started = time.monotonic()
    summary = {
        "status": "fail",
        "languages": list(LANGUAGES),
        "renewalEnabled": not args.skip_renewal,
        "checks": [],
    }
    try:
        manifest = source_manifest(args.allow_source_changes)
        (directory / "sources.json").write_text(json.dumps(manifest, indent=2) + "\n")
        run(args, directory, summary["checks"])
        summary["status"] = "pass"
    except (Exception, KeyboardInterrupt) as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        print(f"FAIL: {summary['error']}", file=sys.stderr, flush=True)
    finally:
        summary["durationSeconds"] = round(time.monotonic() - started, 3)
        (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        write_junit(
            directory / "junit.xml",
            summary,
            [
                {
                    "id": "concord-without-renewal"
                    if args.skip_renewal
                    else "concord-full",
                    "language": "all",
                    "target": "concord",
                }
            ],
            suite_name="deckr-concord",
        )
    return 0 if summary["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())

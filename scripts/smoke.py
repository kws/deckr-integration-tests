#!/usr/bin/env python3
"""Run and independently verify the complete Compose interoperability matrix."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
LANGUAGES = ("typescript", "python", "rust")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def json_equal(actual, expected):
    """Compare JSON values without Python's true == 1 / false == 0 coercion."""
    if isinstance(expected, dict):
        return (
            isinstance(actual, dict)
            and actual.keys() == expected.keys()
            and all(json_equal(actual[key], value) for key, value in expected.items())
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(json_equal(a, b) for a, b in zip(actual, expected))
        )
    if isinstance(expected, bool) or isinstance(actual, bool):
        return type(actual) is type(expected) and actual == expected
    return actual == expected


def verify_results(records, ready, run_id, replicas, rounds, cases):
    """Verify identity, complete matrix coverage, each reply and every view."""
    clients = [item for item in ready if item["role"] == "client"]
    providers = [item for item in ready if item["role"] == "provider"]
    require(len(providers) == 3, "expected three provider readiness records")
    require(
        {item["language"] for item in providers} == set(LANGUAGES),
        "provider languages incomplete",
    )
    provider_sessions = {item["language"]: item["sessionId"] for item in providers}
    require(
        len(set(provider_sessions.values())) == 3, "provider sessions must be distinct"
    )
    expected_pairs = Counter(
        {(client, target): replicas for client in LANGUAGES for target in LANGUAGES}
    )
    require(
        Counter((item["language"], item["target"]) for item in clients)
        == expected_pairs,
        "readiness matrix incomplete",
    )
    require(
        len(records) == 9 * replicas, "result count does not match the complete matrix"
    )
    require(
        Counter((item["language"], item["target"]) for item in records)
        == expected_pairs,
        "result matrix incomplete",
    )
    ready_by_id = {item["id"]: item for item in clients}
    require(len(ready_by_id) == len(clients), "duplicate client identity")
    require(
        len({item["sessionId"] for item in clients}) == len(clients),
        "duplicate client session",
    )
    require(
        {item["id"] for item in records} == set(ready_by_id),
        "results do not match ready clients",
    )
    contracts = set()
    operations = set()
    for record in [*ready, *records]:
        require(record["runId"] == run_id, "stale or foreign run artifact")
    for record in records:
        require(
            record["role"] == "client" and record["status"] == "pass",
            f"failed participant: {record}",
        )
        initial = ready_by_id[record["id"]]
        require(
            record["sessionId"] == initial["sessionId"], "client restarted during smoke"
        )
        require(
            record["providerSession"]
            == provider_sessions[record["target"]]
            == initial["providerSession"],
            "wrong provider session",
        )
        contract = record["contract"]
        require(
            contract["generation"] == 1 and bool(contract["contractId"]),
            "invalid negotiated pointer",
        )
        require(contract["contractId"] not in contracts, "clients share an agreement")
        contracts.add(contract["contractId"])
        if "contract" in initial:
            require(initial["contract"] == contract, "agreement changed during smoke")
        observations = record["observations"]
        require(
            len(observations) == rounds * len(cases), "missing operation observations"
        )
        expected_cases = {(r, c) for r in range(rounds) for c in range(len(cases))}
        require(
            {(item["round"], item["caseIndex"]) for item in observations}
            == expected_cases,
            "missing or duplicate workload cases",
        )
        for item in observations:
            operation_id = f"{record['id']}-{item['round']}-{item['caseIndex']}"
            require(
                item["operationId"] == operation_id and operation_id not in operations,
                "duplicate or wrong operation identity",
            )
            operations.add(operation_id)
            expected = dict(
                operationId=operation_id,
                payload=cases[item["caseIndex"]],
                providerLanguage=record["target"],
                providerSession=record["providerSession"],
            )
            require(
                json_equal(item["reply"], expected),
                f"incorrect reply for {operation_id}",
            )
            require(
                json_equal(item["view"], expected),
                f"incorrect protected view for {operation_id}",
            )
    return {
        "pairs": 9,
        "clients": len(records),
        "operations": len(operations),
        "protectedViewReads": len(operations),
    }


def read_records(directory, kind):
    return [
        json.loads(path.read_text())
        for path in sorted(directory.glob(f"*.{kind}.json"))
    ]


def command(args, *, cwd=ROOT, env=None, timeout=60):
    return subprocess.check_output(
        args, cwd=cwd, env=env, text=True, stderr=subprocess.STDOUT, timeout=timeout
    )


def source_manifest(allow_changes):
    lock = json.loads((ROOT / "sources.lock.json").read_text())
    observed = {}
    for name, pinned in lock.items():
        checkout = ROOT.parent / name
        revision = command(["git", "rev-parse", "HEAD"], cwd=checkout).strip()
        status = command(["git", "status", "--porcelain"], cwd=checkout)
        diff = command(["git", "diff", "HEAD", "--binary"], cwd=checkout)
        if not allow_changes:
            require(
                revision == pinned["revision"] and not status,
                f"{name} differs from sources.lock.json; use --allow-source-changes for a recorded development run",
            )
        tree = hashlib.sha256()
        copied = [
            path
            for path in (checkout / "src").rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        ]
        copied += [
            checkout / filename
            for filename in (
                "Cargo.toml",
                "Cargo.lock",
                "package.json",
                "package-lock.json",
                "tsconfig.json",
                "tsconfig.build.json",
                "pyproject.toml",
                "uv.lock",
                "README.md",
                "LICENSE",
            )
            if (checkout / filename).is_file()
        ]
        for path in sorted(copied):
            tree.update(str(path.relative_to(checkout)).encode())
            tree.update(path.read_bytes())
        observed[name] = {
            "revision": revision,
            "status": status,
            "diffSha256": hashlib.sha256(diff.encode()).hexdigest(),
            "buildInputsSha256": tree.hexdigest(),
        }
        if name != "deckr":
            pin = "spec-revision" if name.endswith("rust") else ".deckr-spec-revision"
            require(
                (checkout / pin).read_text().strip() == lock["deckr"]["revision"],
                f"{name} uses a different specification pin",
            )
    return observed


def build_fingerprint(sources):
    digest = hashlib.sha256(json.dumps(sources, sort_keys=True).encode())
    inputs = [
        *ROOT.glob("agents/*/*.py"),
        *ROOT.glob("agents/*/*.ts"),
        *ROOT.glob("agents/*/package.json"),
        *ROOT.glob("agents/*/Cargo.*"),
        *ROOT.glob("agents/*/src/*.rs"),
        *ROOT.glob("docker/*.Dockerfile"),
        *ROOT.glob("protocol/*.json"),
        ROOT / "scripts/proxy.py",
    ]
    for path in sorted(inputs):
        digest.update(str(path.relative_to(ROOT)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


def compose_status(compose, env):
    raw = command([*compose, "ps", "--all", "--format", "json"], env=env)
    if not raw.strip():
        return []
    return (
        json.loads(raw)
        if raw.lstrip().startswith("[")
        else [json.loads(line) for line in raw.splitlines()]
    )


def check_processes(statuses, expected_clients, finished=False):
    clients = [item for item in statuses if item["Service"].startswith("client-")]
    providers = [item for item in statuses if item["Service"].startswith("provider-")]
    require(
        len(clients) == expected_clients and len(providers) == 3,
        "container matrix incomplete",
    )
    for item in statuses:
        client = item["Service"].startswith("client-")
        allowed = ("running", "exited") if client else ("running",)
        require(
            item["State"] in allowed,
            f"{item['Name']}: unexpected state {item['State']}",
        )
        if item["State"] == "exited":
            require(
                int(item["ExitCode"]) == 0,
                f"{item['Name']}: exited with code {item['ExitCode']}",
            )
        if client and finished:
            require(
                item["State"] == "exited",
                f"client did not finish cleanup: {item['Name']}",
            )
    return all(item["State"] == "exited" for item in clients)


def write_junit(path, summary, records, *, suite_name="deckr-smoke"):
    suite = ET.Element(
        "testsuite",
        name=suite_name,
        tests=str(len(records) if summary["status"] == "pass" else 1),
        failures="0" if summary["status"] == "pass" else "1",
    )
    if summary["status"] == "pass":
        for record in records:
            ET.SubElement(
                suite,
                "testcase",
                classname=f"{record['language']}.to.{record['target']}",
                name=record["id"],
            )
    else:
        case = ET.SubElement(
            suite, "testcase", classname="harness", name="complete-matrix"
        )
        ET.SubElement(case, "failure", message=summary["error"]).text = summary["error"]
    ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replicas", type=int, default=2, choices=range(1, 9))
    parser.add_argument("--rounds", type=int, default=2, choices=range(1, 11))
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument(
        "--no-build", action="store_true", help="reuse local worker images"
    )
    parser.add_argument("--allow-source-changes", action="store_true")
    args = parser.parse_args()
    require(args.timeout > 0, "timeout must be positive")
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    )
    output = ROOT / "artifacts" / run_id
    participants = output / "participants"
    participants.mkdir(parents=True)
    (output / "control").mkdir()
    project = f"deckr-smoke-{run_id.lower()}"
    env = {
        **os.environ,
        "ARTIFACT_DIR": str(output),
        "RUN_ID": run_id,
        "CLIENT_REPLICAS": str(args.replicas),
        "ROUNDS": str(args.rounds),
    }
    compose = [
        "docker",
        "compose",
        "--project-name",
        project,
        "--file",
        str(ROOT / "compose.yaml"),
    ]
    started = time.monotonic()
    summary = {
        "runId": run_id,
        "status": "fail",
        "replicasPerPair": args.replicas,
        "rounds": args.rounds,
    }
    records = []
    stack_attempted = False
    print(f"Run {run_id}; evidence: {output}", flush=True)
    try:
        manifest = source_manifest(args.allow_source_changes)
        env["BUILD_TAG"] = build_fingerprint(manifest)
        summary["buildTag"] = env["BUILD_TAG"]
        (output / "sources.json").write_text(json.dumps(manifest, indent=2))
        (output / "compose.yaml").write_text(command([*compose, "config"], env=env))
        if not args.no_build:
            print("Building the three language images…", flush=True)
            with (output / "build.log").open("w") as log:
                subprocess.run(
                    [*compose, "build", *(f"provider-{lang}" for lang in LANGUAGES)],
                    env=env,
                    cwd=ROOT,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=1800,
                )
        stack_attempted = True
        (output / "startup.log").write_text(
            command([*compose, "up", "--detach", "--no-build"], env=env, timeout=180)
        )
        deadline = time.monotonic() + args.timeout
        while True:
            statuses = compose_status(compose, env)
            check_processes(statuses, 9 * args.replicas)
            failures = [
                item
                for item in read_records(participants, "result")
                if item["status"] != "pass"
            ]
            require(
                not failures,
                "worker failed: "
                + "; ".join(
                    f"{item['id']}: {item.get('error', 'unknown error')}"
                    for item in failures
                ),
            )
            ready = read_records(participants, "ready")
            if len(ready) == 3 + 9 * args.replicas:
                break
            require(
                time.monotonic() < deadline,
                f"readiness deadline exceeded: {len(ready)} participants ready",
            )
            time.sleep(0.5)
        print(
            f"All 3 providers and {9 * args.replicas} clients ready; releasing workload barrier.",
            flush=True,
        )
        (output / "control" / "start").touch()
        while True:
            statuses = compose_status(compose, env)
            if check_processes(statuses, 9 * args.replicas):
                break
            require(
                time.monotonic() < deadline, "workload or cleanup deadline exceeded"
            )
            time.sleep(0.5)
        check_processes(statuses, 9 * args.replicas, finished=True)
        records = read_records(participants, "result")
        counts = verify_results(
            records,
            ready,
            run_id,
            args.replicas,
            args.rounds,
            json.loads((ROOT / "protocol/cases.json").read_text()),
        )
        summary.update(status="pass", **counts)
    except (Exception, KeyboardInterrupt) as exc:
        summary["error"] = str(exc)
        if isinstance(exc, subprocess.CalledProcessError) and exc.output:
            summary["commandOutput"] = exc.output
        print(f"FAIL: {exc}", file=sys.stderr, flush=True)
    finally:
        if stack_attempted:
            for filename, arguments in [
                ("containers.json", ["ps", "--all", "--format", "json"]),
                ("images.json", ["images", "--format", "json"]),
                ("compose.log", ["logs", "--no-color", "--timestamps"]),
                (
                    "jetstream.json",
                    [
                        "exec",
                        "-T",
                        "nats",
                        "wget",
                        "-qO-",
                        "http://127.0.0.1:8222/jsz?streams=true&consumers=true&config=true",
                    ],
                ),
            ]:
                try:
                    (output / filename).write_text(
                        command([*compose, *arguments], env=env)
                    )
                except Exception as exc:
                    (output / filename).write_text(f"Collection failed: {exc}\n")
            try:
                (output / "cleanup.log").write_text(
                    command(
                        [*compose, "down", "--volumes", "--remove-orphans"],
                        env=env,
                        timeout=90,
                    )
                )
            except Exception as exc:
                summary.update(status="fail", error=f"Stack cleanup failed: {exc}")
                print(f"FAIL: {summary['error']}", file=sys.stderr, flush=True)
        summary["durationSeconds"] = round(time.monotonic() - started, 3)
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        write_junit(output / "junit.xml", summary, records)
        if summary["status"] == "pass":
            print(
                f"PASS: {summary['clients']} clients, all 9 pairings, {summary['operations']} replies and protected views; stack removed.",
                flush=True,
            )
        print(f"Report: {output / 'summary.json'}", flush=True)
    return 0 if summary["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())

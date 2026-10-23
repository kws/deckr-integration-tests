#!/usr/bin/env python3
"""Run a timed, seeded soak and prove recovery using independent observations."""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import os
import random
import subprocess
import sys
import time
import uuid

from restart import verified_exit_handoff
from smoke import (
    ROOT,
    LANGUAGES,
    build_fingerprint,
    command,
    compose_status,
    json_equal,
    require,
    source_manifest,
    write_junit,
)


def atomic(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(value)
    temporary.replace(path)


def append(path, value):
    with path.open("a") as stream:
        stream.write(json.dumps(value) + "\n")


def participants(replicas):
    members = {}
    for language in LANGUAGES:
        identity = f"provider-{language}"
        members[identity] = dict(
            id=identity, role="provider", language=language, target=language
        )
    for language in LANGUAGES:
        for target in LANGUAGES:
            for replica in range(replicas):
                identity = f"client-{language}-{target}-{replica}"
                members[identity] = dict(
                    id=identity, role="client", language=language, target=target
                )
    return members


FAULT_CASES = (
    "client-disconnect",
    "client-crash",
    "provider-disconnect",
    "provider-crash",
)


def sweep(members, fault_case="all"):
    """Both fault types for every client pairing and every provider language."""
    selected = [
        identity
        for identity in members
        if identity.startswith("client-") and identity.endswith("-0")
    ]
    selected += [identity for identity in members if identity.startswith("provider-")]
    return [
        (identity, kind)
        for kind in ("disconnect", "crash")
        for identity in selected
        if fault_case == "all" or f"{members[identity]['role']}-{kind}" == fault_case
    ]


def choose_faults(rng, members, probabilities):
    """One candidate per role per tick; disjoint outcomes, no accumulated ticks."""
    result = []
    for role in ("client", "provider"):
        candidate = rng.choice(
            [identity for identity, member in members.items() if member["role"] == role]
        )
        draw = rng.random() * 100
        disconnect, crash = probabilities[role]
        if draw < disconnect:
            result.append((candidate, "disconnect"))
        elif draw < disconnect + crash:
            result.append((candidate, "crash"))
    return result


class Tail:
    """Read complete new records only, including across process restarts."""

    def __init__(self, path):
        self.path = path
        self.offset = 0
        self.pending = b""

    def read(self):
        if not self.path.exists():
            return []
        require(
            self.path.stat().st_size >= self.offset, f"evidence truncated: {self.path}"
        )
        with self.path.open("rb") as stream:
            stream.seek(self.offset)
            data = stream.read()
            self.offset = stream.tell()
        lines = (self.pending + data).split(b"\n")
        self.pending = lines.pop()
        require(len(self.pending) < 1_000_000, "oversized or corrupt event record")
        return [json.loads(line) for line in lines if line]


class Oracle:
    def __init__(self, members, run_id, cases):
        self.members, self.run_id, self.cases = members, run_id, cases
        self.boots = {}
        self.sessions = defaultdict(set)
        self.ready = set()
        self.latest = {}
        self.last_received = {}
        self.last_seq = {}
        self.counts = Counter()
        self.errors = Counter()
        self.epoch_cases = defaultdict(set)
        self.finished = set()
        self.contract_owners = {}
        self.retired = defaultdict(set)
        self.recoveries = []

    def accept(self, row, allowed, strict):
        identity = row["id"]
        require(identity in self.members, f"unknown participant: {identity}")
        member = self.members[identity]
        require(row["runId"] == self.run_id, "foreign run evidence")
        require(
            all(row[key] == member[key] for key in ("role", "language", "target")),
            "participant identity changed",
        )
        kind = row["kind"]
        if kind == "boot":
            if identity in self.boots:
                require(identity in allowed, f"unplanned restart: {identity}")
                require(
                    row["sessionId"] != self.boots[identity], "restart reused a session"
                )
            require(
                row["sessionId"]
                and not any(
                    row["sessionId"] in sessions for sessions in self.sessions.values()
                ),
                "process session was reused",
            )
            self.boots[identity] = row["sessionId"]
            self.sessions[identity].add(row["sessionId"])
            self.ready.discard(identity)
            return
        require(
            row["sessionId"] == self.boots.get(identity),
            "event from a retired process session",
        )
        if kind == "ready":
            self.ready.add(identity)
        elif kind == "result":
            require(
                row["status"] == "pass",
                f"participant failed: {identity}: {row.get('error')}",
            )
            self.finished.add(identity)
        elif kind == "unavailable":
            self.errors[identity] += 1
            require(
                not strict or identity in allowed,
                f"unexpected unavailability: {identity}: {row['error']}",
            )
        elif kind == "success":
            self.success(row, allowed, strict)
        elif kind not in ("execution", "providerError"):
            raise AssertionError(f"unknown event kind: {kind}")

    def success(self, row, allowed, strict):
        identity = row["id"]
        require(row["role"] == "client", "provider emitted client success")
        seq = row["seq"]
        key = (identity, row["sessionId"])
        require(
            type(seq) is int and seq > self.last_seq.get(key, -1),
            "duplicate or out-of-order success",
        )
        self.last_seq[key] = seq
        require(
            type(row["caseIndex"]) is int and row["caseIndex"] == seq % len(self.cases),
            "incorrect payload case",
        )
        operation_id = f"{identity}-{row['sessionId']}-{seq}"
        require(row["operationId"] == operation_id, "wrong operation identity")
        expected = dict(
            operationId=operation_id,
            payload=self.cases[row["caseIndex"]],
            providerLanguage=row["target"],
            providerSession=row["providerSession"],
        )
        require(json_equal(row["reply"], expected), f"incorrect reply: {operation_id}")
        require(
            json_equal(row["view"], expected),
            f"incorrect protected view: {operation_id}",
        )
        provider = f"provider-{row['target']}"
        require(
            row["providerSession"] == self.boots.get(provider)
            or (
                identity in allowed
                and row["providerSession"] in self.sessions[provider]
            ),
            f"wrong provider incarnation: {operation_id}",
        )
        pointer = row["contract"]
        require(
            type(pointer["generation"]) is int
            and pointer["generation"] == 1
            and isinstance(pointer["contractId"], str)
            and pointer["contractId"],
            "invalid successor agreement",
        )
        contract = pointer["contractId"]
        owner = (identity, row["sessionId"], row["providerSession"])
        require(
            self.contract_owners.setdefault(contract, owner) == owner,
            "agreement shared across sessions or clients",
        )
        require(contract not in self.retired[identity], "retired agreement resumed")
        previous = self.latest.get(identity)
        if previous and previous["contract"] != pointer:
            require(
                not strict or identity in allowed,
                f"unexpected agreement replacement: {identity}",
            )
            self.retired[identity].add(previous["contract"]["contractId"])
            if previous["sessionId"] == row["sessionId"]:
                require(
                    row.get("previousContract") == previous["contract"]
                    and row.get("oldAuthorityRejected") is True,
                    "old authority rejection was not demonstrated",
                )
        if previous and previous["sessionId"] != row["sessionId"]:
            require(previous["contract"] != pointer, "restart reused an agreement")
        if previous and any(
            previous[key] != row[key]
            for key in ("contract", "sessionId", "providerSession")
        ):
            # Recovery needs all payload cases under the successor authority.
            # A completed operation from an earlier lease cannot supply coverage.
            self.epoch_cases[(identity, row["epoch"])].clear()
        self.latest[identity] = row
        self.last_received[identity] = time.monotonic()
        self.counts[identity] += 1
        self.epoch_cases[(identity, row["epoch"])].add(row["caseIndex"])

    def recovered(self, fault, epoch):
        for identity, before in fault["before"].items():
            row = self.latest.get(identity)
            if (
                row is None
                or row["epoch"] != epoch
                or len(self.epoch_cases[(identity, epoch)]) < len(self.cases)
            ):
                return False
            if row["providerSession"] != self.boots.get(f"provider-{row['target']}"):
                return False
            require(
                row["contract"] != before["contract"],
                f"fault recovery reused authority: {identity}",
            )
            crashed_client = (
                fault["kind"] == "crash" and fault["participant"] == identity
            )
            require(
                (row["sessionId"] != before["sessionId"]) == crashed_client,
                f"incorrect client restart behavior: {identity}",
            )
            crashed_provider = fault["kind"] == "crash" and fault["role"] == "provider"
            require(
                (row["providerSession"] != before["providerSession"])
                == crashed_provider,
                f"incorrect provider restart behavior: {identity}",
            )
        return True

    def summary(self):
        pairs = Counter()
        coverage = defaultdict(set)
        for identity, count in self.counts.items():
            member = self.members[identity]
            pairs[f"{member['language']}->{member['target']}"] += count
        for fault in self.recoveries:
            for identity in fault["before"]:
                member = self.members[identity]
                coverage[f"{member['language']}->{member['target']}"].add(
                    f"{fault['role']}-{fault['kind']}"
                )
        return dict(
            operations=sum(self.counts.values()),
            protectedViewReads=sum(self.counts.values()),
            pairs=dict(pairs),
            faultsRecovered=len(self.recoveries),
            recoveryCoverage={key: sorted(value) for key, value in coverage.items()},
            unavailableAttempts=sum(self.errors.values()),
        )


class Runner:
    def __init__(self, args):
        self.args = args
        self.run_id = (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-")
            + uuid.uuid4().hex[:8]
        )
        self.output = ROOT / "artifacts" / f"soak-{self.run_id}"
        self.events = self.output / "participants"
        self.control = self.output / "control"
        self.events.mkdir(parents=True)
        self.control.mkdir()
        self.members = participants(args.replicas)
        self.clients = {
            identity
            for identity, row in self.members.items()
            if row["role"] == "client"
        }
        self.oracle = Oracle(
            self.members,
            self.run_id,
            json.loads((ROOT / "protocol/cases.json").read_text()),
        )
        self.tails = {
            identity: Tail(self.events / f"{identity}.events.jsonl")
            for identity in self.members
        }
        self.epoch = 0
        self.revision = 0
        self.strict = False
        self.allowed = set()
        self.dead = set()
        self.next_status = self.next_resource = 0
        self.stopping = False
        self.started = False
        self.env = {
            **os.environ,
            "ARTIFACT_DIR": str(self.output),
            "RUN_ID": self.run_id,
        }
        self.compose = [
            "docker",
            "compose",
            "--project-name",
            f"deckr-soak-{self.run_id.lower()}",
            "--file",
            str(self.output / "compose.json"),
        ]

    def log(self, event, **data):
        append(
            self.output / "timeline.jsonl", dict(event=event, at=time.time(), **data)
        )

    def bump_epoch(self):
        self.epoch += 1
        atomic(self.control / "epoch", str(self.epoch))
        return self.epoch

    def setup(self):
        sources = source_manifest(self.args.allow_source_changes)
        self.env["BUILD_TAG"] = build_fingerprint(sources)
        (self.output / "sources.json").write_text(json.dumps(sources, indent=2))
        (self.output / "settings.json").write_text(
            json.dumps(vars(self.args), indent=2)
        )
        base = json.loads(
            command(
                [
                    "docker",
                    "compose",
                    "--file",
                    str(ROOT / "compose.yaml"),
                    "config",
                    "--format",
                    "json",
                ],
                env=self.env,
            )
        )
        services = dict(nats=base["services"]["nats"])
        routes = {}
        for index, (identity, member) in enumerate(self.members.items()):
            template = (
                f"provider-{member['language']}"
                if member["role"] == "provider"
                else f"client-{member['language']}-{member['target']}"
            )
            service = json.loads(json.dumps(base["services"][template]))
            service.pop("deploy", None)
            service.pop("scale", None)
            routes[identity] = 20000 + index
            service["environment"].update(
                MODE="soak",
                DECKR_INSTANCE_ID=identity,
                DECKR_RESTART_HANDOFF=f"/control/{identity}.restart.json",
                PARTICIPANT_ID=identity,
                NATS_URL=f"nats://proxy:{routes[identity]}",
                OPERATION_INTERVAL=str(self.args.operation_interval),
                OPERATION_TIMEOUT=str(self.args.operation_timeout),
            )
            service["depends_on"]["proxy"] = dict(
                condition="service_healthy", required=True
            )
            services[identity] = service
        services["proxy"] = dict(
            image=f"deckr-integration-proxy:{self.env['BUILD_TAG']}",
            build=dict(context=str(ROOT), dockerfile="docker/proxy.Dockerfile"),
            init=True,
            restart="no",
            depends_on=dict(nats=dict(condition="service_healthy")),
            volumes=[f"{self.control}:/control:ro", f"{self.events}:/artifacts"],
            healthcheck=dict(
                test=[
                    "CMD",
                    "python",
                    "-c",
                    "import pathlib; assert pathlib.Path('/artifacts/proxy.json').exists()",
                ],
                interval="1s",
                timeout="2s",
                retries=30,
            ),
        )
        base["services"] = services
        base.pop("name", None)
        for section in ("volumes", "networks"):
            for resource in base.get(section, {}).values():
                require(not resource.get("external"), "soak resources must be isolated")
                resource.pop("name", None)
        (self.output / "compose.json").write_text(json.dumps(base, indent=2))
        (self.control / "routes.json").write_text(json.dumps(routes))
        self.set_proxy([])
        self.bump_epoch()
        if not self.args.no_build:
            print("Building language workers and fault proxy…", flush=True)
            with (self.output / "build.log").open("w") as log:
                subprocess.run(
                    [
                        *self.compose,
                        "build",
                        "proxy",
                        *(f"provider-{language}" for language in LANGUAGES),
                    ],
                    cwd=ROOT,
                    env=self.env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=1800,
                )
        self.started = True
        (self.output / "startup.log").write_text(
            command(
                [*self.compose, "up", "-d", "--no-build"], env=self.env, timeout=180
            )
        )

    def set_proxy(self, disabled):
        self.revision += 1
        atomic(
            self.control / "faults.json",
            json.dumps(dict(revision=self.revision, disabled=disabled)),
        )

    def proxy(self):
        path = self.events / "proxy.json"
        deadline = time.monotonic() + 1
        while True:
            try:
                return json.loads(path.read_text())
            except FileNotFoundError:
                if time.monotonic() >= deadline:
                    return {}
                time.sleep(0.01)

    def pump(self):
        # Boot records precede observations from peers, including after restart.
        batch = [row for tail in self.tails.values() for row in tail.read()]
        for row in sorted(batch, key=lambda item: item["kind"] != "boot"):
            self.oracle.accept(row, self.allowed, self.strict)
        now = time.monotonic()
        if now >= self.next_status:
            statuses = compose_status(self.compose, self.env)
            require(
                len(statuses) == len(self.members) + 2, "container matrix incomplete"
            )
            for row in statuses:
                identity = row["Service"]
                if identity in self.dead:
                    require(
                        row["State"] in ("running", "exited"),
                        f"fault target in unexpected state: {row}",
                    )
                elif self.stopping and identity in self.clients:
                    require(
                        row["State"] == "running"
                        or (row["State"] == "exited" and row["ExitCode"] == 0),
                        f"client cleanup failed: {row}",
                    )
                else:
                    require(
                        row["State"] == "running",
                        f"unplanned container exit: {identity} ({row['ExitCode']})",
                    )
            self.next_status = now + 2
        if self.strict and not self.stopping:
            for identity in self.clients - self.allowed:
                require(
                    now - self.oracle.last_received.get(identity, now)
                    <= self.args.stall_timeout,
                    f"healthy client stopped progressing: {identity}",
                )
        if now >= self.next_resource:
            self.sample_resources()
            self.next_resource = now + self.args.sample_interval

    def sample_resources(self):
        ids = command([*self.compose, "ps", "--quiet"], env=self.env).split()
        if not ids:
            return
        raw = command(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
            timeout=30,
        )
        rows = [json.loads(line) for line in raw.splitlines() if line]
        for row in rows:
            used = row["MemUsage"].split("/")[0].strip()
            row["memoryMiB"] = memory_mib(used)
        append(self.output / "resources.jsonl", dict(at=time.time(), containers=rows))

    def wait(self, predicate, timeout, description):
        deadline = time.monotonic() + timeout
        while True:
            self.pump()
            if predicate():
                return
            require(time.monotonic() < deadline, f"{description} timed out")
            time.sleep(0.1)

    def delay(self, seconds):
        deadline = time.monotonic() + seconds
        self.wait(
            lambda: time.monotonic() >= deadline, seconds + 30, "controller delay"
        )

    def baseline(self):
        self.wait(
            lambda: self.oracle.ready == set(self.members),
            self.args.recovery_timeout,
            "initial readiness",
        )
        (self.control / "start").touch()
        self.wait(
            lambda: all(
                len(self.oracle.epoch_cases[(identity, self.epoch)])
                == len(self.oracle.cases)
                for identity in self.clients
            ),
            self.args.recovery_timeout,
            "baseline matrix",
        )
        self.strict = True
        print(
            f"Baseline ready: {len(self.clients)} clients, all nine pairings.",
            flush=True,
        )

    def fault(self, identity, kind):
        self.pump()
        member = self.members[identity]
        affected = (
            {identity}
            if member["role"] == "client"
            else {
                client
                for client in self.clients
                if self.members[client]["target"] == member["language"]
            }
        )
        self.allowed = affected | {identity}
        fault = dict(
            participant=identity,
            kind=kind,
            role=member["role"],
            before={client: self.oracle.latest[client] for client in affected},
            startedAt=time.time(),
            oldSession=self.oracle.boots[identity],
        )
        self.log(
            "fault-start",
            participant=identity,
            kind=kind,
            affected=sorted(affected),
            before={
                client: dict(
                    sessionId=row["sessionId"],
                    providerSession=row["providerSession"],
                    contract=row["contract"],
                )
                for client, row in fault["before"].items()
            },
        )
        self.bump_epoch()
        print(f"Inject {kind}: {identity}", flush=True)
        if kind == "disconnect":
            status = self.proxy()
            require(
                status["connections"][identity] > 0,
                f"fault target has no proxy connection: {identity}",
            )
            closed = status["closed"][identity]
            self.set_proxy([identity])
            self.wait(
                lambda: self.proxy().get("revision") == self.revision,
                10,
                "proxy disable acknowledgement",
            )
            status = self.proxy()
            require(
                status["connections"][identity] == 0
                and status["closed"][identity] > closed,
                "proxy did not close a real connection",
            )
            self.log("fault-injected", participant=identity, kind=kind, proxy=status)
        else:
            self.dead.add(identity)
            container = command(
                [*self.compose, "ps", "--quiet", identity], env=self.env
            ).strip()
            before_process = json.loads(
                command(["docker", "inspect", container], env=self.env)
            )[0]
            diagnostics = command(
                [
                    "docker",
                    "logs",
                    "--since",
                    before_process["State"]["StartedAt"],
                    container,
                ],
                env=self.env,
            )
            sessions = []
            for line in diagnostics.splitlines():
                try:
                    report = json.loads(line)
                except ValueError:
                    continue
                if report.get("event") == "endpoint-session":
                    sessions.append(
                        {key: report[key] for key in ("endpoint", "sessionId")}
                    )
            endpoint = (
                f"service:interop-{member['language']}"
                if member["role"] == "provider"
                else f"controller:{identity}"
            )
            require(
                sessions == [dict(endpoint=endpoint, sessionId=fault["oldSession"])],
                "process-local session diagnostics do not match the fault target",
            )
            command(
                [*self.compose, "kill", "--signal", "SIGKILL", identity], env=self.env
            )
            state = next(
                row
                for row in compose_status(self.compose, self.env)
                if row["Service"] == identity
            )
            require(
                state["State"] == "exited" and state["ExitCode"] == 137,
                "crash was not a SIGKILL exit",
            )
            after_process = json.loads(
                command(["docker", "inspect", container], env=self.env)
            )[0]
            path = self.control / f"{identity}.restart.json"
            previous = json.loads(path.read_text()) if path.exists() else None
            proof = verified_exit_handoff(
                identity,
                before_process,
                after_process,
                sessions,
                previous,
            )
            atomic(path, json.dumps(proof))
            self.log(
                "fault-injected",
                participant=identity,
                kind=kind,
                container=state,
                restartHandoff=proof,
            )
        self.delay(self.args.downtime)
        if kind == "disconnect":
            self.set_proxy([])
            self.wait(
                lambda: self.proxy().get("revision") == self.revision,
                10,
                "proxy restore acknowledgement",
            )
        else:
            command([*self.compose, "start", identity], env=self.env)
            self.dead.remove(identity)
        restored = time.monotonic()
        epoch = self.bump_epoch()
        self.log("fault-restored", participant=identity, kind=kind, epoch=epoch)
        self.wait(
            lambda: self.oracle.recovered(fault, epoch),
            self.args.recovery_timeout,
            f"{kind} recovery for {identity}",
        )
        if kind == "disconnect":
            require(
                self.oracle.boots[identity] == fault["oldSession"],
                "disconnect restarted the application",
            )
        else:
            require(
                self.oracle.boots[identity] != fault["oldSession"],
                "crash did not create a fresh session",
            )
        fault["recoverySeconds"] = time.monotonic() - restored
        if kind == "crash":
            require(
                fault["recoverySeconds"] < 30,
                "verified restart exceeded the pre-expiry recovery budget (30s)",
            )
        self.oracle.recoveries.append(fault)
        self.log(
            "fault-recovered",
            participant=identity,
            kind=kind,
            recoverySeconds=fault["recoverySeconds"],
            epoch=epoch,
            after={client: self.oracle.latest[client] for client in affected},
        )
        self.allowed = set()
        # All cases from previous epochs are already preserved in the event stream.
        self.oracle.epoch_cases = defaultdict(
            set,
            {
                key: value
                for key, value in self.oracle.epoch_cases.items()
                if key[1] == epoch
            },
        )

    def run(self):
        self.setup()
        self.baseline()
        if self.args.fault_sweep:
            for identity, kind in sweep(self.members, self.args.fault_case):
                self.fault(identity, kind)
            coverage = self.oracle.summary()["recoveryCoverage"]
            required = (
                set(FAULT_CASES)
                if self.args.fault_case == "all"
                else {self.args.fault_case}
            )
            require(
                len(coverage) == 9
                and all(set(value) == required for value in coverage.values()),
                "incomplete recovery sweep",
            )
        rng = random.Random(self.args.seed)
        probabilities = dict(
            client=(
                self.args.client_disconnect_percent,
                self.args.client_crash_percent,
            ),
            provider=(
                self.args.service_disconnect_percent,
                self.args.service_crash_percent,
            ),
        )
        deadline = time.monotonic() + self.args.duration
        next_fault = time.monotonic() + self.args.fault_interval
        next_report = 0
        print(f"Soak for {self.args.duration:g}s; seed {self.args.seed}.", flush=True)
        while time.monotonic() < deadline:
            self.pump()
            if time.monotonic() >= next_fault:
                decisions = choose_faults(rng, self.members, probabilities)
                self.log("random-tick", selected=decisions)
                for identity, kind in decisions:
                    self.fault(identity, kind)
                next_fault = time.monotonic() + self.args.fault_interval
            if time.monotonic() >= next_report:
                print(
                    f"Progress: {sum(self.oracle.counts.values())} operations, {len(self.oracle.recoveries)} verified recoveries.",
                    flush=True,
                )
                next_report = time.monotonic() + 30
            time.sleep(0.1)
        epoch = self.bump_epoch()
        self.wait(
            lambda: all(
                len(self.oracle.epoch_cases[(identity, epoch)])
                == len(self.oracle.cases)
                for identity in self.clients
            ),
            self.args.recovery_timeout,
            "final healthy matrix",
        )
        self.stopping = True
        (self.control / "stop").touch()
        self.wait(
            lambda: self.oracle.finished == self.clients
            and all(
                row["State"] == "exited" and row["ExitCode"] == 0
                for row in compose_status(self.compose, self.env)
                if row["Service"] in self.clients
            ),
            45,
            "client cleanup",
        )
        self.pump()
        require(len(self.oracle.summary()["pairs"]) == 9, "final matrix incomplete")
        return self.oracle.summary()

    def cleanup(self):
        if not self.started:
            return
        if not self.stopping:
            try:
                python_services = [
                    identity
                    for identity, row in self.members.items()
                    if row["language"] == "python" and identity not in self.dead
                ]
                command(
                    [*self.compose, "kill", "--signal", "SIGUSR1", *python_services],
                    env=self.env,
                )
                time.sleep(0.2)
            except Exception:
                pass
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
                (self.output / filename).write_text(
                    command([*self.compose, *arguments], env=self.env)
                )
            except Exception as error:
                (self.output / filename).write_text(f"Collection failed: {error}")
        (self.output / "cleanup.log").write_text(
            command(
                [*self.compose, "down", "--volumes", "--remove-orphans"],
                env=self.env,
                timeout=90,
            )
        )


def memory_mib(text):
    units = {
        "B": 1 / 1024**2,
        "kB": 1000 / 1024**2,
        "KiB": 1 / 1024,
        "MB": 1000**2 / 1024**2,
        "MiB": 1,
        "GB": 1000**3 / 1024**2,
        "GiB": 1024,
    }
    for unit in sorted(units, key=len, reverse=True):
        if text.endswith(unit):
            return float(text[: -len(unit)]) * units[unit]
    raise ValueError(f"unknown memory unit: {text}")


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration",
        type=float,
        default=3600,
        help="seconds of steady/random soak after the optional sweep",
    )
    parser.add_argument("--replicas", type=int, choices=range(1, 9), default=2)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--fault-sweep",
        action="store_true",
        help="guarantee all four recovery categories for all nine pairings",
    )
    parser.add_argument(
        "--fault-case",
        choices=("all", *FAULT_CASES),
        default="all",
        help="limit the sweep to one recovery category; all nine pairings remain required",
    )
    for role in ("client", "service"):
        for kind in ("disconnect", "crash"):
            parser.add_argument(f"--{role}-{kind}-percent", type=float, default=0)
    for name, default in [
        ("fault-interval", 10),
        ("downtime", 3),
        ("recovery-timeout", 180),
        ("stall-timeout", 45),
        ("operation-timeout", 10),
        ("operation-interval", 1),
        ("sample-interval", 30),
    ]:
        parser.add_argument(f"--{name}", type=float, default=default)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--allow-source-changes", action="store_true")
    args = parser.parse_args(argv)
    import math

    require(
        args.fault_sweep or args.fault_case == "all",
        "--fault-case requires --fault-sweep",
    )
    for name, value in vars(args).items():
        if isinstance(value, float):
            require(
                math.isfinite(value)
                and (value >= 0 if name.endswith("_percent") else value > 0),
                f"invalid {name}",
            )
    for role in ("client", "service"):
        require(
            getattr(args, f"{role}_disconnect_percent")
            + getattr(args, f"{role}_crash_percent")
            <= 100,
            f"{role} fault percentages exceed 100",
        )
    return args


def main():
    runner = Runner(arguments())
    start = time.monotonic()
    summary = dict(
        runId=runner.run_id,
        status="fail",
        clients=len(runner.clients),
        seed=runner.args.seed,
        faultCase=runner.args.fault_case,
    )
    print(f"Soak {runner.run_id}; evidence: {runner.output}", flush=True)
    try:
        summary.update(runner.run(), status="pass")
    except (Exception, KeyboardInterrupt) as error:
        summary.update(runner.oracle.summary())
        summary["error"] = str(error) or type(error).__name__
        runner.log("run-failed", error=summary["error"])
        (runner.output / "last-observations.json").write_text(
            json.dumps(runner.oracle.latest, indent=2)
        )
        print(f"FAIL: {summary['error']}", file=sys.stderr, flush=True)
    finally:
        try:
            runner.cleanup()
        except Exception as error:
            summary.update(
                status="fail",
                error=f"Stack cleanup failed: {error}; prior error: {summary.get('error')}",
            )
        summary["durationSeconds"] = round(time.monotonic() - start, 3)
        (runner.output / "summary.json").write_text(
            json.dumps(summary, indent=2) + "\n"
        )
        write_junit(
            runner.output / "junit.xml",
            summary,
            [
                dict(id=identity, **{key: row[key] for key in ("language", "target")})
                for identity, row in runner.members.items()
                if identity in runner.clients
            ],
            suite_name=f"deckr-soak-{runner.args.fault_case}",
        )
        print(
            f"{summary['status'].upper()}: {runner.output / 'summary.json'}", flush=True
        )
    return 0 if summary["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())

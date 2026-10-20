# Deckr integration tests

Real-broker interoperability tests for the TypeScript, Python and Rust Deckr
implementations. Docker Compose runs one service per language and, by default,
two independent client containers for every client/service pairing: 3 services,
18 clients and one JetStream-enabled NATS server.

## Run the smoke matrix

Requirements: Docker with Compose 2.20+ (including additional build contexts),
Python 3.9+, and these sibling checkouts:

```text
deckr/
deckr-core-typescript/
deckr-core-python/
deckr-core-rust/
deckr-integration-tests/
```

`sources.lock.json` records the exact implementation and specification commits.
The runner checks the commits and shared specification pins before starting.
Language dependencies are installed inside the images using committed lockfiles;
the host does not need Node, Rust, uv, or the Python Deckr package.

From this repository:

```sh
python3 scripts/smoke.py
```

The first run builds three worker images and downloads the pinned base images.
The runner creates a unique Compose project and broker volume, waits for every
provider and negotiated client, releases a shared workload barrier, verifies all
results and client exit codes, and removes its containers/network/volume.
No broker or monitoring ports are published to the host.

Useful variations:

```sh
# Nine clients, one for each pairing.
python3 scripts/smoke.py --replicas 1

# Iterate against changed sibling implementation checkouts, recording revisions.
python3 scripts/smoke.py --allow-source-changes

# Reuse worker images after a successful build; rebuild after changing source.
python3 scripts/smoke.py --no-build
```

`--rounds` defaults to two repetitions of the three shared payload cases per
client. `--timeout` defaults to 180 seconds for readiness, workload and client
cleanup, starting after Compose startup. Build and stack startup have separate
bounds. All three languages are required; there is no silently skipped matrix.

## What the smoke test proves

Each worker uses its language's public managed service implementation. Clients
select `interop-typescript`, `interop-python`, or `interop-rust` through Beacon,
then negotiate a termless service-use agreement through Concord. Each client
has a unique endpoint/session, and the runner checks distinct agreement IDs.

The common application declaration is `protocol/service.json`. For each case
in `protocol/cases.json`, the provider publishes a contract-scoped protected
view and responds to an `echo` query with the operation ID, original payload,
provider language and provider session. The client checks the reply and reads
the same value through its authorized view API. Cases cover nested objects,
arrays, null, Unicode, safe numeric boundaries and prototype-like JSON keys.

The runner independently checks the actual observations against the shared
cases, the expected provider session and the complete 3 × 3 matrix. A missing
result, duplicate identity, wrong provider, incorrect payload, nonzero process
exit or unfinished client cleanup fails the run. Participants do not restart
automatically during smoke, so a crash cannot be hidden by a restart policy.

The smoke workload does not prove event delivery, reverse request/reply,
or view-watch recovery. The soak runner below adds continuous request/view
traffic and real disconnect/crash recovery checks.
The preceding `mixed-language` Concord harness in the parent workspace remains
a separate test; this suite does not replace its renewal and recovery checks.

## Soak and recovery

```sh
# One hour, 18 clients, no injected faults.
python3 scripts/soak.py --duration 3600

# Verify every recovery category, then run four hours of seeded faults.
python3 scripts/soak.py --duration 14400 --fault-sweep --seed 42 \
  --client-disconnect-percent 10 --client-crash-percent 2 \
  --service-disconnect-percent 5 --service-crash-percent 1

# Reproduce one recovery category, still checking all nine pairings.
python3 scripts/soak.py --duration 125 --fault-sweep --fault-case provider-crash
```

Each fault tick selects one client and one service uniformly. For each selected
participant, a seeded draw chooses disconnect, crash or no fault using that
role's two percentages. The percentages must sum to at most 100. Selected
faults run sequentially, with recovery verified before the next fault. Ticks
resume after `--fault-interval` seconds (default 10); slow recovery does not
accumulate a burst of overdue faults. All percentages default to zero.

`--fault-sweep` first injects both fault types into one client in each of the
nine pairings and into each of the three services: 24 faults, providing all
four recovery categories for all nine pairings. Every client attached to a
faulted service must recover, including both replicas. This sweep guarantees
coverage even when a probability is low or zero. `--duration` measures the
steady/random phase after the sweep; startup, final recovery and cleanup are
additional. A fault already in progress is always restored and checked.
Use `--fault-case client-disconnect`, `client-crash`, `provider-disconnect`, or
`provider-crash` with `--fault-sweep` to isolate one category. The default `all`
requires the complete sweep; a selected category still requires all nine pairings.

Disconnects use an isolated transparent TCP proxy with an independent route
for every participant. The controller verifies that a real connection was
closed and keeps that route disabled for `--downtime` seconds (default 3).
The application process and its managed client stay alive. Crashes use Docker
SIGKILL, verify exit code 137, wait the same downtime, and restart the container.
The controller records each injection, acknowledgement, restoration and recovery.
It controls only containers belonging to that run's unique Compose project.

Recovery requires a fresh generation-1 agreement, the correct provider session,
and successful replies **and** authorized protected-view reads for all three
payload cases from every affected client. Disconnects must preserve application
sessions; crashes must create new sessions. Surviving clients retain their
managed service object and demonstrate rejection of their previous agreement.
Old agreement IDs may never reappear. Published operations are never retried
by the application; each new attempt has a new operation ID.

Failures are tolerated only for clients affected by the active fault and its
bounded recovery window. A healthy client error, unplanned process exit,
unexpected agreement replacement, incorrect data or stalled unaffected client
fails the run. Recovery must finish within `--recovery-timeout` seconds
(default 180), and healthy progress must continue within `--stall-timeout`
(default 45). Individual operation waits use `--operation-timeout` (default 10
seconds). A final complete healthy matrix and clean client exit are required.
The recovery window allows the standard 120-second Concord token lifetime:
when a disconnected owner cannot withdraw its token, peers can need to observe
expiry before negotiating a successor. The harness keeps the canonical bucket
policies rather than shortening token lifetimes just for tests.

`--operation-interval` (default 1 second) controls workload pacing per client.
Each live agreement updates one `latest` view, so healthy traffic does not
create one retained record or view subscription per operation. Old contracts
and views remain in the isolated broker for inspection; fault-heavy runs can
therefore grow storage with agreement churn. No maintenance process hides that
growth. Container CPU/memory samples are saved every `--sample-interval`
seconds (default 30); exceeding `--max-memory-mib` (default 1024 per container)
fails the run. These bounds detect failures during the chosen duration; they
do not establish indefinite resource stability.

Evidence lives in `artifacts/soak-<run-id>/`: append-only participant events,
fault timeline, random choices, source pins, settings, resource samples,
container logs, broker diagnostics, summary and JUnit. Event streams are read
incrementally; full request/view observations remain on disk, so reserve disk
space for long runs. A seed reproduces target/outcome choices for the same
number of ticks and topology; timing and actual recovery latency can vary.

Push/PR CI runs independent smoke, 125-second steady and seeded-random jobs,
plus each of the four recovery categories followed by 125 seconds of healthy
traffic. Every job uses 18 clients, and a failed job does not cancel the others.
The **Extended soak** workflow can be dispatched manually with duration,
seed and all four probabilities; it uses 18 clients and always includes the
sweep. Local duration is configurable without a fixed maximum. CI runs also
have the timeout configured in the workflow.

## Evidence

Every run writes to `artifacts/<run-id>/`, including failures:

- `summary.json` and `junit.xml`: outcome and coverage counts.
- `participants/`: per-process readiness and full operation observations.
- `sources.json`: source revisions, dirty state and tracked-diff fingerprints.
- `compose.yaml`: resolved configuration.
- Build/startup logs, container states, participant logs, JetStream diagnostics
  and cleanup output.

The evidence directory remains after cleanup. These generated files are ignored
by Git. Each run starts with empty stores; broker persistence is scoped to that
run. There is no automated maintenance/reaper in the smoke stack.

## Development

```sh
python3 -m unittest discover -s tests
docker compose config --quiet
```

The runner tests deliberately corrupt successful-looking evidence to ensure
that partial matrices and incorrect replies cannot pass. Runtime validation is
the actual Compose matrix, not these runner checks alone.

The root Compose file can also be inspected or operated directly. Use the runner
for assertions and isolation; direct Compose startup leaves clients waiting at
the workload barrier. The runner only removes resources in its own unique
Compose project and never prunes unrelated Docker resources.

## Next increments

1. Add event, reverse-request and protected-view watch/update/delete workloads.
2. Add network blackholes, overlapping faults and broker restart scenarios.
3. Add command-side-effect reconciliation and exact fault-timeline replay.
4. Add isolated bucket creator-order/policy tests and optional maintenance runs.

Deckr's authoritative schemas and conformance scenarios stay in `kws/deckr`.
This repository owns the test application, worker adapters and orchestration.

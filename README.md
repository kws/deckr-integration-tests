# Deckr integration tests

Real-broker interoperability tests for the TypeScript, Python and Rust Deckr
implementations. Docker Compose runs one service per language and, by default,
two independent client containers for every client/service pairing: 3 services,
18 clients and one JetStream-enabled NATS server.

## Place in the project

The [project repository map](https://github.com/kws/deckr#project-repositories)
describes the reference, three language cores, and this harness.
[deckr](https://github.com/kws/deckr) owns the specifications, schemas, fixtures,
vectors, and language-neutral conformance scenarios. The
[Python](https://github.com/kws/deckr-core-python),
[Rust](https://github.com/kws/deckr-core-rust), and
[TypeScript](https://github.com/kws/deckr-core-typescript) cores own their APIs,
implementation tests, and upstream artifact checks.

This repository owns test applications and orchestration using those public APIs.
It checks interoperability for the workloads described below; it does not provide
a production controller, hardware driver, or Action Runtime application.
Implementation defects belong in the corresponding core repository. Contract
changes start in the reference repository.

| Check | Purpose | Execution |
| --- | --- | --- |
| Runner unit tests | Reject incomplete or incorrect evidence; check orchestration logic. | Host Python, no broker. |
| Smoke matrix | Service discovery, agreement negotiation, echo replies, and protected-view reads for all nine language pairings. | Docker Compose. |
| Soak and recovery | Continuous service traffic, renewal, and selected disconnect/crash recovery categories. | Docker Compose. |
| Native Concord lifecycle | Direct three-language Concord participation, numeric terms, renewal, broker restart, and owner closure. | Native language tools and a local NATS process. |
| Action Runtime matrix | All 16 messages, availability, authorization fences, and provider successors in nine language pairings. | Native language peers and local NATS. |
| Python controller / foreign providers | Controller lifecycle, input/output, pages, rendering, races, and authority loss with three provider languages. | Controller pytest suite and native provider peers over local NATS. |

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
The runner checks for clean checkouts at those commits and matching specification
pins before starting.
Language dependencies are installed inside the images using committed lockfiles;
the host does not need Node, Rust, uv, or the Python Deckr package.

For a fresh workspace with none of the four source siblings present, run
`python3 scripts/checkout_sources.py` from this repository. It creates detached
checkouts at the locked commits and refuses an existing destination. For existing
checkouts or local changes, use the [source-pin workflow](#source-pins-and-coordinated-changes).

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
The [native Concord test](#native-concord-lifecycle-test) below additionally
checks numeric terms hashing, automatic renewal and broker restart.

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
For SIGKILL restarts, the supervisor also reads session diagnostics directly from
the target container, verifies its exact ID/start time and exit code 137, then
atomically writes a handoff through the read-only `/control` mount. Each core
performs its own cleanup. Earlier exited lifetimes remain in the handoff across
repeated crashes. Verified restarts must recover within 30 seconds of restoration;
the canonical 120-second token TTL is unchanged. Unit tests additionally assert
that predecessor tokens still exist when cleanup finishes, and that live
competing sessions and fresh successors survive.

For disconnections without verified exit, the recovery window allows the standard
120-second Concord token lifetime:
when a disconnected owner cannot withdraw its token, peers can need to observe
expiry before negotiating a successor. The harness keeps the canonical bucket
policies rather than shortening token lifetimes just for tests.

`--operation-interval` (default 1 second) controls workload pacing per client.
Each live agreement updates one `latest` view, so healthy traffic does not
create one retained record or view subscription per operation. Old contracts
and views remain in the isolated broker for inspection; fault-heavy runs can
therefore grow storage with agreement churn. No maintenance process hides that
growth. Container CPU/memory samples are saved every `--sample-interval`
seconds (default 30), including memory in MiB. Resource usage has no fixed abort
threshold, so growth remains visible throughout the configured run.

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

## Native Concord lifecycle test

`scripts/concord.py` runs the existing interop drivers from all three core
repositories against its own local NATS process. It supplements the managed
service matrix with direct Concord checks:

- Delayed broker startup and successful connection by all three languages.
- Each language proposing an agreement attached by the other two, with identical
  hashes for floating-point, large-integer and negative-zero terms.
- Real automatic token renewals preserving authority.
- Broker loss revoking authority, source recovery after broker restart, and
  fresh proposals without resurrecting old authority.
- Managed owner-lifetime closure revoking participant authority and constant
  shared-consumer counts across all rounds.

This test uses the same sibling checkouts and `sources.lock.json`. Unlike the
Compose runners, it needs native tools: Node 22.18+, Python 3.11+ with uv,
Rust 1.88+ with Cargo, and a `nats-server` binary supporting Deckr's bucket
policies (validated with NATS 2.14.0). Install the drivers' dependencies and run
from this repository:

```sh
(cd ../deckr-core-python && uv sync --locked)
(cd ../deckr-core-typescript && npm ci)
python3 scripts/concord.py --nats-server /absolute/path/to/nats-server
```

The runner builds the Rust interop example with its locked dependencies.
`NATS_SERVER_BINARY` or `nats-server` on `PATH` can replace `--nats-server`.
Allow roughly two minutes for the full test, plus compilation time: renewal uses
the standard 120-second token lifetime. `--skip-build` reuses an already built
Rust example; rebuild after changing Rust sources. `--skip-renewal` is a partial quick check
and does not establish renewal coverage. `--allow-source-changes` records a
development run against changed sibling checkouts.

Evidence is retained in `artifacts/concord-<run-id>/`: source revisions,
`summary.json` with completed checks and renewal coverage, `junit.xml`, peer
stderr logs, broker logs/data, and a build log when building. `--logs` selects
a new evidence directory; an existing directory is rejected to prevent mixing
runs. The runner stops its own peers and broker on completion. This optional
native test is run separately from the Compose CI jobs; process crash/restart
coverage belongs to the service soak runner.

## Compose evidence

Every Compose run writes to `artifacts/<run-id>/`, including failures:

- `summary.json` and `junit.xml`: outcome and coverage counts.
- `participants/`: per-process readiness and full operation observations.
- `sources.json`: source revisions, dirty state and tracked-diff fingerprints.
- `compose.yaml`: resolved configuration.
- Build/startup logs, container states, participant logs, JetStream diagnostics
  and cleanup output.

The evidence directory remains after cleanup. These generated files are ignored
by Git. Each run starts with empty stores; broker persistence is scoped to that
run. There is no automated maintenance/reaper in the smoke stack.

The locked sources pass the full smoke matrix: three providers, 18 clients, all
nine language pairings, 108 successful replies, and 108 protected-view reads.
Validation evidence is in `artifacts/20261021T074208Z-80a76b67/`, including the
exact clean source revisions in `sources.json`. All worker images were rebuilt
for this run, and its stack was removed after validation.

Each core checks the upstream Action Runtime semantic cases for requirement
matching, explicit page targets, generations, service subjects, and rendering
admission. Rendering coverage includes its message models, support declarations,
result correlation, and managed command/result delivery. Controller content
rendering and output ordering are checked in the controller repository.

The Compose smoke matrix checks managed service interoperability at the source
revisions in `sources.lock.json`. It does not execute Action Runtime rendering
between languages, controller page mutations, real hardware, recovery soak, or
the native Concord scenario suite. Those workloads require separate checks.

## Native Action Runtime workloads

These are separate workloads from the generic Compose echo matrix. They use
public managed service boundaries with each core's Action Runtime declaration,
payload models and authorization. Test control uses local JSON-line stdio; Deckr
messages, discovery, agreements and availability travel over real NATS.

Use Node 22.18+, Rust/Cargo through `mise`, and a Python 3.11+ environment with
the sibling Python core installed editable and its `supervised-nats` extra.
For the controller workload, install the sibling `deckr-controller` and its
development dependencies into that same environment. Install TypeScript core
dependencies with `npm ci` in its checkout. From this repository, with that
Python environment active:

```sh
python scripts/action_runtime.py --allow-source-changes
python scripts/controller_action_runtime.py --allow-source-changes
```

In the parent Deckr workspace, `../.venv/bin/python` selects the shared editable
environment. Both commands build the Rust peer with locked dependencies and
start/stop their own localhost brokers. `--skip-build` reuses the existing Rust
peer; use it only after building the current Rust sources. Development runs use
`--allow-source-changes` to record working changes without moving source pins.
Omit it to require the locked reference/core sources. The controller checkout is
an additional recorded input; it is not part of `sources.lock.json`.

The nine-pair matrix checks all 16 declared messages in both protocol directions
(144 initial deliveries), exact service subjects, endpoint/session/contract
identities, render-result causation, fenced availability, malformed page payloads,
and rejected session/contract substitutions through the public authorizers.
Every pairing restarts its provider gracefully and verifies a fresh session and
successor contract, fresh availability, rejection of predecessor authority, no
event replay, and two fresh deliveries. Reference payloads are independent wire
examples; they do not model a controller's page or binding state machine.

The controller runner executes
[`tests/test_cross_language_action_runtime.py`](https://github.com/kws/deckr-controller/blob/main/tests/test_cross_language_action_runtime.py)
against Python, TypeScript and Rust provider processes: ten scenarios, 30 cases.
It verifies lifecycle and shared capability matches, single input delivery,
authorized hardware output, page open/replace/close generations, text/image/clear
rendering, error results and causation, reverse completion, failed newer work,
direct-frame ordering, overlay composition, late hardware rejection, hardware
claim loss, provider successors, and service loss during page planning. Gated
render backends make ordering checks deterministic. The hardware manager uses
the real managed API with a recording backend, not a physical device.

Both runners reject missing pairs, scenarios, languages, duplicate results and
skips. Evidence is retained under `artifacts/action-runtime-*` and
`artifacts/controller-action-runtime-*`: summary, JUnit, source revisions and
hashes (including untracked harness/controller inputs), provider transcripts,
stderr, and broker logs/data. The controller module skips in the ordinary
controller suite unless explicitly enabled by this runner; those skips never
count as a successful cross-language run.

These workloads do not prove abrupt process death, broker/network failure,
token renewal over long durations, physical device behavior, every optional
owner-defined render format, or application reference policies in every
specialized Action Runtime facade. The matrix tests managed service transport
and typed Action Runtime payloads; the controller suite supplies real controller
policy. The existing conformance checks and service soak cover different claims.

## Source pins and coordinated changes

`sources.lock.json` pins the reference and each core independently. Each core also
pins the reference for its own conformance checks: Python and TypeScript use
`.deckr-spec-revision`, and Rust uses `spec-revision`. All three files must name
the reference commit in the harness lock.

For local iteration, use `--allow-source-changes` with `smoke.py`, `soak.py`, or
`concord.py`. It permits different commits and dirty sibling checkouts and records
their revisions and fingerprints. It still requires matching specification pins.
Rebuild worker images after source changes; `--no-build` is for reuse of matching
images. These development runs do not validate the unchanged committed lock.

For a coordinated update:

1. Define and validate any contract change in `deckr`.
2. Update the affected cores and their specification pins. Run each core's
   implementation and conformance checks.
3. Record the reviewed reference and core commits in `sources.lock.json`, keeping
   unchanged revisions where applicable. Make new commits available before CI
   fetches them.
4. Run the relevant workloads with clean siblings at the locked revisions.
   Service recovery changes need the recovery sweep; native Concord changes need
   `concord.py`. Keep evidence and report any skipped coverage.

Package release versions are independent of these Git pins. Documentation-only
edits do not require moving the integration lock; local runners still require
`--allow-source-changes` while the source checkouts are dirty.

## Repository layout

```text
agents/                 Python, Rust, and TypeScript test applications
protocol/               Shared test-service declaration and payload cases
scripts/                Source checkout, smoke, soak, native Concord, and proxy runners
tests/                  Host-side runner tests
docker/                 Worker/proxy images and NATS configuration
compose.yaml            Service matrix topology
sources.lock.json       Reference and core commits used by the harness
.github/workflows/      Compose smoke/recovery matrix and extended soak
artifacts/              Generated run evidence (ignored)
```

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
2. Add network blackholes, overlapping faults and broker restart scenarios for
   the managed service workloads.
3. Add command-side-effect reconciliation and exact fault-timeline replay.
4. Add isolated bucket creator-order/policy tests and optional maintenance runs.

Deckr's authoritative schemas and conformance scenarios stay in `kws/deckr`.
This repository owns the test application, worker adapters and orchestration.

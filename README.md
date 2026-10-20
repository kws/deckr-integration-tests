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

This milestone does not yet prove event delivery, reverse request/reply,
view-watch recovery, crash recovery or long-running resource stability.
The preceding `mixed-language` Concord harness in the parent workspace remains
a separate test; this suite does not replace its renewal and recovery checks.

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
2. Add per-participant proxy routes and an out-of-band control interface for
   deterministic disconnects, blackholes, process kills and broker restarts.
3. Assert permanent revocation of old authority and fresh successor agreements,
   without recreating managed clients to mask recovery failures.
4. Add steady and seeded-fault soak profiles, bounded resource measurements,
   durable command-execution evidence and fault timeline replay.
5. Add isolated bucket creator-order/policy tests and optional maintenance runs.

Deckr's authoritative schemas and conformance scenarios stay in `kws/deckr`.
This repository owns the test application, worker adapters and orchestration.

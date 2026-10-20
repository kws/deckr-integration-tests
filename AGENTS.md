# Integration harness

This repository owns the cross-language Docker Compose harness, native Concord
runner, and test applications. Use [README.md](README.md) for setup, workload
coverage, evidence, and the source-pin workflow. The
[project repository map](https://github.com/kws/deckr#project-repositories)
defines the relationship to the reference and three language cores.

Use each implementation's public managed APIs. Do not implement discovery,
agreement negotiation, authority recovery, or wire serialization in the harness.

The sibling `deckr` repository owns Deckr contracts and conformance scenarios.
`protocol/` contains only this test application's declaration and workload data.
Implementation fixes belong in the corresponding core repository.

## Placement and source pins

- `agents/` contains the language-specific test applications; `scripts/` owns
  orchestration, source checkout, and fault injection.
- `tests/` checks runner behavior. `docker/` and `compose.yaml` own the isolated
  test stack. Keep generated evidence under ignored `artifacts/`.
- `sources.lock.json` pins the reference and all three cores. Their specification
  pins must agree, including when using `--allow-source-changes`.
- Use `scripts/checkout_sources.py` only in a fresh workspace. It refuses
  existing sibling destinations; do not reset working checkouts for a test run.
- Keep implementation fixes in the cores and update source pins after the
  relevant commits are reviewed. Documentation-only changes need not move pins.

## Validation and reporting

Run `python3 -m unittest discover -s tests` and `docker compose config --quiet`
for runner/configuration checks. Use `python3 scripts/smoke.py` for the real-broker
service matrix. Recovery changes also need
`python3 scripts/soak.py --fault-sweep --duration 125` against the real stack.
Changes to the native Concord runner or its core drivers need
`python3 scripts/concord.py` with the native dependencies described in the README.

Never report a skipped language or missing result as a passing complete matrix.
Runner tests, single-language tests, service interop, and native Concord checks
prove different things. Keep failure evidence and state the exact workload,
source revisions, and coverage limits.
Use repository URLs for cross-project documentation links.

# Integration harness

This repository owns the cross-language Docker Compose harness and test applications.
Use each implementation's public managed APIs. Do not implement discovery,
agreement negotiation, authority recovery, or wire serialization in the harness.

The sibling `deckr` repository owns Deckr contracts and conformance scenarios.
`protocol/` contains only this test application's declaration and workload data.
Implementation fixes belong in the corresponding core repository.

Run `python3 -m unittest discover -s tests` for runner checks and
`python3 scripts/smoke.py` for the real-broker matrix. Never report a skipped
language or missing result as a passing complete matrix. Keep failure evidence.

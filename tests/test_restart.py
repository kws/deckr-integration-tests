"""Confirmed exact process exit is the only source of early cleanup evidence."""

import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from restart import verified_exit_handoff


class RestartTests(unittest.TestCase):
    def test_exact_exit_and_repeated_crashes(self):
        live = dict(
            Id="container", State=dict(Running=True, StartedAt="start", ExitCode=0)
        )
        dead = dict(
            Id="container", State=dict(Running=False, StartedAt="start", ExitCode=137)
        )
        rows = [dict(endpoint="service:provider", sessionId="old")]
        proof = verified_exit_handoff("provider", live, dead, rows)
        self.assertEqual(
            verified_exit_handoff("provider", live, dead, rows, proof), proof
        )
        for invalid in [
            live,
            dict(dead, Id="different"),
            dict(dead, State=dict(dead["State"], StartedAt="restarted")),
            dict(dead, State=dict(dead["State"], ExitCode=0)),
        ]:
            with self.assertRaises(ValueError):
                verified_exit_handoff("provider", live, invalid, rows)
        next_live, next_dead = copy.deepcopy(live), copy.deepcopy(dead)
        next_live["State"]["StartedAt"] = next_dead["State"]["StartedAt"] = "second"
        retained = verified_exit_handoff(
            "provider",
            next_live,
            next_dead,
            [dict(endpoint="service:provider", sessionId="second")],
            proof,
        )
        self.assertEqual(len(retained["predecessors"]), 2)
        self.assertEqual(len(proof["predecessors"]), 1)
        with self.assertRaises(ValueError):
            verified_exit_handoff("other", live, dead, rows, proof)
        for invalid_rows in [[], rows * 2]:
            with self.assertRaises(ValueError):
                verified_exit_handoff("provider", live, dead, invalid_rows)

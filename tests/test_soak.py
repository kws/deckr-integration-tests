"""Negative evidence tests for fault attribution and recovery claims."""

import copy
import json
from pathlib import Path
import random
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import soak


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.members = soak.participants(1)
        self.cases = [{"boolean": True}, {"nested": [None, 7]}, {"text": "日本語"}]
        self.oracle = soak.Oracle(self.members, "run", self.cases)
        for member in self.members.values():
            self.oracle.accept(
                dict(
                    **member,
                    runId="run",
                    sessionId=member["id"] + "-session",
                    kind="boot",
                ),
                set(),
                False,
            )
        self.client = "client-python-rust-0"
        for seq in range(3):
            self.oracle.accept(self.row(self.client, seq), set(), True)
        self.before = copy.deepcopy(self.oracle.latest[self.client])

    def row(self, identity, seq, epoch=1, contract=None, session=None, provider=None):
        member = self.members[identity]
        session = session or identity + "-session"
        provider = provider or f"provider-{member['target']}-session"
        operation = f"{identity}-{session}-{seq}"
        value = dict(
            operationId=operation,
            payload=self.cases[seq % 3],
            providerLanguage=member["target"],
            providerSession=provider,
        )
        return dict(
            **member,
            runId="run",
            kind="success",
            sessionId=session,
            seq=seq,
            epoch=epoch,
            caseIndex=seq % 3,
            operationId=operation,
            providerSession=provider,
            contract=dict(contractId=contract or identity + "-agreement", generation=1),
            reply=value,
            view=copy.deepcopy(value),
        )

    def fault(self, role="client", kind="disconnect"):
        return dict(
            participant=self.client if role == "client" else "provider-rust",
            role=role,
            kind=kind,
            before={self.client: self.before},
        )

    def replacement(self, seq, **kwargs):
        row = self.row(self.client, seq, epoch=2, contract="successor", **kwargs)
        row.update(previousContract=self.before["contract"], oldAuthorityRejected=True)
        return row

    def test_disconnect_requires_new_authority_same_session_and_all_cases(self):
        fault = self.fault()
        self.oracle.accept(self.replacement(3), {self.client}, True)
        self.assertFalse(self.oracle.recovered(fault, 2))
        for seq in (4, 5):
            self.oracle.accept(self.replacement(seq), {self.client}, True)
        self.assertTrue(self.oracle.recovered(fault, 2))
        with self.assertRaises(AssertionError):
            self.oracle.recovered(self.fault(kind="crash"), 2)

    def test_pass_looking_recovery_without_revocation_fails(self):
        for bad in (None, False):
            with self.subTest(rejected=bad):
                row = self.replacement(3)
                row["oldAuthorityRejected"] = bad
                with self.assertRaises(AssertionError):
                    copy.deepcopy(self.oracle).accept(row, {self.client}, True)

    def test_old_agreement_cases_do_not_count_toward_successor_recovery(self):
        for seq in (3, 4, 5):
            self.oracle.accept(self.row(self.client, seq, epoch=2), {self.client}, True)
        self.oracle.accept(self.replacement(6), {self.client}, True)
        self.assertFalse(self.oracle.recovered(self.fault(), 2))
        for seq in (7, 8):
            self.oracle.accept(self.replacement(seq), {self.client}, True)
        self.assertTrue(self.oracle.recovered(self.fault(), 2))

    def test_boot_cannot_reuse_another_participant_session(self):
        row = dict(
            **self.members[self.client],
            runId="run",
            kind="boot",
            sessionId="provider-rust-session",
        )
        with self.assertRaisesRegex(AssertionError, "process session was reused"):
            self.oracle.accept(row, {self.client}, True)

    def test_retired_agreement_cannot_return(self):
        self.oracle.accept(self.replacement(3), {self.client}, True)
        with self.assertRaises(AssertionError):
            self.oracle.accept(self.row(self.client, 4), {self.client}, True)

    def test_unplanned_errors_restart_and_corrupt_results_fail(self):
        for mutation in (
            lambda row: row.update(runId="another-run"),
            lambda row: row.update(providerSession="wrong-provider"),
            lambda row: row["view"]["payload"].update(boolean=1),
            lambda row: row["contract"].update(generation=2),
            lambda row: row.update(kind="unavailable", error="unexpected"),
            lambda row: row.update(kind="boot", sessionId="unexpected-restart"),
        ):
            with self.subTest(mutation=mutation):
                row = copy.deepcopy(self.row(self.client, 3))
                mutation(row)
                with self.assertRaises(AssertionError):
                    copy.deepcopy(self.oracle).accept(row, set(), True)

    def test_crash_requires_a_fresh_process_and_agreement(self):
        member = self.members[self.client]
        self.oracle.accept(
            dict(**member, runId="run", kind="boot", sessionId="new-session"),
            {self.client},
            True,
        )
        for seq in range(3):
            self.oracle.accept(
                self.replacement(seq, session="new-session"), {self.client}, True
            )
        self.assertTrue(self.oracle.recovered(self.fault(kind="crash"), 2))
        with self.assertRaises(AssertionError):
            self.oracle.recovered(self.fault(), 2)

    def test_sweep_covers_each_pair_for_each_fault_category(self):
        coverage = set()
        for identity, kind in soak.sweep(self.members):
            member = self.members[identity]
            affected = (
                [member]
                if member["role"] == "client"
                else [
                    client
                    for client in self.members.values()
                    if client["role"] == "client"
                    and client["target"] == member["language"]
                ]
            )
            for client in affected:
                coverage.add(
                    (client["language"], client["target"], member["role"], kind)
                )
        self.assertEqual(len(coverage), 9 * 4)

    def test_independent_sweeps_partition_the_full_sweep(self):
        full = set(soak.sweep(self.members))
        combined = []
        for category in soak.FAULT_CASES:
            selected = soak.sweep(self.members, category)
            self.assertEqual(len(selected), 9 if category.startswith("client") else 3)
            for identity, kind in selected:
                self.assertEqual(f"{self.members[identity]['role']}-{kind}", category)
            combined.extend(selected)
        self.assertEqual(len(combined), len(full))
        self.assertEqual(set(combined), full)


class ControlsTests(unittest.TestCase):
    def test_probability_endpoints_and_seed_reproducibility(self):
        members = soak.participants(2)
        self.assertEqual(
            soak.choose_faults(
                random.Random(7), members, dict(client=(0, 0), provider=(0, 0))
            ),
            [],
        )
        forced = soak.choose_faults(
            random.Random(7), members, dict(client=(100, 0), provider=(0, 100))
        )
        self.assertEqual([kind for _, kind in forced], ["disconnect", "crash"])
        first, second = random.Random(1234), random.Random(1234)
        probabilities = dict(client=(13, 27), provider=(22, 15))
        self.assertEqual(
            [soak.choose_faults(first, members, probabilities) for _ in range(100)],
            [soak.choose_faults(second, members, probabilities) for _ in range(100)],
        )

    def test_invalid_configuration_rejected(self):
        for args in (
            ["--duration", "nan"],
            ["--duration", "-1"],
            ["--client-crash-percent", "101"],
            ["--service-crash-percent", "50", "--service-disconnect-percent", "51"],
            ["--operation-interval", "0"],
            ["--fault-case", "client-crash"],
        ):
            with self.subTest(args=args), self.assertRaises(AssertionError):
                soak.arguments(args)

    def test_tail_preserves_partial_records_and_rejects_truncation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text('{"a":')
            tail = soak.Tail(path)
            self.assertEqual(tail.read(), [])
            with path.open("a") as stream:
                stream.write("1}\n" + json.dumps({"a": 2}) + "\n")
            self.assertEqual(tail.read(), [{"a": 1}, {"a": 2}])
            self.assertEqual(tail.read(), [])
            path.write_text("")
            with self.assertRaises(AssertionError):
                tail.read()

    def test_docker_memory_units(self):
        self.assertEqual(soak.memory_mib("1.5GiB"), 1536)
        self.assertEqual(soak.memory_mib("20MiB"), 20)

    def test_large_memory_usage_is_recorded_without_aborting(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = soak.Runner.__new__(soak.Runner)
            runner.compose, runner.env, runner.output = [], {}, Path(directory)
            row = {"Name": "growing-worker", "MemUsage": "25GiB / 32GiB"}
            with patch.object(soak, "command", side_effect=["container", json.dumps(row)]):
                runner.sample_resources()
            sample = json.loads((runner.output / "resources.jsonl").read_text())
            self.assertEqual(sample["containers"][0]["memoryMiB"], 25 * 1024)


if __name__ == "__main__":
    unittest.main()

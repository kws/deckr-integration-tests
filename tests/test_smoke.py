"""Adversarial checks for the runner's pass/fail oracle; no broker required."""

import copy
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "smoke", Path(__file__).resolve().parents[1] / "scripts/smoke.py"
)
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


def evidence():
    ready = [
        dict(runId="run", role="provider", language=lang, sessionId=f"provider-{lang}")
        for lang in smoke.LANGUAGES
    ]
    records = []
    for language in smoke.LANGUAGES:
        for target in smoke.LANGUAGES:
            identity = f"{language}-{target}"
            contract = dict(contractId=identity, generation=1)
            common = dict(
                runId="run",
                role="client",
                language=language,
                target=target,
                id=identity,
                sessionId=identity,
                providerSession=f"provider-{target}",
                contract=contract,
            )
            ready.append(common)
            result = dict(
                operationId=f"{identity}-0-0",
                payload={"answer": 42},
                providerLanguage=target,
                providerSession=f"provider-{target}",
            )
            records.append(
                dict(
                    **common,
                    status="pass",
                    observations=[
                        dict(
                            round=0,
                            caseIndex=0,
                            operationId=result["operationId"],
                            reply=result,
                            view=copy.deepcopy(result),
                        )
                    ],
                )
            )
    return records, ready


class SmokeOracleTests(unittest.TestCase):
    def verify(self, records, ready):
        return smoke.verify_results(records, ready, "run", 1, 1, [{"answer": 42}])

    def test_complete_matrix_passes(self):
        self.assertEqual(self.verify(*evidence())["operations"], 9)

    def test_boolean_numeric_coercion_fails(self):
        for field in ("reply", "view"):
            for original, corrupted in ((True, 1), (False, 0), (1, True), (0, False)):
                with self.subTest(field=field, original=original):
                    records, ready = evidence()
                    payload = {"nested": [original]}
                    for record in records:
                        for value in ("reply", "view"):
                            record["observations"][0][value]["payload"] = copy.deepcopy(
                                payload
                            )
                    records[0]["observations"][0][field]["payload"]["nested"][0] = (
                        corrupted
                    )
                    with self.assertRaises(AssertionError):
                        smoke.verify_results(records, ready, "run", 1, 1, [payload])

    def test_missing_duplicate_and_foreign_results_fail(self):
        for mutation in (
            lambda rows: rows.pop(),
            lambda rows: rows.__setitem__(1, copy.deepcopy(rows[0])),
            lambda rows: rows[0].update(runId="old-run"),
            lambda rows: rows[0].update(status="fail"),
            lambda rows: rows[0].update(sessionId="restarted"),
        ):
            with self.subTest(mutation=mutation):
                records, ready = evidence()
                mutation(records)
                with self.assertRaises(AssertionError):
                    self.verify(records, ready)

    def test_wrong_payload_provider_and_view_fail(self):
        for field in ("reply", "view"):
            for mutation in (
                lambda value: value.update(payload={"answer": 43}),
                lambda value: value.update(providerLanguage="wrong"),
                lambda value: value.update(providerSession="predecessor"),
            ):
                with self.subTest(field=field, mutation=mutation):
                    records, ready = evidence()
                    mutation(records[0]["observations"][0][field])
                    with self.assertRaises(AssertionError):
                        self.verify(records, ready)

    def test_missing_observation_and_invalid_agreement_fail(self):
        for mutate in (
            lambda row: row["observations"].clear(),
            lambda row: row["contract"].update(generation=2),
            lambda row: row["contract"].update(contractId=""),
        ):
            records, ready = evidence()
            mutate(records[0])
            with self.assertRaises(AssertionError):
                self.verify(records, ready)

    def test_container_exit_failure_cannot_be_hidden_by_pass_artifact(self):
        statuses = [
            dict(Service=f"provider-{lang}", Name=lang, State="running", ExitCode=0)
            for lang in smoke.LANGUAGES
        ]
        statuses += [
            dict(Service=f"client-{n}", Name=str(n), State="exited", ExitCode=0)
            for n in range(9)
        ]
        self.assertTrue(smoke.check_processes(statuses, 9, finished=True))
        statuses[-1]["ExitCode"] = 1
        with self.assertRaises(AssertionError):
            smoke.check_processes(statuses, 9, finished=True)

    def test_provider_crash_and_unfinished_cleanup_fail(self):
        statuses = [
            dict(Service=f"provider-{lang}", Name=lang, State="running", ExitCode=0)
            for lang in smoke.LANGUAGES
        ]
        statuses += [
            dict(Service=f"client-{n}", Name=str(n), State="running", ExitCode=0)
            for n in range(9)
        ]
        with self.assertRaises(AssertionError):
            smoke.check_processes(statuses, 9, finished=True)
        statuses[0]["State"] = "exited"
        with self.assertRaises(AssertionError):
            smoke.check_processes(statuses, 9)


if __name__ == "__main__":
    unittest.main()

"""A complete matrix must contain every pair/scenario and no skipped cases."""

import copy
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from action_runtime import check_matrix_results
from action_runtime_support import LANGUAGES
from controller_action_runtime import SCENARIOS, check_controller_results


class CompletionTests(unittest.TestCase):
    def matrix(self):
        return [
            {
                "language": c,
                "target": p,
                "contract": "old",
                "successor": "new",
                "messages": [{"name": str(i)} for i in range(16)],
            }
            for c in LANGUAGES
            for p in LANGUAGES
        ]

    def test_matrix_requires_every_pair_and_message(self):
        rows = self.matrix()
        check_matrix_results(rows)
        for fault in ("missing", "duplicate", "message", "contract"):
            bad = copy.deepcopy(rows)
            if fault == "missing":
                bad.pop()
            elif fault == "duplicate":
                bad[-1] = bad[0]
            elif fault == "message":
                bad[0]["messages"][-1] = bad[0]["messages"][0]
            else:
                bad[0]["successor"] = "old"
            with self.subTest(fault=fault), self.assertRaises(AssertionError):
                check_matrix_results(bad)

    def controller_xml(self):
        root = ET.Element("testsuites")
        suite = ET.SubElement(root, "testsuite")
        for name in SCENARIOS:
            for language in LANGUAGES:
                ET.SubElement(suite, "testcase", name=f"{name}[{language}]")
        return root, suite

    def check_xml(self, root):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "junit.xml"
            ET.ElementTree(root).write(path)
            return check_controller_results(path)

    def test_controller_requires_all_languages_and_scenarios(self):
        root, _ = self.controller_xml()
        self.assertEqual(self.check_xml(root)["tests"], 30)
        for fault in ("missing", "duplicate", "skipped", "failure", "error"):
            root, suite = self.controller_xml()
            if fault == "missing":
                suite.remove(suite[-1])
            elif fault == "duplicate":
                suite[-1].set("name", suite[0].get("name"))
            else:
                ET.SubElement(suite[-1], fault)
            with self.subTest(fault=fault), self.assertRaises(AssertionError):
                self.check_xml(root)

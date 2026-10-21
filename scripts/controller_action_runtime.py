#!/usr/bin/env python3
"""Run the real Python controller against all three native provider languages."""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

from action_runtime_support import (
    LANGUAGES,
    ROOT,
    WORKSPACE,
    build_rust,
    source_snapshot,
)
from smoke import source_manifest

SCENARIOS = (
    "test_lifecycle_matching_input_and_output",
    "test_page_targets_generations_and_notifications",
    "test_render_formats_rejections_and_correlation",
    "test_render_reverse_completion_and_clear",
    "test_hardware_rejection_after_newer_render",
    "test_failed_render_and_direct_frame_supersede_older_work",
    "test_overlay_composition_during_render_and_clear",
    "test_hardware_loss_fences_queued_render",
    "test_provider_successor_fences_queued_render_and_old_binding",
    "test_service_loss_during_page_planning",
)


def check_controller_results(path):
    cases = ET.parse(path).findall(".//testcase")
    assert not any(
        c.find("skipped") is not None
        or c.find("failure") is not None
        or c.find("error") is not None
        for c in cases
    )
    expected = {f"{name}[{language}]" for name in SCENARIOS for language in LANGUAGES}
    assert len(cases) == len(expected)
    assert {c.attrib["name"] for c in cases} == expected
    return {
        "status": "pass",
        "languages": list(LANGUAGES),
        "scenarios": len(SCENARIOS),
        "tests": len(cases),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-source-changes", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    directory = (
        ROOT
        / "artifacts"
        / (
            "controller-action-runtime-"
            + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        )
    )
    directory.mkdir(parents=True)
    summary = {"status": "fail"}
    try:
        sources = source_manifest(args.allow_source_changes)
        sources["controller"] = source_snapshot(WORKSPACE / "deckr-controller")
        sources["harness"] = source_snapshot(ROOT)
        (directory / "sources.json").write_text(json.dumps(sources, indent=2))
        if not args.skip_build:
            asyncio.run(build_rust(directory))
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/test_cross_language_action_runtime.py",
                "-v",
                "--tb=short",
                "--no-cov",
                f"--junitxml={directory / 'junit.xml'}",
            ],
            cwd=WORKSPACE / "deckr-controller",
            env={
                **os.environ,
                "DECKR_INTEROP_ROOT": str(ROOT),
                "DECKR_INTEROP_EVIDENCE": str(directory / "tests"),
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=600,
        )
        (directory / "pytest.log").write_text(result.stdout)
        print(result.stdout)
        assert result.returncode == 0, f"Controller workload exited {result.returncode}"
        summary = check_controller_results(directory / "junit.xml")
    except BaseException as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        (directory / "summary.json").write_text(json.dumps(summary, indent=2))
        print(f"Evidence: {directory}")


if __name__ == "__main__":
    main()

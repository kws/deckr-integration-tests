"""Continuous workload; the same managed service owns every recovery."""

import json
import asyncio
import os
from pathlib import Path
import time
import signal
import traceback

import anyio
from deckr.contracts.models import thaw_json
from deckr.services import ServiceUnavailable, ServiceViewRef

SOAKING = os.environ.get("MODE") == "soak"


def install_diagnostics():
    def dump():
        for task in asyncio.all_tasks():
            stack = []
            current = task.get_coro()
            while current is not None:
                frame = getattr(current, "cr_frame", None) or getattr(
                    current, "gi_frame", None
                )
                if frame:
                    stack.append(
                        f"{frame.f_code.co_filename}:{frame.f_lineno}:{frame.f_code.co_name}"
                    )
                current = getattr(current, "cr_await", None) or getattr(
                    current, "gi_yieldfrom", None
                )
            print("TASK " + task.get_name() + " " + " -> ".join(stack), flush=True)

    asyncio.get_running_loop().add_signal_handler(signal.SIGUSR1, dump)


async def epoch_value():
    deadline = time.monotonic() + 1
    while True:
        try:
            return int(Path("/control/epoch").read_text())
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                raise
            await anyio.sleep(0.01)


def event(common, kind, **values):
    with Path(f"/artifacts/{common['id']}.events.jsonl").open("a") as stream:
        stream.write(
            json.dumps(dict(**common, kind=kind, atMs=time.time() * 1000, **values))
            + "\n"
        )


async def consume(service, common, cases, store):
    timeout = float(os.environ.get("OPERATION_TIMEOUT", "10"))
    previous = None
    sequence = 0
    while not Path("/control/stop").exists():
        seq = sequence
        sequence += 1
        epoch = await epoch_value()
        case_index = seq % len(cases)
        operation_id = f"{common['id']}-{common['sessionId']}-{seq}"
        started = time.monotonic()
        phase = "ready"
        observed_operation = None
        try:
            await service.wait_for_state(
                lambda state: state.local_authority, timeout_seconds=timeout
            )
            descriptor = service.descriptor
            version = service.state.version
            access = service.view_access(store)
            provider_session = descriptor.session_id
            expected = dict(
                operationId=operation_id,
                payload=cases[case_index],
                providerLanguage=common["target"],
                providerSession=provider_session,
            )
            phase = "request"
            response = await service.request(
                "echo",
                {"operationId": operation_id, "payload": cases[case_index]},
                timeout_seconds=timeout,
            )
            if response.status != "ok":
                raise RuntimeError(f"request unavailable: {response}")
            if service.state.version != version:
                raise RuntimeError("agreement changed during operation")
            reply = thaw_json(response.result)
            assert reply == expected, (reply, expected)
            view = ServiceViewRef(
                store_name=store, key=descriptor.views["results"].key_prefix + "latest"
            )
            phase = "view"
            with anyio.fail_after(timeout):
                while True:
                    phase = "view-read"
                    entry = await access.read(view)
                    phase = "view-match"
                    observed_operation = (
                        entry.value.get("operationId") if entry is not None else None
                    )
                    if entry is not None and entry.value["operationId"] == operation_id:
                        break
                    await anyio.sleep(0.02)
            contract = dict(
                contractId=entry.contract.contract_id,
                generation=entry.contract.generation,
            )
            assert contract["generation"] == 1
            assert entry.consumer_session_id == common["sessionId"]
            assert entry.service_session_id == provider_session
            actual = {key: thaw_json(entry.value[key]) for key in expected}
            assert actual == expected, (actual, expected)
            old_rejected = None
            if previous and previous[0] != contract:
                try:
                    await previous[1].read(previous[2])
                except ServiceUnavailable:
                    old_rejected = True
                else:
                    raise AssertionError("retired agreement regained view authority")
            event(
                common,
                "success",
                seq=seq,
                epoch=epoch,
                caseIndex=case_index,
                operationId=operation_id,
                contract=contract,
                previousContract=previous[0] if previous else None,
                oldAuthorityRejected=old_rejected,
                providerSession=provider_session,
                reply=reply,
                view=actual,
                latencyMs=(time.monotonic() - started) * 1000,
            )
            previous = (contract, access, view)
        except AssertionError:
            raise
        except Exception as exc:
            event(
                common,
                "unavailable",
                seq=seq,
                epoch=epoch,
                operationId=operation_id,
                error=f"{type(exc).__name__}: {exc}",
                phase=phase,
                observedOperation=observed_operation,
                stack=traceback.format_exc(),
            )
        await anyio.sleep(float(os.environ.get("OPERATION_INTERVAL", "1")))

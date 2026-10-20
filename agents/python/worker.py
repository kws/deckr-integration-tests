"""Test application using the Python core's public managed service APIs."""

import json
import os
from pathlib import Path
import signal
import traceback
import uuid

import anyio

from deckr.bindings.nats import DeckrNatsSubstrate
from deckr.contracts.lanes import DEFAULT_MESSAGE_CONTRACT_REGISTRY
from deckr.contracts.models import thaw_json
from deckr.core.kv import KvBucketPolicy
from deckr.runtime import Deckr
from deckr.services import ServiceProtocol, ServiceViewRef, ServiceViewStore

LANGUAGE = "python"
ROLE = os.environ["ROLE"]
TARGET = os.environ.get("TARGET_LANGUAGE", LANGUAGE)
IDENTITY = f"{ROLE}-{LANGUAGE}-{TARGET}-{os.environ.get('HOSTNAME', uuid.uuid4().hex)}"
SESSION = uuid.uuid4().hex
DIRECTORY = Path(os.environ.get("PROTOCOL_DIR", "/protocol"))
DECLARATION = json.loads((DIRECTORY / "service.json").read_text())
CASES = json.loads((DIRECTORY / "cases.json").read_text())
PROTOCOL = ServiceProtocol(
    namespace=DECLARATION["namespace"],
    feature_id=DECLARATION["featureId"],
    advertisement_profile=DECLARATION["advertisementProfile"],
    use_profile=DECLARATION["useProfile"],
    operations=DECLARATION["operations"],
    messages=DECLARATION["messages"],
    view_families=DECLARATION["viewFamilies"],
)
STORE = DECLARATION["viewFamilies"]["results"]["storeName"]


def report(kind, **values):
    data = dict(
        runId=os.environ["RUN_ID"],
        id=IDENTITY,
        language=LANGUAGE,
        role=ROLE,
        target=TARGET,
        sessionId=SESSION,
        **values,
    )
    path = Path(f"/artifacts/{IDENTITY}.{kind}.json")
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data))
    temporary.replace(path)
    print(json.dumps(dict(event=kind, **data)), flush=True)


async def provide(runtime, endpoint):
    async with anyio.create_task_group() as tasks:
        store = ServiceViewStore(
            bucket=runtime.kv_bucket(KvBucketPolicy(bucket=STORE, ttl_seconds=None))
        )
        store.start(tasks)
        try:
            async with runtime.service_provider(
                endpoint, PROTOCOL, service_id="interop-python"
            ) as provider:
                report("ready", serviceId="interop-python")

                async def requests():
                    try:
                        async for request in provider.requests():
                            if request.failure:
                                raise request.failure
                            authorized = request.authorization
                            params = thaw_json(authorized.body.params)
                            result = dict(
                                **params,
                                providerLanguage=LANGUAGE,
                                providerSession=SESSION,
                            )
                            contract = provider.contract(authorized.contract)
                            if contract is None:
                                raise RuntimeError("provider lost agreement authority")
                            key = (
                                provider.descriptor.views["results"].key_prefix
                                + params["operationId"]
                            )
                            await contract.view_access(store).create(
                                ServiceViewRef(store_name=STORE, key=key), result
                            )
                            await provider.authorize(request.message)
                            body = authorized.body.to_dict()
                            body.pop("params", None)
                            body.update(status="ok", result=result)
                            await endpoint.reply_to(
                                request.message,
                                message_type="serviceMessage",
                                body=body,
                            )
                    except Exception as exc:
                        report(
                            "result",
                            status="fail",
                            error=str(exc),
                            stack=traceback.format_exc(),
                        )
                        raise

                tasks.start_soon(requests)
                with anyio.open_signal_receiver(
                    signal.SIGTERM, signal.SIGINT
                ) as signals:
                    await anext(signals)
                tasks.cancel_scope.cancel()
        finally:
            with anyio.CancelScope(shield=True):
                await store.aclose()


async def consume(runtime, endpoint):
    async with runtime.services(endpoint) as services:
        async with services.use_matching(
            PROTOCOL,
            predicate=lambda item: item.service_id == f"interop-{TARGET}",
            timeout_seconds=60,
        ) as service:
            descriptor = service.descriptor
            contract = None
            provider_session = descriptor.session_id
            report("ready", providerSession=provider_session)
            with anyio.fail_after(90):
                while not Path("/control/start").exists():
                    await anyio.sleep(0.05)
            observations = []
            for round_index in range(int(os.environ.get("ROUNDS", "2"))):
                for case_index, payload in enumerate(CASES):
                    operation_id = f"{IDENTITY}-{round_index}-{case_index}"
                    expected = dict(
                        operationId=operation_id,
                        payload=payload,
                        providerLanguage=TARGET,
                        providerSession=provider_session,
                    )
                    response = await service.request(
                        "echo",
                        {"operationId": operation_id, "payload": payload},
                        timeout_seconds=15,
                    )
                    assert response.status == "ok", response
                    reply = thaw_json(response.result)
                    assert reply == expected, (reply, expected)
                    view = ServiceViewRef(
                        store_name=STORE,
                        key=descriptor.views["results"].key_prefix + operation_id,
                    )
                    with anyio.fail_after(15):
                        while (
                            entry := await service.view_access(STORE).read(view)
                        ) is None:
                            await anyio.sleep(0.02)
                    observed_contract = {
                        "contractId": entry.contract.contract_id,
                        "generation": entry.contract.generation,
                    }
                    assert observed_contract["generation"] == 1
                    if contract is not None:
                        assert contract == observed_contract
                    contract = observed_contract
                    assert entry.consumer_session_id == SESSION
                    assert entry.service_session_id == provider_session
                    actual = {key: thaw_json(entry.value[key]) for key in expected}
                    assert actual == expected, (actual, expected)
                    observations.append(
                        dict(
                            round=round_index,
                            caseIndex=case_index,
                            operationId=operation_id,
                            reply=reply,
                            view=actual,
                        )
                    )
            report(
                "result",
                status="pass",
                contract=contract,
                providerSession=provider_session,
                observations=observations,
            )


async def main():
    bus = DeckrNatsSubstrate(
        url=os.environ["NATS_URL"],
        lane_contracts=DEFAULT_MESSAGE_CONTRACT_REGISTRY,
        reconnect_max_interval_seconds=1,
    )
    async with Deckr(message_bus=bus) as runtime:
        address = (
            "service:interop-python" if ROLE == "provider" else f"controller:{IDENTITY}"
        )
        async with runtime.endpoint(address, session_id=SESSION) as endpoint:
            if ROLE == "provider":
                await provide(runtime, endpoint)
            elif ROLE == "client":
                await consume(runtime, endpoint)
            else:
                raise ValueError(f"Unknown role: {ROLE}")


if __name__ == "__main__":
    try:
        anyio.run(main)
    except Exception as exc:
        report("result", status="fail", error=str(exc), stack=traceback.format_exc())
        raise

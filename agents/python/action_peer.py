"""Action Runtime test peer. Stdio controls the app; Deckr owns the wire/lifecycle."""

import json
import sys
from contextlib import AsyncExitStack

import anyio
from deckr.action_runtime import (
    ACTION_RUNTIME_SERVICE_PROTOCOL as PROTOCOL,
)
from deckr.action_runtime import (
    ACTION_RUNTIME_SERVICE_VIEW_STORE_NAME as STORE,
)
from deckr.action_runtime import (
    ActionRuntimeAvailabilityViewPayload,
    action_availability_view_ref,
    action_runtime_payload,
    validate_action_runtime_message,
)
from deckr.bindings.nats import DeckrNatsSubstrate
from deckr.contracts.lanes import CORE_LANE_CONTRACTS, MessageContractRegistry
from deckr.contracts.messages import DeckrMessage
from deckr.contracts.models import thaw_json
from deckr.core.kv import KvBucketPolicy
from deckr.runtime import Deckr
from deckr.services import ServiceViewStore


async def main():
    config = json.loads(sys.argv[1])
    provider_role = config["role"] == "provider"
    instance = config["instance"]
    service_id = f"action-runtime.{instance}"
    received, errors, channels, published = [], [], {}, set()
    entries = config["entries"]
    contracts = MessageContractRegistry(CORE_LANE_CONTRACTS.values())
    async with AsyncExitStack() as stack:
        runtime = await stack.enter_async_context(
            Deckr(
                lane_contracts=contracts,
                message_bus=DeckrNatsSubstrate(
                    url=config["url"], lane_contracts=contracts
                ),
            )
        )
        endpoint = await stack.enter_async_context(
            runtime.endpoint(
                f"service:{service_id}"
                if provider_role
                else f"controller:{config['id']}"
            )
        )
        tg = await stack.enter_async_context(anyio.create_task_group())
        store = ServiceViewStore(
            bucket=runtime.kv_bucket(KvBucketPolicy(bucket=STORE, ttl_seconds=None))
        )
        store.start(tg)
        stack.push_async_callback(store.aclose)
        provider = client = None

        def availability():
            return ActionRuntimeAvailabilityViewPayload(
                providerInstanceId=instance,
                providerId=config["providerId"],
                serviceId=service_id,
                serviceEndpoint=endpoint.address,
                serviceSessionId=endpoint.session_id,
                entries=entries,
            ).to_dict()

        if provider_role:
            provider = await stack.enter_async_context(
                runtime.service_provider(
                    endpoint,
                    PROTOCOL,
                    service_id=service_id,
                    diagnostics={"providerId": config["providerId"]},
                )
            )

            async def publish():
                async with provider.watch_state() as states:
                    async for state in states:
                        for item in state.contracts:
                            key = item.contract.contract_id
                            if (
                                item.local_authority
                                and item.validity.valid
                                and key not in published
                            ):
                                channel = provider.contract(item.contract)
                                channels[key] = channel
                                await channel.view_access(store).put(
                                    action_availability_view_ref(service_id),
                                    availability(),
                                )
                                published.add(key)

            async def receive():
                async for request in provider.requests():
                    if request.authorization is None:
                        continue
                    validate_action_runtime_message(request.message)
                    received.append(request.message.to_dict())

            tg.start_soon(publish)
            tg.start_soon(receive)
        else:
            services = await stack.enter_async_context(runtime.services(endpoint))
            # Subscribe before readiness is reported, so one-way events cannot race it.
            inbox = await stack.enter_async_context(endpoint.subscribe("services"))
            client = await stack.enter_async_context(
                services.use_matching(
                    PROTOCOL,
                    predicate=lambda d: d.service_id == service_id,
                    timeout_seconds=30,
                )
            )

            async def receive():
                async for message in inbox:
                    try:
                        await client.authorize_inbound_message(message)
                        validate_action_runtime_message(message)
                    except (ValueError, RuntimeError):
                        continue
                    received.append(message.to_dict())

            tg.start_soon(receive)

        def pointer():
            if provider_role:
                return next(
                    (
                        item.contract
                        for item in provider.state.contracts
                        if item.local_authority and item.validity.valid
                    ),
                    None,
                )
            return client.state.contract

        print(
            json.dumps(
                {
                    "ready": True,
                    "sessionId": endpoint.session_id,
                    "endpoint": str(endpoint.address),
                }
            ),
            flush=True,
        )
        while line := await anyio.to_thread.run_sync(sys.stdin.readline):
            command = json.loads(line)
            try:
                op = command["op"]
                held = pointer()
                contract = (
                    {"contractId": held.contract_id, "generation": held.generation}
                    if held
                    else None
                )
                if op == "stop":
                    print(json.dumps({"ok": True}), flush=True)
                    break
                if op == "status":
                    result = {
                        "contract": contract,
                        "received": received,
                        "errors": errors,
                    }
                elif op == "availability":
                    result = thaw_json(
                        await client.read_view(action_availability_view_ref(service_id))
                        or {}
                    )
                elif op == "send":
                    name, payload = command["name"], command["payload"]
                    params, event = action_runtime_payload(name, payload)
                    normalized = event if event is not None else params
                    is_event = PROTOCOL.messages[name].intent.value == "event"
                    kwargs = (
                        {"event": normalized} if is_event else {"params": normalized}
                    )
                    if provider_role:
                        channel = channels[
                            command.get("contractId") or held.contract_id
                        ]
                        message = await channel.send(name, **kwargs)
                    else:
                        message = await client.send(
                            name,
                            **kwargs,
                            causation_id=payload.get("requestMessageId")
                            if name == "binding_render_result"
                            else None,
                        )
                    result = {
                        "messageId": message.message_id,
                        "message": message.to_dict(),
                    }
                elif op == "authorize":
                    message = DeckrMessage.from_wire(command["message"])
                    if provider_role:
                        await provider.authorize(message)
                    else:
                        await client.authorize_inbound_message(message)
                    result = {"authorized": True}
                else:
                    raise ValueError(f"unknown operation {op}")
            except Exception as exc:
                result = {"error": f"{type(exc).__name__}: {exc}"}
            print(json.dumps(result), flush=True)
        # Close managed owners before cancelling their host task group.
        if provider:
            await provider.aclose()
        if client:
            await client.aclose()
        await store.aclose()
        tg.cancel_scope.cancel()


if __name__ == "__main__":
    anyio.run(main)

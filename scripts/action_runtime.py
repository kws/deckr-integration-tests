#!/usr/bin/env python3
"""Nine-pair Action Runtime message/availability/recovery checks over real NATS."""

import argparse
import asyncio
import copy
import json
from datetime import UTC, datetime

from action_runtime_support import (
    LANGUAGES,
    ROOT,
    WORKSPACE,
    Peer,
    build_rust,
    source_snapshot,
)
from smoke import source_manifest, write_junit


def cases(instance):
    from deckr.action_runtime import action_runtime_payload

    root = WORKSPACE / "deckr/contract/v1"
    protocol = json.loads(
        (root / "catalogs/action-runtime-protocol.v1.json").read_text()
    )
    items = {}

    def identities(value):
        if isinstance(value, dict):
            return {
                key: instance if key == "providerInstanceId" else identities(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [identities(item) for item in value]
        return value

    for name, definition in protocol["messages"].items():
        slug = name.replace("_", "-")
        path = (
            root
            / f"fixtures/valid/coverage/deckr/action-runtime/messages/{slug}.v1.json"
        )
        if not path.exists():
            path = (
                root
                / f"fixtures/valid/action-runtime/rendering/messages-{slug}.v1.json"
            )
        wire = json.loads(path.read_text())
        body = wire["body"]
        params, event = action_runtime_payload(
            name, identities(body.get("event") or body["params"])
        )
        items[name] = {
            "direction": definition["direction"],
            "payload": event if event is not None else params,
        }
    return items


def assert_delivery(message, name, payload, sender, receiver, contract):
    assert message["contract"] == contract
    assert message["sender"] == sender.ready["endpoint"]
    assert message["senderSessionId"] == sender.ready["sessionId"]
    assert message["recipient"] == {
        "targetType": "endpoint",
        "endpoint": receiver.ready["endpoint"],
    }
    assert message["recipientSessionId"] == receiver.ready["sessionId"]
    assert message["subject"] == {
        "kind": "service",
        "identifiers": {
            "serviceId": f"action-runtime.{sender.config['instance']}",
            "namespace": "dev.deckr.action_runtime.provider",
            "name": name,
        },
    }
    body = message["body"]
    assert body["name"] == name and body["exchangePattern"] == "one_way"
    # Compare typed meaning: Rust and Python may serialize schema defaults.
    from deckr.action_runtime import action_runtime_payload

    actual = action_runtime_payload(name, body.get("event") or body.get("params", {}))
    expected = action_runtime_payload(name, payload)
    assert actual == expected, (name, actual, expected)
    if name == "binding_render_result":
        assert message["causationId"] == payload["requestMessageId"]
        assert message.get("inReplyTo") is None


async def pair(url, consumer_language, provider_language, directory):
    instance = f"matrix-{consumer_language}-{provider_language}"
    items = cases(instance)
    # The reference wire samples deliberately test payloads independently. The
    # controller workload below owns real binding/page state transitions.
    entries = [
        {
            "actionId": "example",
            "status": "available",
            "descriptor": {"actionId": "example"},
        }
    ]
    base = {
        "url": url,
        "instance": instance,
        "providerId": "example",
        "entries": entries,
    }
    provider = Peer(
        provider_language,
        {**base, "role": "provider", "id": instance},
        directory / "provider",
    )
    consumer = Peer(
        consumer_language,
        {**base, "role": "consumer", "id": instance},
        directory / "consumer",
    )
    records = []
    try:
        await provider.start()
        await consumer.start()
        state = await consumer.wait(lambda s: s["contract"] is not None)
        contract = state["contract"]
        assert contract["generation"] == 1
        await provider.wait(lambda s: s["contract"] == contract)
        async with asyncio.timeout(15):
            while True:
                availability = await consumer.command("availability")
                if availability.get("entries"):
                    break
                await asyncio.sleep(0.02)
        assert availability["entries"] == entries
        assert availability["serviceSessionId"] == provider.ready["sessionId"]
        order = [
            "action_instance_created",
            *[n for n in items if n != "action_instance_created"],
        ]
        render_id = None
        for name in order:
            case = items[name]
            sender, receiver = (
                (consumer, provider)
                if case["direction"] == "consumer_to_service"
                else (provider, consumer)
            )
            payload = copy.deepcopy(case["payload"])
            if name == "binding_render_result" and render_id:
                payload["requestMessageId"] = render_id
            receipt = await sender.command("send", name=name, payload=payload)
            message = await receiver.delivered(receipt["messageId"])
            assert_delivery(message, name, payload, sender, receiver, contract)
            if name == "binding_render":
                render_id = receipt["messageId"]
            records.append({"name": name, "message": message})
        # Public managed APIs must reject malformed page commands before publish.
        invalid = await provider.command(
            "send", name="close_page", payload={}, allow_error=True
        )
        assert "error" in invalid, invalid
        # Wrong session/contract cannot be authorized by a different language.
        for receiver, inbound in (
            (provider, records[0]["message"]),
            (
                consumer,
                next(
                    r["message"]
                    for r in records
                    if items[r["name"]]["direction"] == "service_to_consumer"
                ),
            ),
        ):
            for field in ("senderSessionId", "contract"):
                stale = copy.deepcopy(inbound)
                stale[field] = (
                    "wrong-session"
                    if field == "senderSessionId"
                    else {"contractId": "wrong-contract", "generation": 1}
                )
                assert "error" in await receiver.command(
                    "authorize", message=stale, allow_error=True
                )
        # Restart the provider cleanly; the same managed consumer must negotiate
        # a successor, reject predecessor traffic and observe fresh availability.
        old_provider_session = provider.ready["sessionId"]
        before_restart = len((await consumer.command("status"))["received"])
        old_inbound = next(
            r["message"]
            for r in records
            if items[r["name"]]["direction"] == "service_to_consumer"
        )
        await provider.stop()
        provider = Peer(
            provider_language,
            {**base, "role": "provider", "id": instance},
            directory / "provider-successor",
        )
        await provider.start()
        successor = await consumer.wait(
            lambda s: s["contract"] is not None and s["contract"] != contract,
            timeout=35,
        )
        assert successor["contract"]["generation"] == 1
        assert provider.ready["sessionId"] != old_provider_session
        assert len(successor["received"]) == before_restart
        await provider.wait(lambda s: s["contract"] == successor["contract"])
        async with asyncio.timeout(15):
            while True:
                current_view = await consumer.command("availability")
                if current_view.get("serviceSessionId") == provider.ready["sessionId"]:
                    break
                await asyncio.sleep(0.02)
        assert "error" in await consumer.command(
            "authorize", message=old_inbound, allow_error=True
        )
        receipt = await consumer.command(
            "send",
            name="action_instance_created",
            payload=items["action_instance_created"]["payload"],
        )
        await provider.delivered(receipt["messageId"])
        receipt = await provider.command(
            "send", name="open_page", payload=items["open_page"]["payload"]
        )
        fresh = await consumer.delivered(receipt["messageId"])
        assert fresh["contract"] == successor["contract"]
        assert_delivery(
            fresh,
            "open_page",
            items["open_page"]["payload"],
            provider,
            consumer,
            successor["contract"],
        )
        final_messages = (await consumer.command("status"))["received"]
        assert len(final_messages) == before_restart + 1
        assert len({m["messageId"] for m in final_messages}) == len(final_messages)
        await consumer.stop()
        await provider.stop()
        result = {
            "language": consumer_language,
            "target": provider_language,
            "id": instance,
            "contract": contract,
            "successor": successor["contract"],
            "messages": records,
            "checks": [
                "availability",
                "16-message-exchange",
                "malformed-page",
                "session-and-contract-fences",
                "provider-restart",
                "successor-delivery",
            ],
        }
        (directory / "result.json").write_text(json.dumps(result, indent=2))
        return result
    finally:
        for peer in (consumer, provider):
            if peer.process and peer.process.returncode is None:
                await peer.stop(check=False)


async def run(args, directory):
    from deckr.substrates.nats_server import NatsServerSupervisor

    if not args.skip_build:
        await build_rust(directory)
    supervisor = NatsServerSupervisor(
        port=-1, runtime_dir=directory / "broker", auth_enabled=False
    )
    handle = await supervisor.start()
    results = []
    try:
        for consumer in LANGUAGES:
            for provider in LANGUAGES:
                result = await pair(
                    handle.url, consumer, provider, directory / f"{consumer}-{provider}"
                )
                results.append(result)
                print(
                    f"PASS {consumer} consumer / {provider} runtime: "
                    "16 messages, availability, fences, successor",
                    flush=True,
                )
    finally:
        await supervisor.stop()
        (directory / "nats.log").write_text("\n".join(supervisor.recent_logs()))
    check_matrix_results(results)
    return results


def check_matrix_results(results):
    assert len(results) == 9
    assert {(r["language"], r["target"]) for r in results} == {
        (c, p) for c in LANGUAGES for p in LANGUAGES
    }
    for result in results:
        assert len(result["messages"]) == 16
        assert len({m["name"] for m in result["messages"]}) == 16
        assert result["successor"] != result["contract"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-source-changes", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    directory = (
        ROOT
        / "artifacts"
        / ("action-runtime-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    )
    directory.mkdir(parents=True)
    summary, results = {"status": "fail"}, []
    try:
        sources = source_manifest(args.allow_source_changes)
        sources["harness"] = source_snapshot(ROOT)
        (directory / "sources.json").write_text(json.dumps(sources, indent=2))
        results = asyncio.run(run(args, directory))
        summary = {
            "status": "pass",
            "pairs": len(results),
            "messages": sum(len(r["messages"]) for r in results),
        }
    except BaseException as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        (directory / "summary.json").write_text(json.dumps(summary, indent=2))
        write_junit(
            directory / "junit.xml", summary, results, suite_name="deckr-action-runtime"
        )
        print(f"Evidence: {directory}", flush=True)


if __name__ == "__main__":
    main()

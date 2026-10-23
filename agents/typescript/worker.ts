import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY, DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME } from "@deckr/core/beacon";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY, DEFAULT_CONCORD_CONTRACT_STORE_NAME, DEFAULT_CONCORD_TOKEN_STORE_NAME } from "@deckr/core/concord";
import { restartHandoffFromEnv } from "@deckr/core/restart";
import { EndpointSession } from "@deckr/core/messaging";
import { connectNats, NatsMessageTransport, NatsStateStore } from "@deckr/core/nats";
import { ManagedServiceClient, ManagedServiceProvider, ServiceViewStoreWriter, serviceProtocolViews, type ServiceProtocol } from "@deckr/core/services";
import type { JsonObject } from "@deckr/core/json";
import { consume, event, soaking } from "./soak.ts";

const language = "typescript";
const role = process.env.ROLE!;
const target = process.env.TARGET_LANGUAGE ?? language;
const id = process.env.PARTICIPANT_ID ?? `${role}-${language}-${target}-${process.env.HOSTNAME ?? randomUUID()}`;
const sessionId = randomUUID();
const runId = process.env.RUN_ID!;
const directory = process.env.PROTOCOL_DIR ?? "/protocol";
const protocol: ServiceProtocol = JSON.parse(readFileSync(`${directory}/service.json`, "utf8"));
const cases: JsonObject[] = JSON.parse(readFileSync(`${directory}/cases.json`, "utf8"));
const storeName = protocol.viewFamilies!.results!.storeName;
const serviceId = `interop-${language}`;
const common = { runId, id, language, role, target, sessionId };
function report(kind: string, value: object) {
  const data = { ...common, ...value };
  const path = `/artifacts/${id}.${kind}.json`;
  writeFileSync(`${path}.tmp`, JSON.stringify(data));
  renameSync(`${path}.tmp`, path);
  console.log(JSON.stringify({ event: kind, ...data }));
  if (soaking) event(common, kind, value);
}
const stop = new Promise<void>(resolve => {
  process.once("SIGTERM", resolve);
  process.once("SIGINT", resolve);
});

async function main() {
  if (soaking) event(common, "boot");
  const connection: any = await connectNats({ servers: process.env.NATS_URL, reconnectMaxIntervalSeconds: 1 });
  const transport = new NatsMessageTransport(connection);
  const session = new EndpointSession({ endpoint: role === "provider" ? `service:${serviceId}` : `controller:${id}`, sessionId, transport });
  console.log(JSON.stringify({ event: "endpoint-session", endpoint: session.endpoint, sessionId: session.sessionId }));
  const beaconStore = await NatsStateStore.open({ connection, bucket: DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME, policy: BEACON_ADVERTISEMENT_STORE_POLICY });
  const contracts = await NatsStateStore.open({ connection, bucket: DEFAULT_CONCORD_CONTRACT_STORE_NAME });
  const tokens = await NatsStateStore.open({ connection, bucket: DEFAULT_CONCORD_TOKEN_STORE_NAME, policy: CONCORD_TOKEN_STORE_POLICY });
  const views = await NatsStateStore.open({ connection, bucket: storeName });
  const beacon = new BeaconService(new BeaconDiscovery(beaconStore));
  const concord = new ConcordService(new ConcordCoordinator(contracts, tokens), { restart: restartHandoffFromEnv() });
  const shared = { protocol, beacon, concord, endpoint: session.lane("services"), stateFor: (name: string) => {
    assert.equal(name, storeName); return views;
  } };
  let provider: ManagedServiceProvider | undefined;
  let client: ManagedServiceClient | undefined;
  const retained = new Map<string, ServiceViewStoreWriter>();
  try {
    if (role === "provider") {
      provider = new ManagedServiceProvider({ ...shared, serviceId,
        onError: error => {
          if (soaking) { event(common, "providerError", { error: String(error) }); return; }
          report("result", { status: "fail", error: String(error) });
          console.error(error);
          process.exitCode = 1;
        },
        handle: async (body, context) => {
          const result = { ...body.params!, providerLanguage: language, providerSession: sessionId };
          let writer: ServiceViewStoreWriter;
          if (soaking) {
            // A continuously updated application view belongs to one agreement.
            // The convenience viewWriter factory retains a separate operation writer.
            const key = context.contract.contractId;
            const existing = retained.get(key);
            if (existing) writer = existing;
            else {
              writer = new ServiceViewStoreWriter({ state: views, context: await context.viewContext() });
              retained.set(key, writer);
              const owned = writer;
              const release = () => { if (retained.get(key) === owned) retained.delete(key); void owned.close(); };
              context.signal.addEventListener("abort", release, { once: true });
              if (context.signal.aborted) release();
            }
          } else writer = await context.viewWriter(storeName);
          const key = serviceProtocolViews(protocol, serviceId).results!.keyPrefix + (soaking ? "latest" : body.params!.operationId);
          if (soaking) {
            event(common, "execution", { operationId: body.params!.operationId });
            await writer.put({ storeName, key }, result);
          } else await writer.create({ storeName, key }, result);
          await context.assertActive();
          return { result };
        },
      });
      await provider.start();
      report("ready", { serviceId });
      await stop;
    } else {
      assert.equal(role, "client");
      client = new ManagedServiceClient({ ...shared, serviceId: `interop-${target}`, operations: ["echo"], views: ["results"], negotiationTimeoutMs: 30_000 });
      const lease = await client.ready(60_000);
      const contract = { contractId: lease.agreement.contract.contractId, generation: lease.agreement.contract.generation };
      const providerSession = lease.descriptor.sessionId;
      assert.equal(lease.descriptor.serviceId, `interop-${target}`);
      assert.equal(contract.generation, 1);
      report("ready", { contract, providerSession });
      const deadline = Date.now() + 90_000;
      while (!existsSync("/control/start")) {
        assert.ok(Date.now() < deadline, "start barrier timed out");
        await sleep(50);
      }
      if (soaking) {
        await consume(client, common, cases, storeName);
        report("result", { status: "pass" });
        return;
      }
      const observations = [];
      for (let round = 0; round < Number(process.env.ROUNDS ?? 2); round++) {
        for (const [caseIndex, payload] of cases.entries()) {
          const operationId = `${id}-${round}-${caseIndex}`;
          const expected = { operationId, payload, providerLanguage: target, providerSession };
          const response = await client.request("echo", { operationId, payload }, { timeoutMs: 15_000 });
          assert.equal(response.status, "ok");
          assert.deepStrictEqual(response.result, expected);
          const view = { storeName, key: lease.descriptor.views.results!.keyPrefix + operationId };
          const until = Date.now() + 15_000;
          let actual: JsonObject | null = null;
          while (Date.now() < until) {
            actual = await client.read(view);
            if (actual) break;
            await sleep(20);
          }
          assert.ok(actual, "protected view did not arrive");
          assert.equal(actual.consumerSessionId, sessionId);
          assert.equal(actual.serviceSessionId, providerSession);
          assert.equal(actual.contractId, contract.contractId);
          assert.equal(actual.generation, contract.generation);
          const value: JsonObject = Object.fromEntries(Object.keys(expected).map(key => [key, actual![key]]));
          assert.deepStrictEqual(value, expected);
          observations.push({ round, caseIndex, operationId, reply: response.result, view: value });
        }
      }
      report("result", { status: "pass", contract, providerSession, observations });
    }
  } finally {
    await Promise.all([...retained.values()].map(writer => writer.close()));
    await client?.close();
    await provider?.close();
    await session.close();
    await beacon.close();
    await concord.close();
    transport.close();
    await connection.close();
  }
}

main().catch(error => {
  report("result", { status: "fail", error: String(error), stack: error.stack });
  console.error(error);
  process.exitCode = 1;
});

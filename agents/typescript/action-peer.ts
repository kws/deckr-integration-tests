// Test application: public managed services carry the core Action Runtime catalogue.
import { randomUUID } from "node:crypto";
import type { JsonObject } from "../../../deckr-core-typescript/src/json.ts";
import { createInterface } from "node:readline";
import { BeaconDiscovery, BeaconService, BEACON_ADVERTISEMENT_STORE_POLICY, DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME } from "../../../deckr-core-typescript/src/beacon.ts";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY, DEFAULT_CONCORD_CONTRACT_STORE_NAME, DEFAULT_CONCORD_TOKEN_STORE_NAME } from "../../../deckr-core-typescript/src/concord.ts";
import { EndpointSession } from "../../../deckr-core-typescript/src/messaging.ts";
import { connectNats, NatsMessageTransport, NatsStateStore } from "../../../deckr-core-typescript/src/nats.ts";
import { ManagedServiceClient, ManagedServiceProvider, ServiceMessageChannel, ServiceViewStoreWriter, serviceViewKey } from "../../../deckr-core-typescript/src/services.ts";
import { actionRuntimeProtocol, actionAvailabilityPayload, actionRuntimePayloadSchemas, validateActionRuntimeMessage, validateActionRuntimePayload } from "../../../deckr-core-typescript/src/actions.ts";

const config = JSON.parse(process.argv[2]!);
const providerRole = config.role === "provider";
const serviceId = `action-runtime.${config.instance}`;
const sessionId = randomUUID();
const protocol = actionRuntimeProtocol();
const family = "action_availability";
const storeName = protocol.viewFamilies![family]!.storeName;
const received: any[] = [], errors: string[] = [];
const contexts = new Map<string, any>(), writers = new Map<string, any>();
const connection: any = await connectNats({ servers: config.url });
const transport = new NatsMessageTransport(connection);
const session = new EndpointSession({ endpoint: providerRole ? `service:${serviceId}` : `controller:${config.id}`, sessionId, transport });
const beaconStore = await NatsStateStore.open({ connection, bucket: DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME, policy: BEACON_ADVERTISEMENT_STORE_POLICY });
const contracts = await NatsStateStore.open({ connection, bucket: DEFAULT_CONCORD_CONTRACT_STORE_NAME });
const tokens = await NatsStateStore.open({ connection, bucket: DEFAULT_CONCORD_TOKEN_STORE_NAME, policy: CONCORD_TOKEN_STORE_POLICY });
const views = await NatsStateStore.open({ connection, bucket: storeName });
const beacon = new BeaconService(new BeaconDiscovery(beaconStore));
const concord = new ConcordService(new ConcordCoordinator(contracts, tokens));
const shared = { protocol, beacon, concord, endpoint: session.lane("services"), stateFor: () => views, schemas: actionRuntimePayloadSchemas };
let provider: ManagedServiceProvider | undefined, client: ManagedServiceClient | undefined, lease: any;
let messageTask: Promise<void> | undefined;
const stop = new AbortController();
function record(message: any) { validateActionRuntimeMessage(message); received.push(message); }
try {
  if (providerRole) {
    provider = new ManagedServiceProvider({ ...shared, serviceId,
      onError: error => errors.push(String(error)),
      onAgreement: async context => {
        const writer = new ServiceViewStoreWriter({ state: views, context });
        writers.set(context.contract.contractId, writer);
        contexts.set(context.contract.contractId, context);
        await writer.put({ storeName, key: serviceViewKey(serviceId, family, "current") }, actionAvailabilityPayload({ providerInstanceId: config.instance, providerId: config.providerId, sessionId, entries: config.entries }) as unknown as JsonObject);
      },
      handle: (_body, context) => {
        record(context.message);
        contexts.set(context.contract.contractId, context);
      },
    });
    await provider.start();
  } else {
    client = new ManagedServiceClient({ ...shared, serviceId, operations: Object.keys(protocol.operations), views: [family] });
    lease = await client.ready(30_000);
    messageTask = (async () => {
      for await (const delivery of shared.endpoint.watch!({ signal: stop.signal })) {
        try {
          const current = await client!.ready(1000);
          await new ServiceMessageChannel(shared).receive(current, delivery.message);
        } catch { continue; }
        record(delivery.message);
      }
    })().catch(error => { if (!stop.signal.aborted) errors.push(String(error)); });
    // Register the watch before the host can send a command.
    await new Promise(resolve => setImmediate(resolve));
  }
  console.log(JSON.stringify({ ready: true, sessionId, endpoint: session.endpoint }));
  const input = createInterface({ input: process.stdin });
  for await (const line of input) {
    const command = JSON.parse(line);
    let result: any;
    try {
      const context: any = [...contexts.values()].find(c => !c.signal?.aborted && !c.authority?.signal.aborted);
      if (command.op === "stop") { console.log(JSON.stringify({ ok: true })); break; }
      if (command.op === "status") result = { contract: providerRole ? context?.contract ?? null : client!.state.contract ?? null, received, errors };
      else if (command.op === "availability") {
        lease = await client!.ready(30_000);
        result = await client!.read({ storeName, key: serviceViewKey(serviceId, family, "current") }) ?? {};
      } else if (command.op === "send") {
        const payload = validateActionRuntimePayload(command.name, command.payload);
        const messageId = randomUUID();
        const options = { messageId, ...(command.name === "binding_render_result" ? { causationId: String(payload.requestMessageId) } : {}) };
        if (providerRole) {
          const selected = command.contractId ? contexts.get(command.contractId) : context;
          if (!selected?.send) throw new Error("provider needs an incoming lifecycle event before sending");
          await selected.send(command.name, payload, undefined, undefined, options);
        } else {
          lease = await client!.ready(30_000);
          await new ServiceMessageChannel(shared).send(lease, command.name, payload, undefined, undefined, options);
        }
        result = { messageId };
      } else if (command.op === "authorize") {
        if (providerRole) {
          const decision = await provider!.authorizeMessage(command.message);
          if (decision !== "authorized") throw new Error(decision);
        } else {
          lease = await client!.ready(30_000);
          await new ServiceMessageChannel(shared).receive(lease, command.message);
        }
        result = { authorized: true };
      } else throw new Error(`unknown operation ${command.op}`);
    } catch (error) { result = { error: String(error) }; }
    console.log(JSON.stringify(result));
  }
} finally {
  process.stdin.pause();
  stop.abort();
  await client?.close();
  console.error("consumer closed");
  await provider?.close();
  console.error("provider closed");
  await messageTask;
  await Promise.all([...writers.values()].map(writer => writer.close()));
  await session.close(); await beacon.close(); await concord.close();
  transport.close(); await connection.close();
  console.error("transport closed");
}

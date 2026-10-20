import assert from "node:assert/strict";
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { ManagedServiceClient, type ServiceUseLease } from "@deckr/core/services";
import type { JsonObject } from "@deckr/core/json";

export const soaking = process.env.MODE === "soak";
async function epochValue(): Promise<number> {
  const deadline = Date.now() + 1000;
  while (true) {
    try { return Number(readFileSync("/control/epoch", "utf8")); }
    catch (error) { if (Date.now() >= deadline || (error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    await sleep(10);
  }
}
export function event(common: object & { id: string }, kind: string, data: object = {}) {
  appendFileSync(`/artifacts/${common.id}.events.jsonl`, JSON.stringify({ ...common, kind, atMs: Date.now(), ...data }) + "\n");
}

export async function consume(client: ManagedServiceClient, common: { id: string; sessionId: string; target: string }, cases: JsonObject[], storeName: string) {
  const timeout = Number(process.env.OPERATION_TIMEOUT ?? 10) * 1000;
  let previous: ServiceUseLease | undefined;
  let sequence = 0;
  while (!existsSync("/control/stop")) {
    const seq = sequence++;
    const epoch = await epochValue();
    const caseIndex = seq % cases.length;
    const operationId = `${common.id}-${common.sessionId}-${seq}`;
    const started = Date.now();
    let phase = "ready";
    try {
      const lease = await client.ready(timeout);
      const contract = { contractId: lease.agreement.contract.contractId, generation: lease.agreement.contract.generation };
      const providerSession = lease.descriptor.sessionId;
      const previousContract = previous && { contractId: previous.agreement.contract.contractId, generation: previous.agreement.contract.generation };
      let oldAuthorityRejected: boolean | null = null;
      if (previousContract && previousContract.contractId !== contract.contractId) {
        oldAuthorityRejected = previous!.agreement.signal.aborted && !previous!.agreement.valid;
        assert.ok(oldAuthorityRejected, "retired agreement regained authority");
      }
      const expected = { operationId, payload: cases[caseIndex], providerLanguage: common.target, providerSession };
      phase = "request";
      const response = await client.request("echo", { operationId, payload: cases[caseIndex] }, { timeoutMs: timeout });
      if (response.status !== "ok") throw new Error(`request unavailable: ${JSON.stringify(response)}`);
      if (client.state.contract?.contractId !== contract.contractId) throw new Error("agreement changed during operation");
      assert.deepStrictEqual(response.result, expected);
      phase = "view";
      const view = { storeName, key: lease.descriptor.views.results!.keyPrefix + "latest" };
      let actual: JsonObject | null = null;
      const deadline = Date.now() + timeout;
      while (Date.now() < deadline) {
        actual = await client.read(view);
        if (actual?.operationId === operationId) break;
        await sleep(20);
      }
      if (actual?.operationId !== operationId) throw new Error("view unavailable");
      assert.equal(actual.consumerSessionId, common.sessionId);
      assert.equal(actual.serviceSessionId, providerSession);
      assert.equal(actual.contractId, contract.contractId);
      assert.equal(actual.generation, 1);
      const value = Object.fromEntries(Object.keys(expected).map(key => [key, actual![key]]));
      assert.deepStrictEqual(value, expected);
      event(common, "success", { seq, epoch, caseIndex, operationId, contract, previousContract, oldAuthorityRejected, providerSession, reply: response.result, view: value, latencyMs: Date.now() - started });
      previous = lease;
    } catch (error) {
      if (error instanceof assert.AssertionError) throw error;
      event(common, "unavailable", { seq, epoch, operationId, error: String(error), phase, state: client.state });
    }
    await sleep(Number(process.env.OPERATION_INTERVAL ?? 1) * 1000);
  }
}

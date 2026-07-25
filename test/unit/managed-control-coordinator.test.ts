import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { computeManagedRequestDigest } from "../../src/api/managed-dispatch.ts";
import { computeParentSessionIdentityDigest } from "../../src/api/preflight.ts";
import { ManagedControlCoordinator, ManagedControlCoordinatorError } from "../../src/managed/control-coordinator.ts";
import { ManagedOperationJournal, ManagedOperationJournalError } from "../../src/managed/operation-journal.ts";
import {
	consumeManagedControlRequests,
	managedControlAckPath,
	managedControlRequestPath,
	publishManagedControlRequest,
	writeManagedControlAck,
} from "../../src/runs/background/control-channel.ts";

let temporary = "";
let parentFile = "";
let parentDigest = "";
let store: ManagedOperationJournal;

beforeEach(() => {
	temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-control-coordinator-"));
	parentFile = path.join(temporary, "parent.jsonl");
	fs.writeFileSync(parentFile, "{}\n");
	parentDigest = computeParentSessionIdentityDigest("parent-session", parentFile);
	store = new ManagedOperationJournal({ root: path.join(temporary, "journal") });
});

afterEach(() => {
	store.close();
	fs.rmSync(temporary, { recursive: true, force: true });
});

function operationId(byte: number): string {
	return Buffer.alloc(32, byte).toString("base64url");
}

function context(sessionId = "parent-session", sessionFile = parentFile): ExtensionContext {
	return {
		sessionManager: { getSessionId: () => sessionId, getSessionFile: () => sessionFile },
	} as unknown as ExtensionContext;
}

function launchRequest(byte: number, runId: string): Record<string, unknown> {
	return {
		version: 1,
		requestId: `launch-${byte}`,
		method: "spawn",
		managed: { version: 1, consumerId: "consumer-a", operationId: operationId(byte) },
		expectedLaunch: {
			version: 1,
			hostId: "host-1",
			candidateRunId: runId,
			profileIdentityDigest: "a".repeat(64),
			parentSessionIdentityDigest: parentDigest,
			contractDigest: "b".repeat(64),
		},
		input: { request: { agent: "worker", task: "private", async: true, clarify: false, context: "fresh", cwd: temporary, sessionDir: path.join(temporary, "sessions", runId) } },
	};
}

function createActor(byte: number, runId: string, state: "accepted" | "terminal" | "uncertain" | "failed-before-launch" = "accepted"): string {
	const request = launchRequest(byte, runId);
	const digest = computeManagedRequestDigest(request);
	const id = operationId(byte);
	store.claim(parentDigest, request);
	store.transition(parentDigest, "consumer-a", id, digest, "prepared");
	if (state === "failed-before-launch") {
		store.transition(parentDigest, "consumer-a", id, digest, state);
		return id;
	}
	const asyncDir = path.join(temporary, "async", runId);
	const sessionFile = path.join(temporary, "sessions", runId, "run-0", "session.jsonl");
	fs.mkdirSync(asyncDir, { recursive: true });
	store.transition(parentDigest, "consumer-a", id, digest, "dispatching", { runId, terminalAsyncDir: asyncDir, canonicalSessionFile: sessionFile });
	if (state === "uncertain") {
		store.transition(parentDigest, "consumer-a", id, digest, "uncertain");
		return id;
	}
	store.transition(parentDigest, "consumer-a", id, digest, "runner-ready", {
		runnerProcessInstanceId: `runner-${byte}`,
		runnerAdmissionTokenDigest: "c".repeat(64),
	});
	store.transition(parentDigest, "consumer-a", id, digest, "accepted");
	if (state === "terminal") {
		store.transition(parentDigest, "consumer-a", id, digest, "terminal", {
			terminalEvidence: { version: 1, proofDigest: "d".repeat(64), observedAt: 100, canonicalSessionId: "e".repeat(64) },
		});
	} else {
		fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId, mode: "single", state: "running", steps: [] }));
	}
	return id;
}

function controlRequest(method: "steer" | "interrupt" | "stop" | "retire", commandByte: number, target: Record<string, unknown>, extra: Record<string, unknown> = {}): Record<string, unknown> {
	return {
		version: 1,
		requestId: `control-${commandByte}`,
		method,
		managed: { version: 1, consumerId: "consumer-a", operationId: operationId(commandByte) },
		input: { target: { consumerId: "consumer-a", ...target }, ...(method === "steer" ? { message: "raw secret steering text" } : {}), ...extra },
	};
}

function coordinator(publishRequest = publishManagedControlRequest): ManagedControlCoordinator {
	const ctx = context();
	return new ManagedControlCoordinator({
		journal: store,
		getContext: () => ctx,
		getSessionGeneration: () => 1,
		publishRequest,
	});
}

function expectCode(error: unknown, code: string): boolean {
	return (error instanceof ManagedControlCoordinatorError || error instanceof ManagedOperationJournalError) && error.code === code;
}

describe("unregistered managed control coordinator", () => {
	it("binds same-parent and consumer exact actor authority and never republishes replay", async () => {
		const actorId = createActor(1, "actor-one");
		let published = 0;
		const control = coordinator((...args) => { published++; return publishManagedControlRequest(...args); });
		const request = controlRequest("steer", 20, { operationId: actorId });
		const first = await control.dispatchControl(request);
		assert.equal(first.state, "accepted");
		assert.equal(published, 1);
		const command = store.read(parentDigest, "consumer-a", operationId(20));
		assert.equal(command?.targetOperationId, actorId);
		assert.equal(command?.actorOperationId, actorId);
		assert.equal(command?.actorRunId, "actor-one");
		assert.equal(JSON.stringify(command).includes("raw secret steering text"), false, "journal must retain only the semantic digest");
		const replay = await control.dispatchControl({ ...request, requestId: "transport-retry" });
		assert.equal(replay.replayed, true);
		assert.equal(replay.state, "accepted");
		assert.equal(published, 1);
		assert.equal(fs.readFileSync(managedControlRequestPath(path.join(temporary, "async", "actor-one"), operationId(20)), "utf8").includes("raw secret steering text"), true);
	});

	it("reconciles a correlated runner ack without republishing", async () => {
		createActor(2, "actor-two");
		let published = 0;
		const control = coordinator((...args) => { published++; return publishManagedControlRequest(...args); });
		const request = controlRequest("interrupt", 21, { runId: "actor-two" });
		assert.equal((await control.dispatchControl(request)).state, "accepted");
		consumeManagedControlRequests(path.join(temporary, "async", "actor-two"), () => ({ outcome: "acknowledged" }));
		const replay = await control.dispatchControl({ ...request, requestId: "retry" });
		assert.equal(replay.state, "terminal");
		assert.equal(store.read(parentDigest, "consumer-a", operationId(21))?.controlOutcome, "acknowledged");
		assert.equal(published, 1);
	});

	it("makes consume-before-ack uncertainty sticky even if an ack appears later", async () => {
		createActor(3, "actor-three");
		let published = 0;
		const control = coordinator((...args) => { published++; return publishManagedControlRequest(...args); });
		const request = controlRequest("stop", 22, { runId: "actor-three" });
		await control.dispatchControl(request);
		const asyncDir = path.join(temporary, "async", "actor-three");
		consumeManagedControlRequests(asyncDir, () => ({ outcome: "acknowledged" }));
		fs.rmSync(managedControlAckPath(asyncDir, operationId(22)));
		assert.equal((await control.dispatchControl({ ...request, requestId: "retry-1" })).state, "uncertain");
		writeManagedControlAck(asyncDir, { version: 1, commandId: operationId(22), method: "stop", runId: "actor-three", acknowledgedAt: 200, outcome: "acknowledged" });
		assert.equal((await control.dispatchControl({ ...request, requestId: "retry-2" })).state, "uncertain");
		assert.equal(published, 1);
	});

	it("rejects prefix, cross-consumer, cross-parent, command-record, and inactive targets", async () => {
		const actorId = createActor(4, "actor-four");
		const control = coordinator();
		await assert.rejects(control.dispatchControl(controlRequest("steer", 23, { runId: "actor-fou" })), (error) => expectCode(error, "not_found"));
		const crossConsumer = controlRequest("steer", 24, { operationId: actorId });
		(crossConsumer.input as any).target.consumerId = "consumer-b";
		await assert.rejects(control.dispatchControl(crossConsumer), (error) => expectCode(error, "invalid_request"));
		const firstCommand = controlRequest("steer", 25, { operationId: actorId });
		await control.dispatchControl(firstCommand);
		await assert.rejects(control.dispatchControl(controlRequest("stop", 26, { operationId: operationId(25) })), (error) => expectCode(error, "invalid_state"));
		const otherParent = new ManagedControlCoordinator({ journal: store, getContext: () => context("other", path.join(temporary, "other.jsonl")) });
		await assert.rejects(otherParent.dispatchControl(controlRequest("steer", 27, { operationId: actorId })), (error) => expectCode(error, "not_found"));
		fs.writeFileSync(path.join(temporary, "async", "actor-four", "status.json"), JSON.stringify({ runId: "actor-four", mode: "single", state: "paused", steps: [] }));
		await assert.rejects(control.dispatchControl(controlRequest("interrupt", 28, { operationId: actorId })), (error) => expectCode(error, "invalid_state"));
	});

	it("durably tombstones safe actors, rejects active retirement, and gates uncertainty acknowledgment", async () => {
		const activeId = createActor(5, "actor-active");
		const terminalId = createActor(6, "actor-terminal", "terminal");
		const uncertainId = createActor(7, "actor-uncertain", "uncertain");
		const failedId = createActor(8, "actor-never-launched", "failed-before-launch");
		const control = coordinator();
		await assert.rejects(control.dispatchControl(controlRequest("retire", 29, { operationId: activeId })), (error) => expectCode(error, "invalid_state"));
		assert.equal((await control.dispatchControl(controlRequest("retire", 30, { operationId: terminalId }))).state, "terminal");
		assert.equal(store.read(parentDigest, "consumer-a", terminalId)?.state, "retired");
		assert.equal(store.readByRun(parentDigest, "consumer-a", "actor-terminal")?.retiredByOperationId, operationId(30));
		const retireRecordPath = path.join(store.root, "operations", parentDigest, "consumer-a", operationId(30), "record.json");
		const halfCompleted = JSON.parse(fs.readFileSync(retireRecordPath, "utf8"));
		fs.writeFileSync(retireRecordPath, `${JSON.stringify({ ...halfCompleted, state: "prepared" })}\n`, { mode: 0o600 });
		assert.equal((await control.dispatchControl({ ...controlRequest("retire", 30, { operationId: terminalId }), requestId: "retire-saga-retry" })).state, "terminal");
		assert.equal((await control.dispatchControl({ ...controlRequest("retire", 30, { operationId: terminalId }), requestId: "retire-retry" })).replayed, true);
		await assert.rejects(control.dispatchControl(controlRequest("retire", 31, { operationId: uncertainId })), (error) => expectCode(error, "operation_uncertain"));
		assert.equal((await control.dispatchControl(controlRequest("retire", 32, { operationId: uncertainId }, { acknowledgeUncertain: true }))).state, "terminal");
		assert.equal(store.read(parentDigest, "consumer-a", uncertainId)?.state, "retired");
		assert.equal((await control.dispatchControl(controlRequest("retire", 33, { operationId: failedId }))).state, "terminal");
		assert.equal(store.read(parentDigest, "consumer-a", failedId)?.state, "retired");
		await assert.rejects(control.dispatchControl(controlRequest("stop", 34, { operationId: terminalId })), (error) => expectCode(error, "retired"));
	});
});

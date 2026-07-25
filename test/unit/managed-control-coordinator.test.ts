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

describe("managed control coordinator", () => {
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

	it("rejects pre-existing request substitution by command, method/run, and steer body without execution", async () => {
		const actorId = createActor(9, "actor-nine");
		const actor = store.read(parentDigest, "consumer-a", actorId)!;
		const variants = [
			(body: Record<string, unknown>) => ({ ...body, commandId: operationId(99) }),
			(body: Record<string, unknown>) => { const { message: _message, ...rest } = body; return { ...rest, method: "stop", runId: "other-run" }; },
			(body: Record<string, unknown>) => ({ ...body, message: "substituted steer" }),
		];
		for (const [index, mutate] of variants.entries()) {
			const commandByte = 40 + index;
			const intended = controlRequest("steer", commandByte, { operationId: actorId });
			const body = mutate({
				version: 1,
				commandId: operationId(commandByte),
				commandRequestDigest: computeManagedRequestDigest(intended),
				consumerId: "consumer-a",
				targetOperationId: actorId,
				actorOperationId: actorId,
				actorRequestDigest: actor.requestDigest,
				runId: actor.runId,
				runnerProcessInstanceId: actor.runnerProcessInstanceId,
				method: "steer",
				requestedAt: 100,
				targetIndex: 0,
				message: "raw secret steering text",
			});
			const requestPath = managedControlRequestPath(actor.terminalAsyncDir!, operationId(commandByte));
			fs.mkdirSync(path.dirname(requestPath), { recursive: true, mode: 0o700 });
			fs.writeFileSync(requestPath, JSON.stringify(body), { mode: 0o600 });
			assert.equal((await coordinator().dispatchControl(intended)).state, "uncertain");
			let executions = 0;
			consumeManagedControlRequests(actor.terminalAsyncDir!, () => { executions++; return { outcome: "acknowledged" }; });
			assert.equal(executions, 0, `forged request variant ${index} must not execute`);
		}
	});

	it("requires exact consumed proof and never gives an ack precedence over a pending request", async () => {
		const actorId = createActor(10, "actor-ten");
		const actor = store.read(parentDigest, "consumer-a", actorId)!;
		for (const [offset, lifecycle] of ["ack-only", "request-and-ack", "corrupt-consumed-and-ack"].entries()) {
			const commandByte = 50 + offset;
			const intended = controlRequest("interrupt", commandByte, { operationId: actorId });
			const binding = {
				commandId: operationId(commandByte),
				commandRequestDigest: computeManagedRequestDigest(intended),
				consumerId: "consumer-a",
				targetOperationId: actorId,
				actorOperationId: actorId,
				actorRequestDigest: actor.requestDigest,
				runId: actor.runId!,
				runnerProcessInstanceId: actor.runnerProcessInstanceId!,
				method: "interrupt" as const,
			};
			if (lifecycle === "request-and-ack") publishManagedControlRequest(actor.terminalAsyncDir!, { ...binding, requestedAt: 100 });
			if (lifecycle === "corrupt-consumed-and-ack") {
				const consumedPath = path.join(actor.terminalAsyncDir!, "control", "managed-consumed", `${operationId(commandByte)}.json`);
				fs.mkdirSync(path.dirname(consumedPath), { recursive: true, mode: 0o700 });
				fs.writeFileSync(consumedPath, "{}", { mode: 0o600 });
			}
			writeManagedControlAck(actor.terminalAsyncDir!, { version: 1, ...binding, acknowledgedAt: 101, outcome: "acknowledged" });
			assert.equal((await coordinator().dispatchControl(intended)).state, "uncertain", lifecycle);
		}
	});

	it("serializes concurrent retries to one publication", async () => {
		const actorId = createActor(11, "actor-eleven");
		let publications = 0;
		const control = coordinator((...args) => { publications++; return publishManagedControlRequest(...args); });
		const request = controlRequest("stop", 54, { operationId: actorId });
		const [first, second] = await Promise.all([
			control.dispatchControl(request),
			control.dispatchControl({ ...request, requestId: "concurrent-retry" }),
		]);
		assert.equal(publications, 1);
		assert.deepEqual([first.replayed, second.replayed].sort(), [false, true]);
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
		const command = store.read(parentDigest, "consumer-a", operationId(22))!;
		const actor = store.read(parentDigest, "consumer-a", operationId(3))!;
		writeManagedControlAck(asyncDir, {
			version: 1,
			commandId: operationId(22),
			commandRequestDigest: command.requestDigest,
			consumerId: command.consumerId,
			targetRunId: command.targetRunId!,
			actorOperationId: actor.operationId,
			actorRequestDigest: actor.requestDigest,
			runnerProcessInstanceId: actor.runnerProcessInstanceId!,
			method: "stop",
			runId: "actor-three",
			acknowledgedAt: 200,
			outcome: "acknowledged",
		});
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

	it("enforces retirement target/actor equality at prepare, parse, and completion", () => {
		const targetId = createActor(12, "target-never-launched", "failed-before-launch");
		const wrongId = createActor(13, "wrong-never-launched", "failed-before-launch");
		const request = controlRequest("retire", 55, { operationId: targetId });
		const digest = computeManagedRequestDigest(request);
		store.claim(parentDigest, request);
		const wrong = store.read(parentDigest, "consumer-a", wrongId)!;
		assert.throws(() => store.prepareControl(parentDigest, "consumer-a", operationId(55), digest, {
			operationId: wrongId, requestDigest: wrong.requestDigest,
		}), (error: unknown) => error instanceof ManagedOperationJournalError && error.code === "operation_conflict");
		const target = store.read(parentDigest, "consumer-a", targetId)!;
		store.prepareControl(parentDigest, "consumer-a", operationId(55), digest, { operationId: targetId, requestDigest: target.requestDigest });
		const recordPath = path.join(store.root, "operations", parentDigest, "consumer-a", operationId(55), "record.json");
		const durable = JSON.parse(fs.readFileSync(recordPath, "utf8"));
		fs.writeFileSync(recordPath, `${JSON.stringify({ ...durable, actorOperationId: wrongId, actorRequestDigest: wrong.requestDigest })}\n`, { mode: 0o600 });
		assert.throws(() => store.read(parentDigest, "consumer-a", operationId(55)), (error: unknown) => error instanceof ManagedOperationJournalError && error.code === "corrupt");
		assert.throws(() => store.completeRetirement(parentDigest, "consumer-a", operationId(55), digest), (error: unknown) => error instanceof ManagedOperationJournalError && error.code === "corrupt");
		assert.equal(store.read(parentDigest, "consumer-a", targetId)?.state, "failed-before-launch");
		assert.equal(store.read(parentDigest, "consumer-a", wrongId)?.state, "failed-before-launch");
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

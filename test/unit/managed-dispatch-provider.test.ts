import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, computeManagedRequestDigest, managedDispatchReplyEvent } from "../../src/api/managed-dispatch.ts";
import { computeParentSessionIdentityDigest, type SubagentLaunchContract } from "../../src/api/preflight.ts";
import { ManagedDispatchProvider } from "../../src/extension/managed-dispatch-provider.ts";
import { ManagedOperationJournal } from "../../src/managed/operation-journal.ts";
import { createPreparedRunnerAdmission, computePreparedRunnerAdmissionTokenDigest, writePreparedRunnerAdmissionEvidence } from "../../src/runs/background/prepared-runner-admission.ts";
import type { ManagedSpawnExecutor } from "../../src/managed/spawn-coordinator.ts";
import type { SubagentParamsLike } from "../../src/runs/foreground/subagent-executor.ts";

let temporary = "";
let generation = 1;

beforeEach(() => {
	temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-provider-"));
	generation = 1;
});

afterEach(() => fs.rmSync(temporary, { recursive: true, force: true }));

function operationId(byte = 7): string { return Buffer.alloc(32, byte).toString("base64url"); }

class Bus {
	readonly handlers = new Map<string, Set<(payload: unknown) => void>>();
	on(event: string, handler: (payload: unknown) => void): () => void {
		const set = this.handlers.get(event) ?? new Set();
		set.add(handler);
		this.handlers.set(event, set);
		return () => set.delete(handler);
	}
	emit(event: string, payload: unknown): void {
		for (const handler of [...(this.handlers.get(event) ?? [])]) handler(payload);
	}
	request(payload: Record<string, unknown>): Promise<unknown> {
		return new Promise((resolve) => {
			const reply = managedDispatchReplyEvent(String(payload.requestId));
			const off = this.on(reply, (value) => { off(); resolve(value); });
			this.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, payload);
		});
	}
}

function context(): ExtensionContext {
	return {
		cwd: temporary,
		model: { provider: "test", id: "parent" },
		modelRegistry: { getAvailable: () => [] },
		sessionManager: {
			getSessionId: () => "parent-session",
			getSessionFile: () => path.join(temporary, "parent.jsonl"),
		},
	} as unknown as ExtensionContext;
}

function request(runId: string, id = operationId()): Record<string, unknown> {
	const parentDigest = computeParentSessionIdentityDigest("parent-session", path.join(temporary, "parent.jsonl"));
	return {
		version: 1,
		requestId: "spawn-transport",
		method: "spawn",
		managed: { version: 1, consumerId: "pi-signal", operationId: id },
		expectedLaunch: {
			version: 1,
			hostId: "host-1",
			candidateRunId: runId,
			profileIdentityDigest: "a".repeat(64),
			parentSessionIdentityDigest: parentDigest,
			contractDigest: "b".repeat(64),
		},
		input: { request: { agent: "worker", task: "opaque", context: "fresh", async: true, clarify: false, cwd: temporary, sessionDir: path.join(temporary, `session-${runId}`), artifacts: false, output: false } },
	};
}

function resolved(runId: string, payload: Record<string, unknown>) {
	const sessionRoot = path.join(temporary, `session-${runId}`);
	const sessionDir = path.join(sessionRoot, "run-0");
	const asyncDir = path.join(temporary, "async", runId);
	const contract: SubagentLaunchContract = {
		version: 1,
		runId,
		parentSessionIdentityDigest: (payload.expectedLaunch as { parentSessionIdentityDigest: string }).parentSessionIdentityDigest,
		agent: { name: "worker", source: "project", filePath: path.join(temporary, "worker.md"), definitionDigest: "d".repeat(64), shadowedCandidates: [] },
		context: "fresh",
		modelCandidates: [],
		systemPromptMode: "replace",
		inheritProjectContext: false,
		inheritSkills: false,
		skills: { requested: [], resolved: [], missing: [] },
		tools: { requestedBuiltin: [], declaredBuiltin: [], effectiveAllowlist: [], explicitAllowlist: true, requiredChildTools: [], internalTools: [], mcp: [], effectiveMcpTools: [], toolExtensionPaths: [], runtimeExtensions: [], configuredExtensions: [], extensionArgs: [], disableAmbientExtensions: true, fanoutAuthorized: false },
		roots: {
			cwd: temporary,
			sessionRoot,
			sessionDir,
			sessionFile: path.join(sessionDir, "session.jsonl"),
			asyncDir,
			resultPath: path.join(temporary, "results", `${runId}.json`),
			resultReservationPath: path.join(temporary, "results", `${runId}.reservation.json`),
			runnerConfigPath: path.join(temporary, "config", `${runId}.json`),
			runnerAdmissionPath: path.join(asyncDir, "runner-admission.json"),
			runnerAdmissionProceedPath: path.join(asyncDir, "runner-admission-proceed.json"),
			runnerAdmissionCommitPath: path.join(asyncDir, "runner-admission-commit.json"),
			attestations: {},
		},
		protocol: { lifecycleArtifactVersion: 3, packageVersion: "test" },
		diagnostics: [],
		digest: "b".repeat(64),
	};
	return { params: (payload.input as { request: SubagentParamsLike }).request, contract, profile: { version: 1 as const, contentDigest: "e".repeat(64), root: { version: 1 as const, realPath: temporary } }, profileIdentityDigest: "a".repeat(64) };
}

function executor(runId: string, payload: Record<string, unknown>, calls: { value: number }): ManagedSpawnExecutor {
	return {
		executePreparedSpawn: (async (_id, _params, _signal, _update, _ctx, options) => {
			calls.value++;
			const value = resolved(runId, payload).contract;
			const plan = {
				runId,
				parentSessionId: "parent-session",
				parentSessionFile: path.join(temporary, "parent.jsonl"),
				cwd: value.roots.cwd,
				sessionRoot: value.roots.sessionRoot!, sessionDir: value.roots.sessionDir!, sessionFile: value.roots.sessionFile!,
				asyncDir: value.roots.asyncDir!, resultPath: value.roots.resultPath!, resultReservationPath: value.roots.resultReservationPath!,
				runnerConfigPath: value.roots.runnerConfigPath!, runnerAdmissionPath: value.roots.runnerAdmissionPath!,
				runnerAdmissionProceedPath: value.roots.runnerAdmissionProceedPath!, runnerAdmissionCommitPath: value.roots.runnerAdmissionCommitPath!,
			};
			await options.beforeLaunch(plan);
			options.afterAuthorization(plan);
			const admission = createPreparedRunnerAdmission(runId, options.dispatchIdentityDigest);
			const ready = { ...admission, state: "ready" as const, pid: 10, runnerProcessInstanceId: "runner-1", observedAt: 10 };
			options.onRunnerReady(ready);
			options.onRunnerAccepted({ ...ready, state: "accepted", observedAt: 11 });
			fs.mkdirSync(path.dirname(value.roots.runnerAdmissionPath!), { recursive: true });
			writePreparedRunnerAdmissionEvidence(value.roots.runnerAdmissionPath!, admission, "committed", 10, "runner-1", 12);
			assert.equal(computePreparedRunnerAdmissionTokenDigest(admission.token).length, 64);
			return { content: [{ type: "text", text: "started" }], details: { mode: "single", results: [] } };
		}) as ManagedSpawnExecutor["executePreparedSpawn"],
	};
}

async function readyProvider(options: { seed?: boolean } = {}) {
	const bus = new Bus();
	const journalRoot = path.join(temporary, "journal");
	const hostIdPath = path.join(temporary, "host-id");
	fs.writeFileSync(hostIdPath, "host-1\n", { mode: 0o600 });
	const ctx = context();
	const seeded = request("seed-run", operationId(8));
	if (options.seed) {
		const store = new ManagedOperationJournal({ root: journalRoot });
		const digest = computeManagedRequestDigest(seeded);
		store.claim((seeded.expectedLaunch as { parentSessionIdentityDigest: string }).parentSessionIdentityDigest, seeded);
		store.transition((seeded.expectedLaunch as { parentSessionIdentityDigest: string }).parentSessionIdentityDigest, "pi-signal", operationId(8), digest, "prepared");
		store.transition((seeded.expectedLaunch as { parentSessionIdentityDigest: string }).parentSessionIdentityDigest, "pi-signal", operationId(8), digest, "dispatching", {
			runId: "seed-run", terminalAsyncDir: path.join(temporary, "seed-async"), canonicalSessionFile: path.join(temporary, "seed-session.jsonl"),
		});
		store.transition((seeded.expectedLaunch as { parentSessionIdentityDigest: string }).parentSessionIdentityDigest, "pi-signal", operationId(8), digest, "uncertain");
		store.close();
	}
	const calls = { value: 0 };
	const spawn = request("live-run");
	const provider = new ManagedDispatchProvider({
		events: bus,
		executor: executor("live-run", spawn, calls),
		getContext: () => ctx,
		getSessionGeneration: () => generation,
		journalRoot,
		hostIdPath,
		resolveLaunch: async () => resolved("live-run", spawn),
	});
	await provider.bindSession(ctx, generation);
	return { bus, provider, calls, spawn, journalRoot, hostIdPath, ctx };
}

describe("managed dispatch provider", () => {
	it("advertises only recovery-complete spawn/status/details capabilities", async () => {
		const { bus, provider } = await readyProvider();
		const reply = await bus.request({ version: 1, requestId: "cap-1", method: "capabilities" }) as any;
		assert.equal(reply.success, true);
		assert.equal(reply.data.state, "ready");
		assert.deepEqual(reply.data.methods, { preflight: true, spawn: true, status: true, details: true, resume: false, steer: false, interrupt: false, stop: false, retire: false });
		assert.equal(reply.data.lifecycle.managedTerminalCorrelation, true);
		const invalid = await bus.request({ version: 1, requestId: "cap-invalid", method: "capabilities", extra: true }) as any;
		assert.equal(invalid.success, false);
		assert.equal(invalid.error.code, "invalid_request");
		provider.dispose();
	});

	it("recovers bounded journal state and resolves operation or exact run without private material", async () => {
		const { bus, provider } = await readyProvider({ seed: true });
		const status = await bus.request({ version: 1, requestId: "status-1", method: "status", target: { consumerId: "pi-signal", operationId: operationId(8) } }) as any;
		assert.equal(status.success, true);
		assert.equal(status.data.state, "uncertain");
		const details = await bus.request({ version: 1, requestId: "details-1", method: "details", target: { consumerId: "pi-signal", runId: "seed-run" } }) as any;
		assert.equal(details.success, true);
		assert.equal(details.data.runId, "seed-run");
		const serialized = JSON.stringify(details);
		assert.doesNotMatch(serialized, /seed-async|seed-session\.jsonl|runnerAdmission|token|input|task/);
		const prefix = await bus.request({ version: 1, requestId: "status-prefix", method: "status", target: { consumerId: "pi-signal", runId: "seed" } }) as any;
		assert.equal(prefix.success, false);
		assert.equal(prefix.error.code, "not_found");
		provider.dispose();
	});

	it("routes spawn once and projects admitted runs without liveness proof as unknown", async () => {
		const { bus, provider, calls, spawn, journalRoot, hostIdPath, ctx } = await readyProvider();
		const reply = await bus.request(spawn) as any;
		assert.equal(reply.success, true);
		assert.equal(reply.data.state, "accepted");
		assert.equal(calls.value, 1);
		const status = await bus.request({
			version: 1,
			requestId: "accepted-status",
			method: "status",
			target: { consumerId: "pi-signal", operationId: operationId() },
		}) as any;
		assert.equal(status.success, true);
		assert.equal(status.data.state, "accepted");
		assert.equal(status.data.runOutcome, "unknown", "committed admission alone is not current liveness proof");
		bus.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, spawn);
		await Promise.resolve();
		assert.equal(calls.value, 1, "same transport request must not execute twice");
		provider.dispose();

		const restartedBus = new Bus();
		const restarted = new ManagedDispatchProvider({
			events: restartedBus,
			executor: { executePreparedSpawn: async () => ({ content: [] }) } as ManagedSpawnExecutor,
			getContext: () => ctx,
			getSessionGeneration: () => generation,
			journalRoot,
			hostIdPath,
			resolveLaunch: async () => resolved("live-run", spawn),
		});
		await restarted.bindSession(ctx, generation);
		const recovered = await restartedBus.request({
			version: 1,
			requestId: "recovered-accepted-status",
			method: "status",
			target: { consumerId: "pi-signal", operationId: operationId() },
		}) as any;
		assert.equal(recovered.success, true);
		assert.equal(recovered.data.state, "accepted");
		assert.equal(recovered.data.runOutcome, "unknown", "recovered admission cannot substitute for current liveness proof");
		assert.equal(calls.value, 1, "recovery must not launch another runner");
		restarted.dispose();
	});

	it("keeps capability unavailable when recovery finds a legacy duplicate run binding", async () => {
		const bus = new Bus();
		const root = path.join(temporary, "journal-duplicate-recovery");
		const ctx = context();
		const parentDigest = computeParentSessionIdentityDigest("parent-session", path.join(temporary, "parent.jsonl"));
		const firstOperationId = operationId(8);
		const secondOperationId = operationId(9);
		const first = request("duplicate-run", firstOperationId);
		const second = request("duplicate-run", secondOperationId);
		const store = new ManagedOperationJournal({ root });
		for (const [payload, id] of [[first, firstOperationId], [second, secondOperationId]] as const) {
			const digest = computeManagedRequestDigest(payload);
			store.claim(parentDigest, payload);
			store.transition(parentDigest, "pi-signal", id, digest, "prepared");
		}
		const firstDigest = computeManagedRequestDigest(first);
		store.transition(parentDigest, "pi-signal", firstOperationId, firstDigest, "dispatching", {
			runId: "duplicate-run",
			terminalAsyncDir: path.join(temporary, "duplicate-async-first"),
			canonicalSessionFile: path.join(temporary, "duplicate-session-first.jsonl"),
		});
		store.transition(parentDigest, "pi-signal", firstOperationId, firstDigest, "uncertain");
		store.close();

		// Simulate a pre-fix durable record. Current writes cannot create this duplicate.
		const secondRecordPath = path.join(root, "operations", parentDigest, "pi-signal", secondOperationId, "record.json");
		const secondRecord = JSON.parse(fs.readFileSync(secondRecordPath, "utf8")) as Record<string, unknown>;
		fs.writeFileSync(secondRecordPath, `${JSON.stringify({
			...secondRecord,
			state: "uncertain",
			runId: "duplicate-run",
			terminalAsyncDir: path.join(temporary, "duplicate-async-second"),
			canonicalSessionFile: path.join(temporary, "duplicate-session-second.jsonl"),
		})}\n`, { mode: 0o600 });

		const provider = new ManagedDispatchProvider({
			events: bus,
			executor: { executePreparedSpawn: async () => ({ content: [] }) } as ManagedSpawnExecutor,
			getContext: () => ctx,
			getSessionGeneration: () => generation,
			journalRoot: root,
			hostIdPath: path.join(temporary, "duplicate-host-id"),
		});
		await provider.bindSession(ctx, generation);
		assert.equal(provider.capabilities().state, "unavailable");
		assert.equal(provider.capabilities().available, false);
		provider.dispose();
	});

	it("stays unavailable on journal ownership conflict and session-generation races", async () => {
		const bus = new Bus();
		const root = path.join(temporary, "journal-busy");
		const owner = new ManagedOperationJournal({ root });
		const ctx = context();
		const provider = new ManagedDispatchProvider({ events: bus, executor: { executePreparedSpawn: async () => ({ content: [] }) } as ManagedSpawnExecutor, getContext: () => ctx, getSessionGeneration: () => generation, journalRoot: root, hostIdPath: path.join(temporary, "host-id") });
		await provider.bindSession(ctx, generation);
		assert.equal(provider.capabilities().state, "unavailable");
		owner.close();
		generation = 2;
		const binding = provider.bindSession(ctx, generation);
		generation = 3;
		await binding;
		assert.equal(provider.capabilities().state, "unavailable");
		provider.dispose();
	});
});

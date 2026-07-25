import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT,
	managedDispatchReplyEvent,
	type ManagedPreflightResultV1,
} from "../../src/api/managed-dispatch.ts";
import type { SubagentLaunchContract, SubagentLaunchContractInput } from "../../src/api/preflight.ts";
import { ASYNC_DIR, RESULTS_DIR, getAsyncConfigPath } from "../../src/shared/types.ts";
import { preparedResultReservationPath } from "../../src/runs/background/prepared-result-reservation.ts";
import {
	loadOrCreateManagedDispatchHostId,
	registerManagedDispatchPreflightBridge,
} from "../../src/extension/managed-dispatch-preflight.ts";

class TestEvents {
	readonly handlers = new Map<string, Set<(data: unknown) => void>>();

	on(event: string, handler: (data: unknown) => void): () => void {
		const handlers = this.handlers.get(event) ?? new Set();
		handlers.add(handler);
		this.handlers.set(event, handlers);
		return () => handlers.delete(handler);
	}

	emit(event: string, data: unknown): void {
		for (const handler of this.handlers.get(event) ?? []) handler(data);
	}
}

let temporary = "";

beforeEach(() => {
	temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-preflight-"));
});

afterEach(() => {
	fs.rmSync(temporary, { recursive: true, force: true });
});

function context(active = true): ExtensionContext {
	return {
		cwd: temporary,
		model: { provider: "test", id: "parent" },
		modelRegistry: { getAvailable: () => [] },
		sessionManager: {
			getSessionId: () => active ? "parent-session" : null,
			getSessionFile: () => active ? path.join(temporary, "parent.jsonl") : null,
		},
	} as unknown as ExtensionContext;
}

function executorRequest(): Record<string, unknown> {
	return {
		agent: "worker",
		task: "inspect",
		context: "fresh",
		async: true,
		clarify: false,
		cwd: temporary,
		sessionDir: path.join(temporary, "child-session"),
		artifacts: false,
		output: false,
	};
}

function request(requestId = "request-1"): Record<string, unknown> {
	return {
		version: 1,
		requestId,
		method: "preflight",
		consumerId: "pi-signal",
		input: { kind: "spawn", request: executorRequest() },
	};
}

function contract(input: SubagentLaunchContractInput): SubagentLaunchContract {
	const sessionRoot = input.sessionDir!;
	const sessionDir = path.join(sessionRoot, "run-0");
	const sessionFile = path.join(sessionDir, "session.jsonl");
	const asyncDir = path.join(ASYNC_DIR, input.runId!);
	const resultPath = path.join(RESULTS_DIR, `${input.runId!}.json`);
	const resultReservationPath = preparedResultReservationPath(resultPath);
	const runnerConfigPath = getAsyncConfigPath(input.runId!);
	const attestation = (attestedPath: string) => ({
		path: attestedPath,
		existingAncestor: temporary,
		existingAncestorRealPath: fs.realpathSync(temporary),
		existingAncestorDevice: "1",
		existingAncestorInode: "2",
		relativeSuffix: path.relative(temporary, attestedPath),
	});
	return {
		version: 1,
		runId: input.runId!,
		parentSessionIdentityDigest: "c".repeat(64),
		agent: {
			name: "worker",
			source: "project",
			filePath: path.join(temporary, ".pi", "agents", "worker.md"),
			definitionDigest: "a".repeat(64),
			shadowedCandidates: [],
		},
		context: "fresh",
		model: "test/worker",
		modelCandidates: ["test/worker"],
		systemPromptMode: "replace",
		inheritProjectContext: false,
		inheritSkills: true,
		skills: { requested: [], resolved: [], missing: [] },
		tools: {
			requestedBuiltin: ["read"],
			declaredBuiltin: ["read"],
			effectiveAllowlist: ["read"],
			explicitAllowlist: true,
			requiredChildTools: [],
			internalTools: [],
			mcp: [],
			effectiveMcpTools: [],
			toolExtensionPaths: [],
			runtimeExtensions: [],
			configuredExtensions: [],
			extensionArgs: [],
			disableAmbientExtensions: true,
			fanoutAuthorized: false,
		},
		roots: {
			cwd: temporary,
			sessionRoot,
			sessionDir,
			sessionFile,
			asyncDir,
			resultPath,
			resultReservationPath,
			runnerConfigPath,
			attestations: {
				cwd: attestation(temporary),
				sessionRoot: attestation(sessionRoot),
				sessionDir: attestation(sessionDir),
				sessionFile: attestation(sessionFile),
				asyncDir: attestation(asyncDir),
				resultPath: attestation(resultPath),
				resultReservationPath: attestation(resultReservationPath),
				runnerConfigPath: attestation(runnerConfigPath),
			},
		},
		protocol: { lifecycleArtifactVersion: 3, packageVersion: "0.11.4" },
		diagnostics: [],
		digest: "b".repeat(64),
	};
}

function nextReply(events: TestEvents, requestId: string): Promise<ManagedPreflightResultV1> {
	return new Promise((resolve) => {
		const dispose = events.on(managedDispatchReplyEvent(requestId), (payload) => {
			dispose();
			resolve(payload as ManagedPreflightResultV1);
		});
	});
}

describe("managed dispatch preflight bridge", () => {
	it("derives host-owned profile identity from an exact non-launching contract", async () => {
		const events = new TestEvents();
		const activeContext = context();
		let observed: SubagentLaunchContractInput | undefined;
		const dispose = registerManagedDispatchPreflightBridge({
			events,
			getContext: () => activeContext,
			hostIdPath: path.join(temporary, "host-id"),
			createRunId: () => "candidate-1",
			resolveCapabilityCeiling: () => undefined,
			resolveContract: async (input) => {
				observed = input;
				return { ok: true, contract: contract(input) };
			},
		});
		const reply = nextReply(events, "request-1");
		events.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, request());
		const result = await reply;
		assert.equal(result.ok, true);
		assert.equal(result.ok && result.candidateRunId, "candidate-1");
		assert.equal(result.ok && result.contractDigest, "b".repeat(64));
		assert.equal(result.ok && result.parentSessionIdentityDigest, "c".repeat(64));
		assert.match(result.ok ? result.profileIdentityDigest : "", /^[a-f0-9]{64}$/);
		assert.equal(result.ok && result.profile.root.realPath, fs.realpathSync(temporary));
		assert.equal(observed?.sessionDir, path.join(temporary, "child-session"));
		assert.equal(observed?.parentSessionId, "parent-session");
		assert.equal(observed?.runId, "candidate-1");
		assert.equal(fs.existsSync(path.join(temporary, "child-session")), false);
		dispose();
		assert.equal(events.handlers.get(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT)?.size, 0);
	});

	it("fails closed with fixed diagnostics for invalid, inactive, and unsupported requests", async () => {
		const events = new TestEvents();
		registerManagedDispatchPreflightBridge({
			events,
			getContext: () => context(false),
			hostIdPath: path.join(temporary, "host-id"),
		});
		for (const [requestId, payload, code] of [
			["bad-1", { ...request("bad-1"), extra: true }, "invalid_request"],
			["inactive-1", request("inactive-1"), "no_active_session"],
		] as const) {
			const reply = nextReply(events, requestId);
			events.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, payload);
			const result = await reply;
			assert.equal(result.ok, false);
			assert.equal(!result.ok && result.code, code);
		}

		const activeEvents = new TestEvents();
		registerManagedDispatchPreflightBridge({ events: activeEvents, getContext: () => context() });
		const unsupportedReply = nextReply(activeEvents, "resume-1");
		activeEvents.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, {
			...request("resume-1"),
			input: { kind: "resume", sourceRunId: "source-1", index: 0, request: { action: "resume", id: "source-1", index: 0 } },
		});
		const unsupported = await unsupportedReply;
		assert.equal(unsupported.ok, false);
		assert.equal(!unsupported.ok && unsupported.code, "unsupported_method");
	});

	it("fences an asynchronous result to the snapshotted parent session generation", async () => {
		const events = new TestEvents();
		const activeContext = context();
		let generation = 1;
		let finish: ((value: { ok: true; contract: SubagentLaunchContract }) => void) | undefined;
		registerManagedDispatchPreflightBridge({
			events,
			getContext: () => activeContext,
			getSessionGeneration: () => generation,
			hostIdPath: path.join(temporary, "host-id"),
			createRunId: () => "candidate-1",
			resolveCapabilityCeiling: () => undefined,
			resolveContract: (input) => new Promise((resolve) => {
				finish = resolve;
				assert.equal(input.parentSessionId, "parent-session");
			}),
		});
		const reply = nextReply(events, "race-1");
		events.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, request("race-1"));
		generation++;
		assert.ok(finish);
		finish({ ok: true, contract: contract({ runId: "candidate-1", sessionDir: path.join(temporary, "child-session") } as SubagentLaunchContractInput) });
		const result = await reply;
		assert.equal(result.ok, false);
		assert.equal(!result.ok && result.code, "no_active_session");
	});

	it("suppresses stale asynchronous replies after disposal", async () => {
		const events = new TestEvents();
		const activeContext = context();
		let finish: ((value: { ok: true; contract: SubagentLaunchContract }) => void) | undefined;
		let replies = 0;
		events.on(managedDispatchReplyEvent("disposed-1"), () => replies++);
		const dispose = registerManagedDispatchPreflightBridge({
			events,
			getContext: () => activeContext,
			resolveCapabilityCeiling: () => undefined,
			resolveContract: () => new Promise((resolve) => { finish = resolve; }),
		});
		events.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, request("disposed-1"));
		dispose();
		assert.ok(finish);
		finish({ ok: true, contract: contract({ runId: "candidate-1", sessionDir: path.join(temporary, "child-session") } as SubagentLaunchContractInput) });
		await new Promise((resolve) => setImmediate(resolve));
		assert.equal(replies, 0);
	});

	it("ignores mutation methods and creates a stable host identity", () => {
		const events = new TestEvents();
		let replies = 0;
		events.on(managedDispatchReplyEvent("mutation-1"), () => replies++);
		registerManagedDispatchPreflightBridge({ events, getContext: () => context() });
		events.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, {
			version: 1,
			requestId: "mutation-1",
			method: "spawn",
		});
		assert.equal(replies, 0);
		const hostPath = path.join(temporary, "managed", "host-id");
		const first = loadOrCreateManagedDispatchHostId(hostPath);
		assert.equal(loadOrCreateManagedDispatchHostId(hostPath), first);
		assert.match(first, /^[a-f0-9-]{36}$/);
	});
});

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
import type { ManagedResumeExecutor } from "../../src/managed/resume-coordinator.ts";
import type { ResolvedManagedResumeLaunchV1 } from "../../src/managed/resume-contract.ts";
import type { ManagedResumeSourceV1 } from "../../src/managed/resume-source.ts";
import { computeManagedProcessTerminalProofDigest } from "../../src/runs/background/process-terminal.ts";
import { preparedResultReservationPath } from "../../src/runs/background/prepared-result-reservation.ts";
import { canonicalSessionId } from "../../src/runs/shared/session-lease.ts";
import { getAsyncConfigPath, type ProcessTerminalV1 } from "../../src/shared/types.ts";
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

function executor(runId: string, payload: Record<string, unknown>, calls: { value: number }): ManagedSpawnExecutor & ManagedResumeExecutor {
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
		executePreparedResume: (async () => ({ content: [], isError: true, details: { mode: "single", results: [] } })) as ManagedResumeExecutor["executePreparedResume"],
	};
}

function inertExecutor(): ManagedSpawnExecutor & ManagedResumeExecutor {
	return {
		executePreparedSpawn: async () => ({ content: [] }),
		executePreparedResume: async () => ({ content: [] }),
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
		createRunId: () => "provider-spawn-preflight-candidate",
		resolveContract: async (input) => {
			const contract = resolved(input.runId!, request(input.runId!)).contract;
			const real = fs.realpathSync(temporary);
			const stats = fs.statSync(real, { bigint: true });
			contract.roots.attestations = Object.fromEntries(Object.entries(contract.roots)
				.filter(([name, value]) => name !== "attestations" && name !== "artifactPaths" && typeof value === "string")
				.map(([name, value]) => [name, { path: value as string, existingAncestor: temporary, existingAncestorRealPath: real, projectedRealPath: path.resolve(real, path.relative(temporary, value as string)), existingAncestorDevice: String(stats.dev), existingAncestorInode: String(stats.ino), relativeSuffix: path.relative(temporary, value as string) }]));
			return { ok: true, contract };
		},
	});
	await provider.bindSession(ctx, generation);
	return { bus, provider, calls, spawn, journalRoot, hostIdPath, ctx };
}

const resumeSourceOperationId = operationId(30);
const resumeOperationId = operationId(31);

function seedTerminalResumeSource(store: ManagedOperationJournal): { runId: string; sessionFile: string } {
	const parentDigest = computeParentSessionIdentityDigest("parent-session", path.join(temporary, "parent.jsonl"));
	const runId = "provider-resume-source";
	const asyncDir = path.join(temporary, "provider-source-async");
	const sessionFile = path.join(temporary, "provider-source-session", "run-0", "session.jsonl");
	fs.mkdirSync(asyncDir, { recursive: true });
	fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
	fs.writeFileSync(sessionFile, "{}\n");
	const sourceRequest = request(runId, resumeSourceOperationId);
	const digest = computeManagedRequestDigest(sourceRequest);
	store.claim(parentDigest, sourceRequest);
	store.transition(parentDigest, "pi-signal", resumeSourceOperationId, digest, "prepared");
	store.transition(parentDigest, "pi-signal", resumeSourceOperationId, digest, "dispatching", { runId, terminalAsyncDir: asyncDir, canonicalSessionFile: sessionFile });
	const admission = createPreparedRunnerAdmission(runId, digest);
	const admissionTokenDigest = computePreparedRunnerAdmissionTokenDigest(admission.token);
	store.transition(parentDigest, "pi-signal", resumeSourceOperationId, digest, "runner-ready", { runnerProcessInstanceId: "provider-source-runner", runnerAdmissionTokenDigest: admissionTokenDigest });
	store.transition(parentDigest, "pi-signal", resumeSourceOperationId, digest, "accepted");
	writePreparedRunnerAdmissionEvidence(path.join(asyncDir, "runner-admission.json"), admission, "committed", 100, "provider-source-runner", 10);
	const stats = fs.statSync(sessionFile, { bigint: true });
	const sessionDevice = String(stats.dev);
	const sessionInode = String(stats.ino);
	const proof: ProcessTerminalV1 = {
		version: 1,
		state: "observed",
		runId,
		runnerProcessInstanceId: "provider-source-runner",
		observedAt: 20,
		instances: [{ kind: "runner", processInstanceId: "provider-source-runner", closeObservedAt: 20, exitCode: 0, signal: null }],
		managed: { version: 1, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", operationId: resumeSourceOperationId, requestDigest: digest, candidateRunId: runId, runnerAdmissionTokenDigest: admissionTokenDigest },
		canonicalSession: { canonicalSessionId: canonicalSessionId(sessionFile), sessionDevice, sessionInode, leaseDisposition: "not-held", freeAtObservation: true },
		resumeDisposition: "resumable",
	};
	fs.writeFileSync(path.join(asyncDir, "process-terminal.json"), JSON.stringify(proof));
	store.transition(parentDigest, "pi-signal", resumeSourceOperationId, digest, "terminal", {
		terminalEvidence: { version: 1, proofDigest: computeManagedProcessTerminalProofDigest(proof), observedAt: 20, canonicalSessionId: canonicalSessionId(sessionFile), sessionDevice, sessionInode },
	});
	fs.writeFileSync(path.join(asyncDir, "recovery-descriptor.json"), JSON.stringify({ version: 1, sourceRunId: runId, agent: "worker", sessionFile, cwd: temporary, systemPromptMode: "replace", inheritProjectContext: false, inheritSkills: false, outputMode: "inline", maxSubagentDepth: 1, share: false }));
	return { runId, sessionFile };
}

function resolvedResume(runId: string, source: Readonly<ManagedResumeSourceV1>): ResolvedManagedResumeLaunchV1 {
	const parentDigest = computeParentSessionIdentityDigest("parent-session", path.join(temporary, "parent.jsonl"));
	const asyncDir = path.join(temporary, "provider-resume-async", runId);
	const resultPath = path.join(temporary, "provider-resume-results", `${runId}.json`);
	const launchContract: SubagentLaunchContract = {
		version: 1,
		runId,
		parentSessionIdentityDigest: parentDigest,
		agent: { name: "worker", source: "project", filePath: path.join(temporary, "worker.md"), definitionDigest: "7".repeat(64), shadowedCandidates: [] },
		context: "fresh",
		modelCandidates: [],
		systemPromptMode: "replace",
		inheritProjectContext: false,
		inheritSkills: false,
		skills: { requested: [], resolved: [], missing: [] },
		tools: { requestedBuiltin: [], declaredBuiltin: [], effectiveAllowlist: [], explicitAllowlist: true, requiredChildTools: [], internalTools: [], mcp: [], effectiveMcpTools: [], toolExtensionPaths: [], runtimeExtensions: [], configuredExtensions: [], extensionArgs: [], disableAmbientExtensions: true, fanoutAuthorized: false },
		roots: {
			cwd: temporary,
			sessionRoot: path.dirname(path.dirname(source.canonicalSessionFile)),
			sessionDir: path.dirname(source.canonicalSessionFile),
			sessionFile: source.canonicalSessionFile,
			asyncDir,
			resultPath,
			resultReservationPath: preparedResultReservationPath(resultPath),
			runnerConfigPath: getAsyncConfigPath(runId),
			runnerAdmissionPath: path.join(asyncDir, "runner-admission.json"),
			runnerAdmissionProceedPath: path.join(asyncDir, "runner-admission-proceed.json"),
			runnerAdmissionCommitPath: path.join(asyncDir, "runner-admission-commit.json"),
			attestations: {},
		},
		protocol: { lifecycleArtifactVersion: 3, packageVersion: "test" },
		diagnostics: [],
		digest: "8".repeat(64),
	};
	return {
		request: { action: "resume", runId: source.sourceRunId, index: 0, message: "continue", async: true, clarify: false, context: "fresh" },
		source,
		execution: { agentConfig: { name: "worker", description: "recovered", systemPrompt: "", systemPromptMode: "replace", inheritProjectContext: false, inheritSkills: false, source: "project", filePath: path.join(temporary, "worker.md") }, artifactConfig: { enabled: false, includeInput: false, includeOutput: false, includeJsonl: false, includeMetadata: false, cleanupDays: 0 } },
		contract: { version: 1, runId, parentSessionIdentityDigest: parentDigest, source: { operationId: source.sourceOperationId, requestDigest: source.sourceRequestDigest, runId: source.sourceRunId, index: 0, terminalProofDigest: source.sourceTerminalProofDigest, canonicalSessionId: source.canonicalSessionId, canonicalSessionFile: source.canonicalSessionFile, sessionDevice: source.sessionDevice, sessionInode: source.sessionInode, recoveryDescriptorDigest: source.recoveryDescriptorDigest }, launchContract, digest: "9".repeat(64) },
		profile: { version: 1, contentDigest: "6".repeat(64), root: { version: 1, realPath: temporary } },
		profileIdentityDigest: "5".repeat(64),
	};
}

function successfulResumeExecutor(calls: { value: number }, terminal = true): ManagedSpawnExecutor & ManagedResumeExecutor {
	return {
		executePreparedSpawn: async () => ({ content: [] }),
		executePreparedResume: (async (_id, _params, _signal, _update, _ctx, options) => {
			calls.value++;
			const runId = options.runId;
			const resolved = resolvedResume(runId, options.source);
			const roots = resolved.contract.launchContract.roots;
			const plan = { runId, sourceRunId: options.source.sourceRunId, sourceIndex: 0 as const, sourceOperationId: options.source.sourceOperationId, sourceSessionFile: options.source.canonicalSessionFile, sourceCanonicalSessionId: options.source.canonicalSessionId, sourceTerminalProofDigest: options.source.sourceTerminalProofDigest, parentSessionId: "parent-session", parentSessionFile: path.join(temporary, "parent.jsonl"), cwd: temporary, asyncDir: roots.asyncDir!, resultPath: roots.resultPath!, resultReservationPath: roots.resultReservationPath!, runnerConfigPath: roots.runnerConfigPath!, runnerAdmissionPath: roots.runnerAdmissionPath!, runnerAdmissionProceedPath: roots.runnerAdmissionProceedPath!, runnerAdmissionCommitPath: roots.runnerAdmissionCommitPath! };
			await options.beforeLaunch(plan);
			options.afterAuthorization(plan);
			fs.mkdirSync(roots.asyncDir!, { recursive: true });
			const admission = createPreparedRunnerAdmission(runId, options.dispatchIdentityDigest, { version: 1, sourceRunId: options.source.sourceRunId, sourceIndex: 0, canonicalSessionId: options.source.canonicalSessionId });
			const leaseDigest = "4".repeat(64);
			const ready = { ...admission, state: "ready" as const, pid: 200, runnerProcessInstanceId: "provider-resume-runner", observedAt: 30, sessionLeaseTokenDigest: leaseDigest };
			options.onRunnerReady(ready);
			options.onRunnerAccepted({ ...ready, state: "accepted", observedAt: 40 });
			writePreparedRunnerAdmissionEvidence(roots.runnerAdmissionPath!, admission, "committed", 200, "provider-resume-runner", 50, leaseDigest);
			if (terminal) {
				const stats = fs.statSync(options.source.canonicalSessionFile, { bigint: true });
				const proof: ProcessTerminalV1 = { version: 1, state: "observed", runId, runnerProcessInstanceId: "provider-resume-runner", observedAt: 60, instances: [{ kind: "runner", processInstanceId: "provider-resume-runner", closeObservedAt: 60, exitCode: 0, signal: null }], managed: { ...options.processTerminalBinding, runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token), sessionLeaseTokenDigest: leaseDigest }, canonicalSession: { canonicalSessionId: options.source.canonicalSessionId, sessionDevice: String(stats.dev), sessionInode: String(stats.ino), leaseDisposition: "released", freeAtObservation: true, canonicalSessionLeaseReleased: true }, resumeDisposition: "resumable" };
				fs.writeFileSync(path.join(roots.asyncDir!, "process-terminal.json"), JSON.stringify(proof));
				options.onProcessTerminal(proof);
			}
			return { content: [{ type: "text", text: "started" }], details: { mode: "single", results: [] } };
		}) as ManagedResumeExecutor["executePreparedResume"],
	};
}

describe("managed dispatch provider", () => {
	it("advertises only recovery-complete spawn/status/details capabilities", async () => {
		const { bus, provider } = await readyProvider();
		const reply = await bus.request({ version: 1, requestId: "cap-1", method: "capabilities" }) as any;
		assert.equal(reply.success, true);
		assert.equal(reply.data.state, "ready");
		assert.deepEqual(reply.data.methods, { preflight: true, spawn: true, status: true, details: true, resume: true, steer: false, interrupt: false, stop: false, retire: false });
		assert.equal(reply.data.lifecycle.managedTerminalCorrelation, true);
		const spawnInput = (request("ignored").input as { request: Record<string, unknown> }).request;
		let preflightReplies = 0;
		const off = bus.on(managedDispatchReplyEvent("provider-spawn-preflight"), () => { preflightReplies++; });
		const preflight = await bus.request({ version: 1, requestId: "provider-spawn-preflight", method: "preflight", consumerId: "pi-signal", input: { kind: "spawn", request: { ...spawnInput, sessionDir: path.join(temporary, "session-provider-spawn-preflight-candidate") } } }) as any;
		off();
		assert.equal(preflight.ok, true);
		assert.equal(preflight.candidateRunId, "provider-spawn-preflight-candidate");
		assert.equal(preflightReplies, 1, "the consolidated provider must be the only preflight responder");
		const invalid = await bus.request({ version: 1, requestId: "cap-invalid", method: "capabilities", extra: true }) as any;
		assert.equal(invalid.success, false);
		assert.equal(invalid.error.code, "invalid_request");
		provider.dispose();
	});

	it("routes exact resume preflight, mutation, terminal status, and replay once", async () => {
		const bus = new Bus();
		const journalRoot = path.join(temporary, "journal-resume-provider");
		const hostIdPath = path.join(temporary, "resume-host-id");
		fs.writeFileSync(hostIdPath, "host-1\n", { mode: 0o600 });
		const store = new ManagedOperationJournal({ root: journalRoot });
		const source = seedTerminalResumeSource(store);
		store.close();
		const calls = { value: 0 };
		const ctx = context();
		const candidateRunId = "provider-resume-candidate";
		const provider = new ManagedDispatchProvider({
			events: bus,
			executor: successfulResumeExecutor(calls),
			getContext: () => ctx,
			getSessionGeneration: () => generation,
			journalRoot,
			hostIdPath,
			createRunId: () => candidateRunId,
			resolveResumeLaunch: async (_request, runId, resolvedSource) => resolvedResume(runId, resolvedSource),
		});
		await provider.bindSession(ctx, generation);
		assert.equal(provider.capabilities().methods.resume, true);
		const executorRequest = { action: "resume", runId: source.runId, index: 0, message: "continue", async: true, clarify: false, context: "fresh" };
		const preflight = await bus.request({ version: 1, requestId: "resume-preflight", method: "preflight", consumerId: "pi-signal", input: { kind: "resume", sourceRunId: source.runId, index: 0, request: executorRequest } }) as any;
		assert.equal(preflight.ok, true);
		assert.equal(preflight.candidateRunId, candidateRunId);
		assert.equal(preflight.parentSessionIdentityDigest, computeParentSessionIdentityDigest("parent-session", path.join(temporary, "parent.jsonl")));
		const resume = {
			version: 1,
			requestId: "resume-mutation",
			method: "resume",
			managed: { version: 1, consumerId: "pi-signal", operationId: resumeOperationId },
			expectedLaunch: { version: 1, hostId: preflight.host.hostId, candidateRunId, profileIdentityDigest: preflight.profileIdentityDigest, parentSessionIdentityDigest: preflight.parentSessionIdentityDigest, contractDigest: preflight.contractDigest },
			input: { sourceRunId: source.runId, index: 0, request: executorRequest },
		};
		let replies = 0;
		const off = bus.on(managedDispatchReplyEvent("resume-mutation"), () => { replies++; });
		const receipt = await bus.request(resume) as any;
		off();
		assert.equal(receipt.success, true);
		assert.equal(receipt.data.state, "terminal");
		assert.equal(receipt.data.sourceRunId, source.runId);
		assert.equal(calls.value, 1);
		assert.equal(replies, 1, "one transport request must receive exactly one reply");
		const status = await bus.request({ version: 1, requestId: "resume-status", method: "status", target: { consumerId: "pi-signal", operationId: resumeOperationId } }) as any;
		assert.equal(status.success, true);
		assert.equal(status.data.state, "terminal");
		assert.equal(status.data.runId, candidateRunId);
		assert.equal(status.data.sourceRunId, source.runId);
		assert.equal(status.data.child.canonicalSessionId, canonicalSessionId(source.sessionFile));
		assert.equal(status.data.child.resumeDisposition, "resumable");
		const details = await bus.request({ version: 1, requestId: "resume-details", method: "details", target: { consumerId: "pi-signal", runId: candidateRunId } }) as any;
		assert.equal(details.success, true);
		assert.equal(details.data.contractDigest, "9".repeat(64));
		const replay = await bus.request({ ...resume, requestId: "resume-replay" }) as any;
		assert.equal(replay.success, true);
		assert.equal(replay.data.state, "terminal");
		assert.equal(replay.data.replayed, true);
		assert.equal(calls.value, 1, "semantic replay must not launch again");
		provider.dispose();

		const restartedBus = new Bus();
		const restarted = new ManagedDispatchProvider({ events: restartedBus, executor: inertExecutor(), getContext: () => ctx, getSessionGeneration: () => generation, journalRoot, hostIdPath, resolveResumeLaunch: async (_request, runId, resolvedSource) => resolvedResume(runId, resolvedSource) });
		await restarted.bindSession(ctx, generation);
		assert.equal(restarted.capabilities().methods.resume, true);
		const recovered = await restartedBus.request({ version: 1, requestId: "resume-recovered", method: "status", target: { consumerId: "pi-signal", operationId: resumeOperationId } }) as any;
		assert.equal(recovered.success, true);
		assert.equal(recovered.data.state, "terminal");
		assert.equal(calls.value, 1);
		restarted.dispose();
	});

	it("classifies a recovered accepted resume as observer-loss uncertainty without relaunch", async () => {
		const bus = new Bus();
		const journalRoot = path.join(temporary, "journal-resume-observer-loss");
		const hostIdPath = path.join(temporary, "resume-observer-host-id");
		fs.writeFileSync(hostIdPath, "host-1\n", { mode: 0o600 });
		const store = new ManagedOperationJournal({ root: journalRoot });
		const source = seedTerminalResumeSource(store);
		store.close();
		const calls = { value: 0 };
		const ctx = context();
		const candidateRunId = "provider-resume-observer-candidate";
		const provider = new ManagedDispatchProvider({ events: bus, executor: successfulResumeExecutor(calls, false), getContext: () => ctx, getSessionGeneration: () => generation, journalRoot, hostIdPath, createRunId: () => candidateRunId, resolveResumeLaunch: async (_request, runId, resolvedSource) => resolvedResume(runId, resolvedSource) });
		await provider.bindSession(ctx, generation);
		const executorRequest = { action: "resume", runId: source.runId, index: 0, message: "continue", async: true, clarify: false, context: "fresh" };
		const preflight = await bus.request({ version: 1, requestId: "observer-preflight", method: "preflight", consumerId: "pi-signal", input: { kind: "resume", sourceRunId: source.runId, index: 0, request: executorRequest } }) as any;
		assert.equal(preflight.ok, true);
		const mutation = { version: 1, requestId: "observer-resume", method: "resume", managed: { version: 1, consumerId: "pi-signal", operationId: resumeOperationId }, expectedLaunch: { version: 1, hostId: preflight.host.hostId, candidateRunId, profileIdentityDigest: preflight.profileIdentityDigest, parentSessionIdentityDigest: preflight.parentSessionIdentityDigest, contractDigest: preflight.contractDigest }, input: { sourceRunId: source.runId, index: 0, request: executorRequest } };
		const accepted = await bus.request(mutation) as any;
		assert.equal(accepted.success, true);
		assert.equal(accepted.data.state, "accepted");
		assert.equal(calls.value, 1);
		provider.dispose();

		const restartedBus = new Bus();
		const restarted = new ManagedDispatchProvider({ events: restartedBus, executor: inertExecutor(), getContext: () => ctx, getSessionGeneration: () => generation, journalRoot, hostIdPath, resolveResumeLaunch: async (_request, runId, resolvedSource) => resolvedResume(runId, resolvedSource) });
		await restarted.bindSession(ctx, generation);
		const recovered = await restartedBus.request({ version: 1, requestId: "observer-status", method: "status", target: { consumerId: "pi-signal", operationId: resumeOperationId } }) as any;
		assert.equal(recovered.success, true);
		assert.equal(recovered.data.state, "uncertain");
		assert.equal(recovered.data.runOutcome, "unknown");
		assert.equal(calls.value, 1, "startup recovery must never relaunch an accepted resume");
		restarted.dispose();
	});

	it("suppresses stale resume preflight replies across a session-generation reset", async () => {
		const bus = new Bus();
		const journalRoot = path.join(temporary, "journal-resume-race");
		const hostIdPath = path.join(temporary, "resume-race-host-id");
		fs.writeFileSync(hostIdPath, "host-1\n", { mode: 0o600 });
		const store = new ManagedOperationJournal({ root: journalRoot });
		const source = seedTerminalResumeSource(store);
		store.close();
		let finish: ((value: ResolvedManagedResumeLaunchV1) => void) | undefined;
		let pendingSource: Readonly<ManagedResumeSourceV1> | undefined;
		const gate = new Promise<ResolvedManagedResumeLaunchV1>((resolve) => { finish = resolve; });
		const ctx = context();
		const provider = new ManagedDispatchProvider({ events: bus, executor: inertExecutor(), getContext: () => ctx, getSessionGeneration: () => generation, journalRoot, hostIdPath, createRunId: () => "resume-race-candidate", resolveResumeLaunch: async (_request, _runId, resolvedSource) => { pendingSource = resolvedSource; return gate; } });
		await provider.bindSession(ctx, generation);
		let replies = 0;
		const off = bus.on(managedDispatchReplyEvent("resume-race-preflight"), () => { replies++; });
		bus.emit(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, { version: 1, requestId: "resume-race-preflight", method: "preflight", consumerId: "pi-signal", input: { kind: "resume", sourceRunId: source.runId, index: 0, request: { action: "resume", runId: source.runId, index: 0, message: "continue", async: true, clarify: false, context: "fresh" } } });
		await Promise.resolve();
		generation++;
		provider.unbindSession();
		assert.ok(pendingSource);
		finish!(resolvedResume("resume-race-candidate", pendingSource));
		await new Promise((resolve) => setImmediate(resolve));
		await new Promise((resolve) => setImmediate(resolve));
		assert.equal(replies, 0, "a preflight from an old provider epoch must not reply");
		off();
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
			executor: inertExecutor(),
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
		assert.equal(recovered.data.state, "uncertain", "startup without the former live close observer must be durable uncertainty");
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
			executor: inertExecutor(),
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
		const provider = new ManagedDispatchProvider({ events: bus, executor: inertExecutor(), getContext: () => ctx, getSessionGeneration: () => generation, journalRoot: root, hostIdPath: path.join(temporary, "host-id") });
		assert.equal(provider.capabilities().methods.resume, false);
		await provider.bindSession(ctx, generation);
		assert.equal(provider.capabilities().state, "unavailable");
		assert.equal(provider.capabilities().methods.resume, false);
		owner.close();
		generation = 2;
		const binding = provider.bindSession(ctx, generation);
		assert.equal(provider.capabilities().state, "recovering");
		assert.equal(provider.capabilities().methods.resume, false);
		generation = 3;
		await binding;
		assert.equal(provider.capabilities().state, "unavailable");
		assert.equal(provider.capabilities().methods.resume, false);
		provider.unbindSession();
		assert.equal(provider.capabilities().methods.resume, false);
		provider.dispose();
		assert.equal(provider.capabilities().methods.resume, false);
	});
});

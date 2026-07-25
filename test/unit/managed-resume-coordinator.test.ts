import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { computeManagedRequestDigest } from "../../src/api/managed-dispatch.ts";
import type { SubagentLaunchContract } from "../../src/api/preflight.ts";
import { ManagedOperationJournal } from "../../src/managed/operation-journal.ts";
import { ManagedResumeCoordinator, type ManagedResumeExecutor } from "../../src/managed/resume-coordinator.ts";
import type { ResolvedManagedResumeLaunchV1 } from "../../src/managed/resume-contract.ts";
import type { ManagedResumeSourceV1 } from "../../src/managed/resume-source.ts";
import {
	computePreparedRunnerAdmissionTokenDigest,
	createPreparedRunnerAdmission,
	preparedRunnerAdmissionPaths,
	writePreparedRunnerAdmissionEvidence,
} from "../../src/runs/background/prepared-runner-admission.ts";
import { computeManagedProcessTerminalProofDigest } from "../../src/runs/background/process-terminal.ts";
import { preparedResultReservationPath } from "../../src/runs/background/prepared-result-reservation.ts";
import { canonicalSessionId } from "../../src/runs/shared/session-lease.ts";
import { ASYNC_DIR, RESULTS_DIR, getAsyncConfigPath, type ProcessTerminalV1 } from "../../src/shared/types.ts";

let temporary = "";
const parentDigest = "a".repeat(64);
const sourceOperationId = Buffer.alloc(32, 21).toString("base64url");
const resumeOperationId = Buffer.alloc(32, 22).toString("base64url");
const profileDigest = "b".repeat(64);
const resumeContractDigest = "c".repeat(64);

beforeEach(() => { temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-resume-coordinator-")); });
afterEach(() => {
	fs.rmSync(temporary, { recursive: true, force: true });
	for (const entry of fs.readdirSync(ASYNC_DIR)) if (entry.startsWith("managed-resume-coordinator-")) fs.rmSync(path.join(ASYNC_DIR, entry), { recursive: true, force: true });
});

function sourceRequest(runId: string) {
	return {
		version: 1, requestId: "source", method: "spawn",
		managed: { version: 1, consumerId: "pi-signal", operationId: sourceOperationId },
		expectedLaunch: { version: 1, hostId: "host-1", candidateRunId: runId, profileIdentityDigest: "d".repeat(64), parentSessionIdentityDigest: parentDigest, contractDigest: "e".repeat(64) },
		input: { request: { agent: "worker", task: "opaque", context: "fresh", async: true, clarify: false, cwd: temporary, sessionDir: path.join(temporary, "source-session") } },
	};
}

function createTerminalSource(journal: ManagedOperationJournal): { runId: string; sessionFile: string } {
	const runId = "managed-resume-source";
	const asyncDir = path.join(temporary, "source-async");
	const sessionFile = path.join(temporary, "source-session", "run-0", "session.jsonl");
	fs.mkdirSync(asyncDir, { recursive: true });
	fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
	fs.writeFileSync(sessionFile, "{}\n");
	const request = sourceRequest(runId);
	const digest = computeManagedRequestDigest(request);
	journal.claim(parentDigest, request);
	journal.transition(parentDigest, "pi-signal", sourceOperationId, digest, "prepared");
	journal.transition(parentDigest, "pi-signal", sourceOperationId, digest, "dispatching", { runId, terminalAsyncDir: asyncDir, canonicalSessionFile: sessionFile });
	const admission = createPreparedRunnerAdmission(runId, digest);
	journal.transition(parentDigest, "pi-signal", sourceOperationId, digest, "runner-ready", { runnerProcessInstanceId: "source-runner", runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token) });
	journal.transition(parentDigest, "pi-signal", sourceOperationId, digest, "accepted");
	writePreparedRunnerAdmissionEvidence(preparedRunnerAdmissionPaths(asyncDir).evidencePath, admission, "committed", 123, "source-runner", 10);
	const sessionStats = fs.statSync(sessionFile, { bigint: true });
	const sessionDevice = String(sessionStats.dev);
	const sessionInode = String(sessionStats.ino);
	const managed = { version: 1 as const, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", operationId: sourceOperationId, requestDigest: digest, candidateRunId: runId, runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token) };
	const proof: ProcessTerminalV1 = { version: 1, state: "observed", runId, runnerProcessInstanceId: "source-runner", observedAt: 20, instances: [{ kind: "runner", processInstanceId: "source-runner", closeObservedAt: 20, exitCode: 0, signal: null }], managed, canonicalSession: { canonicalSessionId: canonicalSessionId(sessionFile), sessionDevice, sessionInode, leaseDisposition: "not-held", freeAtObservation: true }, resumeDisposition: "resumable" };
	fs.writeFileSync(path.join(asyncDir, "process-terminal.json"), JSON.stringify(proof));
	journal.transition(parentDigest, "pi-signal", sourceOperationId, digest, "terminal", { terminalEvidence: { version: 1, proofDigest: computeManagedProcessTerminalProofDigest(proof), observedAt: 20, canonicalSessionId: canonicalSessionId(sessionFile), sessionDevice, sessionInode } });
	fs.writeFileSync(path.join(asyncDir, "recovery-descriptor.json"), JSON.stringify({ version: 1, sourceRunId: runId, agent: "worker", sessionFile, cwd: temporary, systemPromptMode: "replace", inheritProjectContext: false, inheritSkills: false, outputMode: "inline", maxSubagentDepth: 1, share: false }));
	return { runId, sessionFile };
}

function candidate(suffix: string): string { return `managed-resume-coordinator-${suffix}-${Date.now()}`; }
function resumeRequest(sourceRunId: string, runId: string, requestId = "resume-transport") {
	return {
		version: 1, requestId, method: "resume",
		managed: { version: 1, consumerId: "pi-signal", operationId: resumeOperationId },
		expectedLaunch: { version: 1, hostId: "host-1", candidateRunId: runId, profileIdentityDigest: profileDigest, parentSessionIdentityDigest: parentDigest, contractDigest: resumeContractDigest },
		input: { sourceRunId, index: 0, request: { action: "resume", runId: sourceRunId, index: 0, message: "continue", async: true, clarify: false, context: "fresh" } },
	};
}

function resolved(runId: string, source: Readonly<ManagedResumeSourceV1>): ResolvedManagedResumeLaunchV1 {
	const asyncDir = path.join(ASYNC_DIR, runId);
	const resultPath = path.join(RESULTS_DIR, `${runId}.json`);
	const admission = preparedRunnerAdmissionPaths(asyncDir);
	const launchContract: SubagentLaunchContract = {
		version: 1, runId, parentSessionIdentityDigest: parentDigest,
		agent: { name: "worker", source: "project", filePath: path.join(temporary, "worker.md"), definitionDigest: "f".repeat(64), shadowedCandidates: [] },
		context: "fresh", modelCandidates: [], systemPromptMode: "replace", inheritProjectContext: false, inheritSkills: false,
		skills: { requested: [], resolved: [], missing: [] },
		tools: { requestedBuiltin: [], declaredBuiltin: [], effectiveAllowlist: [], explicitAllowlist: true, requiredChildTools: [], internalTools: [], mcp: [], effectiveMcpTools: [], toolExtensionPaths: [], runtimeExtensions: [], configuredExtensions: [], extensionArgs: [], disableAmbientExtensions: true, fanoutAuthorized: false },
		roots: { cwd: temporary, sessionRoot: path.dirname(path.dirname(source.canonicalSessionFile)), sessionDir: path.dirname(source.canonicalSessionFile), sessionFile: source.canonicalSessionFile, asyncDir, resultPath, resultReservationPath: preparedResultReservationPath(resultPath), runnerConfigPath: getAsyncConfigPath(runId), runnerAdmissionPath: admission.evidencePath, runnerAdmissionProceedPath: admission.proceedPath, runnerAdmissionCommitPath: admission.commitPath, attestations: {} },
		protocol: { lifecycleArtifactVersion: 3, packageVersion: "test" }, diagnostics: [], digest: "1".repeat(64),
	};
	return {
		request: { action: "resume", runId: source.sourceRunId, index: 0, message: "continue", async: true, clarify: false, context: "fresh" },
		source,
		execution: {
			agentConfig: { name: "worker", description: "recovered", systemPrompt: "", systemPromptMode: "replace", inheritProjectContext: false, inheritSkills: false, source: "project", filePath: path.join(temporary, "worker.md") },
			artifactConfig: { enabled: false, includeInput: false, includeOutput: false, includeJsonl: false, includeMetadata: false, cleanupDays: 0 },
		},
		contract: { version: 1, runId, parentSessionIdentityDigest: parentDigest, source: { operationId: source.sourceOperationId, requestDigest: source.sourceRequestDigest, runId: source.sourceRunId, index: 0, terminalProofDigest: source.sourceTerminalProofDigest, canonicalSessionId: source.canonicalSessionId, canonicalSessionFile: source.canonicalSessionFile, sessionDevice: source.sessionDevice, sessionInode: source.sessionInode, recoveryDescriptorDigest: source.recoveryDescriptorDigest }, launchContract, digest: resumeContractDigest },
		profile: { version: 1, contentDigest: "2".repeat(64), root: { version: 1, realPath: temporary } },
		profileIdentityDigest: profileDigest,
	};
}

function context(): ExtensionContext {
	return { cwd: temporary, modelRegistry: { getAvailable: () => [] }, sessionManager: { getSessionId: () => "parent", getSessionFile: () => path.join(temporary, "parent.jsonl") } } as unknown as ExtensionContext;
}

function successfulExecutor(runId: string, calls: { value: number }, terminal = false): ManagedResumeExecutor {
	return { executePreparedResume: (async (_id, _params, _signal, _update, _ctx, options) => {
		calls.value++;
		const source = options.source;
		const asyncDir = path.join(ASYNC_DIR, runId);
		const resultPath = path.join(RESULTS_DIR, `${runId}.json`);
		const admissionPaths = preparedRunnerAdmissionPaths(asyncDir);
		const plan = { runId, sourceRunId: source.sourceRunId, sourceIndex: 0 as const, sourceOperationId: source.sourceOperationId, sourceSessionFile: source.canonicalSessionFile, sourceCanonicalSessionId: source.canonicalSessionId, sourceTerminalProofDigest: source.sourceTerminalProofDigest, parentSessionId: "parent", parentSessionFile: path.join(temporary, "parent.jsonl"), cwd: temporary, asyncDir, resultPath, resultReservationPath: preparedResultReservationPath(resultPath), runnerConfigPath: getAsyncConfigPath(runId), runnerAdmissionPath: admissionPaths.evidencePath, runnerAdmissionProceedPath: admissionPaths.proceedPath, runnerAdmissionCommitPath: admissionPaths.commitPath };
		await options.beforeLaunch(plan); options.afterAuthorization(plan);
		fs.mkdirSync(asyncDir, { recursive: true });
		const admission = createPreparedRunnerAdmission(runId, options.dispatchIdentityDigest, { version: 1, sourceRunId: source.sourceRunId, sourceIndex: 0, canonicalSessionId: source.canonicalSessionId });
		const leaseDigest = "3".repeat(64);
		const ready = { ...admission, state: "ready" as const, pid: 123, runnerProcessInstanceId: "resume-runner", observedAt: 30, sessionLeaseTokenDigest: leaseDigest };
		options.onRunnerReady(ready);
		const accepted = { ...ready, state: "accepted" as const, observedAt: 40 };
		options.onRunnerAccepted(accepted);
		writePreparedRunnerAdmissionEvidence(admissionPaths.evidencePath, admission, "committed", 123, "resume-runner", 50, leaseDigest);
		if (terminal) {
			const managed = { ...options.processTerminalBinding, runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token), sessionLeaseTokenDigest: leaseDigest };
			const sourceStats = fs.statSync(source.canonicalSessionFile, { bigint: true });
			const proof: ProcessTerminalV1 = { version: 1, state: "observed", runId, runnerProcessInstanceId: "resume-runner", observedAt: 60, instances: [{ kind: "runner", processInstanceId: "resume-runner", closeObservedAt: 60, exitCode: 0, signal: null }], managed, canonicalSession: { canonicalSessionId: source.canonicalSessionId, sessionDevice: String(sourceStats.dev), sessionInode: String(sourceStats.ino), leaseDisposition: "released", freeAtObservation: true, canonicalSessionLeaseReleased: true }, resumeDisposition: "resumable" };
			fs.writeFileSync(path.join(asyncDir, "process-terminal.json"), JSON.stringify(proof));
			options.onProcessTerminal(proof);
		}
		return { content: [{ type: "text", text: "started" }], details: { mode: "single", results: [] } };
	}) as ManagedResumeExecutor["executePreparedResume"] };
}

function coordinator(journal: ManagedOperationJournal, executor: ManagedResumeExecutor) {
	const ctx = context();
	return new ManagedResumeCoordinator({ journal, executor, getContext: () => ctx, getSessionGeneration: () => 1, loadHostId: () => "host-1", resolveLaunch: async (_request, runId, source) => resolved(runId, source) });
}

describe("unregistered managed resume coordinator", () => {
	it("joins exact retries, binds lease admission, and never relaunches accepted evidence", async () => {
		const root = path.join(temporary, "journal");
		let journal = new ManagedOperationJournal({ root });
		const source = createTerminalSource(journal);
		const runId = candidate("accepted");
		const calls = { value: 0 };
		const target = coordinator(journal, successfulExecutor(runId, calls));
		const [first, concurrent] = await Promise.all([
			target.dispatchResume(resumeRequest(source.runId, runId)),
			target.dispatchResume(resumeRequest(source.runId, runId, "concurrent")),
		]);
		assert.equal(first.state, "accepted");
		assert.equal(concurrent.state, "accepted");
		assert.equal([first.replayed, concurrent.replayed].filter(Boolean).length, 1);
		const retry = await target.dispatchResume(resumeRequest(source.runId, runId, "retry"));
		assert.equal(retry.state, "accepted");
		assert.equal(retry.replayed, true);
		assert.equal(calls.value, 1);
		const record = journal.read(parentDigest, "pi-signal", resumeOperationId);
		assert.match(record?.runnerSessionLeaseTokenDigest ?? "", /^[a-f0-9]{64}$/);
		assert.equal(record?.runnerCanonicalSessionId, canonicalSessionId(source.sessionFile));
		journal.close();
		journal = new ManagedOperationJournal({ root });
		const reopened = coordinator(journal, { executePreparedResume: (async () => { calls.value++; return { content: [], details: { mode: "single", results: [] } }; }) as ManagedResumeExecutor["executePreparedResume"] });
		assert.equal((await reopened.dispatchResume(resumeRequest(source.runId, runId, "reopened"))).state, "accepted");
		assert.equal(calls.value, 1, "reopened accepted admission must not execute again");
		const admissionPath = preparedRunnerAdmissionPaths(path.join(ASYNC_DIR, runId)).evidencePath;
		const committedAdmission = JSON.parse(fs.readFileSync(admissionPath, "utf8")) as Record<string, unknown>;
		fs.writeFileSync(admissionPath, JSON.stringify({ ...committedAdmission, state: "accepted" }));
		const observerLost = reopened.reconcileAfterObserverLoss(journal.read(parentDigest, "pi-signal", resumeOperationId)!);
		assert.equal(observerLost.state, "uncertain");
		assert.equal(observerLost.observerLost, true, "observer loss must remain durable across a late commit");
		fs.writeFileSync(admissionPath, JSON.stringify(committedAdmission));
		const afterLateCommit = reopened.reconcileExisting(journal.read(parentDigest, "pi-signal", resumeOperationId)!);
		assert.equal(afterLateCommit.state, "uncertain");
		assert.equal(afterLateCommit.observerLost, true);
		assert.equal(calls.value, 1, "observer-loss reconciliation must not execute again");
		fs.writeFileSync(path.join(ASYNC_DIR, runId, "process-terminal.json"), JSON.stringify({ version: 1, state: "unknown", runId, runnerProcessInstanceId: "resume-runner", reason: "observer-unavailable" }));
		assert.equal((await reopened.dispatchResume(resumeRequest(source.runId, runId, "unknown-1"))).state, "uncertain");
		assert.equal((await reopened.dispatchResume(resumeRequest(source.runId, runId, "unknown-2"))).state, "uncertain");
		assert.equal(calls.value, 1);
		journal.close();
	});

	it("terminalizes exact released-lease proof and replays without execution", async () => {
		const journal = new ManagedOperationJournal({ root: path.join(temporary, "journal-terminal") });
		const source = createTerminalSource(journal);
		const runId = candidate("terminal");
		const calls = { value: 0 };
		const target = coordinator(journal, successfulExecutor(runId, calls, true));
		assert.equal((await target.dispatchResume(resumeRequest(source.runId, runId))).state, "terminal");
		assert.equal((await target.dispatchResume(resumeRequest(source.runId, runId, "retry-terminal"))).state, "terminal");
		assert.equal(calls.value, 1);
		journal.close();
	});

	it("marks every post-boundary failure uncertain and does not relaunch", async () => {
		const journal = new ManagedOperationJournal({ root: path.join(temporary, "journal-uncertain") });
		const source = createTerminalSource(journal);
		const runId = candidate("uncertain");
		let calls = 0;
		const executor: ManagedResumeExecutor = { executePreparedResume: (async (_id, _params, _signal, _update, _ctx, options) => {
			calls++;
			const resolvedLaunch = resolved(runId, options.source);
			const roots = resolvedLaunch.contract.launchContract.roots;
			const plan = { runId, sourceRunId: options.source.sourceRunId, sourceIndex: 0 as const, sourceOperationId: options.source.sourceOperationId, sourceSessionFile: options.source.canonicalSessionFile, sourceCanonicalSessionId: options.source.canonicalSessionId, sourceTerminalProofDigest: options.source.sourceTerminalProofDigest, parentSessionId: "parent", parentSessionFile: path.join(temporary, "parent.jsonl"), cwd: temporary, asyncDir: roots.asyncDir!, resultPath: roots.resultPath!, resultReservationPath: roots.resultReservationPath!, runnerConfigPath: roots.runnerConfigPath!, runnerAdmissionPath: roots.runnerAdmissionPath!, runnerAdmissionProceedPath: roots.runnerAdmissionProceedPath!, runnerAdmissionCommitPath: roots.runnerAdmissionCommitPath! };
			await options.beforeLaunch(plan); options.afterAuthorization(plan);
			return { content: [], isError: true, details: { mode: "single", results: [] } };
		}) as ManagedResumeExecutor["executePreparedResume"] };
		const target = coordinator(journal, executor);
		assert.equal((await target.dispatchResume(resumeRequest(source.runId, runId))).state, "uncertain");
		assert.equal((await target.dispatchResume(resumeRequest(source.runId, runId, "retry"))).state, "uncertain");
		assert.equal(calls, 1);
		journal.close();
	});
});

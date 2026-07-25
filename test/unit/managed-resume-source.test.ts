import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { computeManagedRequestDigest } from "../../src/api/managed-dispatch.ts";
import { ManagedOperationJournal } from "../../src/managed/operation-journal.ts";
import { ManagedResumeSourceError, resolveManagedResumeSourceV1 } from "../../src/managed/resume-source.ts";
import {
	computePreparedRunnerAdmissionTokenDigest,
	createPreparedRunnerAdmission,
	preparedRunnerAdmissionPaths,
	writePreparedRunnerAdmissionEvidence,
} from "../../src/runs/background/prepared-runner-admission.ts";
import { computeManagedProcessTerminalProofDigest } from "../../src/runs/background/process-terminal.ts";
import { acquireSessionLease, canonicalSessionId, type SessionLeaseHandle } from "../../src/runs/shared/session-lease.ts";
import type { ProcessTerminalV1 } from "../../src/shared/types.ts";

let temporary = "";
let lease: SessionLeaseHandle | undefined;
const parentDigest = "a".repeat(64);
const sourceOperationId = Buffer.alloc(32, 31).toString("base64url");

beforeEach(() => { temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-resume-source-")); });
afterEach(() => { lease?.release(); lease = undefined; fs.rmSync(temporary, { recursive: true, force: true }); });

function sourceRequest(runId: string, method: "spawn" | "resume" = "spawn") {
	const base = {
		version: 1,
		requestId: "source-transport",
		method,
		managed: { version: 1, consumerId: "pi-signal", operationId: sourceOperationId },
		expectedLaunch: {
			version: 1,
			hostId: "host-1",
			candidateRunId: runId,
			profileIdentityDigest: "b".repeat(64),
			parentSessionIdentityDigest: parentDigest,
			contractDigest: "c".repeat(64),
		},
	};
	return method === "spawn"
		? { ...base, input: { request: { agent: "worker", task: "opaque", context: "fresh", async: true, clarify: false, cwd: temporary, sessionDir: path.join(temporary, "sessions", runId) } } }
		: { ...base, input: { sourceRunId: "prior-run", index: 0, request: { action: "resume", runId: "prior-run", index: 0, message: "continue", async: true, clarify: false, context: "fresh" } } };
}

function terminalSource(method: "spawn" | "resume" = "spawn"): { journal: ManagedOperationJournal; runId: string; sessionFile: string; asyncDir: string } {
	const runId = "source-run";
	const asyncDir = path.join(temporary, "async", runId);
	const sessionFile = path.join(temporary, "sessions", runId, "run-0", "session.jsonl");
	fs.mkdirSync(asyncDir, { recursive: true });
	fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
	fs.writeFileSync(sessionFile, "{}\n", "utf8");
	const request = sourceRequest(runId, method);
	const requestDigest = computeManagedRequestDigest(request);
	const journal = new ManagedOperationJournal({ root: path.join(temporary, "journal") });
	journal.claim(parentDigest, request);
	journal.transition(parentDigest, "pi-signal", sourceOperationId, requestDigest, "prepared", method === "resume" ? {
		sourceOperationId: Buffer.alloc(32, 29).toString("base64url"),
		sourceRequestDigest: "7".repeat(64),
		sourceTerminalProofDigest: "8".repeat(64),
		sourceCanonicalSessionId: canonicalSessionId(sessionFile),
		sourceRecoveryDescriptorDigest: "9".repeat(64),
	} : {});
	journal.transition(parentDigest, "pi-signal", sourceOperationId, requestDigest, "dispatching", { runId, terminalAsyncDir: asyncDir, canonicalSessionFile: sessionFile });
	const resumeBinding = method === "resume" ? { version: 1 as const, sourceRunId: "prior-run", sourceIndex: 0 as const, canonicalSessionId: canonicalSessionId(sessionFile) } : undefined;
	const admission = createPreparedRunnerAdmission(runId, requestDigest, resumeBinding);
	const leaseDigest = method === "resume" ? "6".repeat(64) : undefined;
	journal.transition(parentDigest, "pi-signal", sourceOperationId, requestDigest, "runner-ready", {
		runnerProcessInstanceId: "source-runner",
		runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token),
		...(leaseDigest ? { runnerSessionLeaseTokenDigest: leaseDigest, runnerCanonicalSessionId: canonicalSessionId(sessionFile) } : {}),
	});
	journal.transition(parentDigest, "pi-signal", sourceOperationId, requestDigest, "accepted");
	writePreparedRunnerAdmissionEvidence(preparedRunnerAdmissionPaths(asyncDir).evidencePath, admission, "committed", 123, "source-runner", 10, leaseDigest);
	const sessionStats = fs.statSync(sessionFile, { bigint: true });
	const sessionDevice = String(sessionStats.dev);
	const sessionInode = String(sessionStats.ino);
	const managed = {
		version: 1 as const,
		parentSessionIdentityDigest: parentDigest,
		consumerId: "pi-signal",
		operationId: sourceOperationId,
		requestDigest,
		candidateRunId: runId,
		runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token),
		...(leaseDigest ? { sessionLeaseTokenDigest: leaseDigest } : {}),
	};
	const proof: ProcessTerminalV1 = {
		version: 1,
		state: "observed",
		runId,
		runnerProcessInstanceId: "source-runner",
		observedAt: 20,
		instances: [{ kind: "runner", processInstanceId: "source-runner", closeObservedAt: 20, exitCode: 0, signal: null }],
		managed,
		canonicalSession: { canonicalSessionId: canonicalSessionId(sessionFile), sessionDevice, sessionInode, leaseDisposition: method === "resume" ? "released" : "not-held", freeAtObservation: true, ...(method === "resume" ? { canonicalSessionLeaseReleased: true as const } : {}) },
		resumeDisposition: "resumable",
	};
	fs.writeFileSync(path.join(asyncDir, "process-terminal.json"), JSON.stringify(proof), "utf8");
	journal.transition(parentDigest, "pi-signal", sourceOperationId, requestDigest, "terminal", {
		terminalEvidence: { version: 1, proofDigest: computeManagedProcessTerminalProofDigest(proof), observedAt: 20, canonicalSessionId: canonicalSessionId(sessionFile), sessionDevice, sessionInode },
	});
	fs.writeFileSync(path.join(asyncDir, "recovery-descriptor.json"), JSON.stringify({
		version: 1,
		sourceRunId: runId,
		agent: "worker",
		sessionFile,
		cwd: temporary,
		systemPromptMode: "replace",
		inheritProjectContext: false,
		inheritSkills: false,
		outputMode: "inline",
		maxSubagentDepth: 1,
		share: false,
	}), "utf8");
	return { journal, runId, sessionFile, asyncDir };
}

function expectCode(action: () => unknown, code: string): void {
	assert.throws(action, (error: unknown) => error instanceof ManagedResumeSourceError && error.code === code);
}

describe("exact managed resume source", () => {
	it("resolves only exact same-consumer terminal proof and retained canonical JSONL", () => {
		const source = terminalSource();
		const resolved = resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", sourceRunId: source.runId, index: 0 });
		assert.equal(resolved.sourceOperationId, sourceOperationId);
		assert.equal(resolved.sourceRunId, source.runId);
		assert.equal(resolved.sourceIndex, 0);
		assert.equal(resolved.canonicalSessionFile, source.sessionFile);
		assert.equal(resolved.canonicalSessionId, canonicalSessionId(source.sessionFile));
		assert.match(resolved.recoveryDescriptorDigest, /^[a-f0-9]{64}$/);
		assert.equal(Object.isFrozen(resolved), true);
		assert.equal(Object.isFrozen(resolved.recoveryDescriptor), true);
		expectCode(() => resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "other", sourceRunId: source.runId, index: 0 }), "not_found");
		expectCode(() => resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", sourceRunId: "source", index: 0 }), "not_found");
		expectCode(() => resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", sourceRunId: source.runId, index: 1 }), "invalid_request");
		source.journal.close();
	});

	it("rejects substituted resume admission source and lease correlation", () => {
		const source = terminalSource("resume");
		const admissionPath = preparedRunnerAdmissionPaths(source.asyncDir).evidencePath;
		const original = JSON.parse(fs.readFileSync(admissionPath, "utf8")) as Record<string, unknown>;
		fs.writeFileSync(admissionPath, JSON.stringify({ ...original, resume: { ...(original.resume as object), sourceRunId: "different-prior" } }));
		expectCode(() => resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", sourceRunId: source.runId, index: 0 }), "operation_uncertain");
		fs.writeFileSync(admissionPath, JSON.stringify({ ...original, sessionLeaseTokenDigest: "5".repeat(64) }));
		expectCode(() => resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", sourceRunId: source.runId, index: 0 }), "operation_uncertain");
		source.journal.close();
	});

	it("rejects same-path replacement of the terminal session JSONL", () => {
		const source = terminalSource();
		fs.renameSync(source.sessionFile, `${source.sessionFile}.original`);
		fs.writeFileSync(source.sessionFile, "{\"replacement\":true}\n", "utf8");
		expectCode(() => resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", sourceRunId: source.runId, index: 0 }), "operation_uncertain");
		source.journal.close();
	});

	it("fails closed while the exact canonical session lease is owned", () => {
		const source = terminalSource();
		lease = acquireSessionLease({ sessionFile: source.sessionFile, runId: "contender", sourceRunId: source.runId });
		expectCode(() => resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", sourceRunId: source.runId, index: 0 }), "invalid_state");
		source.journal.close();
	});

	it("rejects unknown or changed terminal evidence without mutating the source", () => {
		const source = terminalSource();
		const before = source.journal.read(parentDigest, "pi-signal", sourceOperationId);
		fs.writeFileSync(path.join(source.asyncDir, "process-terminal.json"), JSON.stringify({ version: 1, state: "unknown", runId: source.runId, runnerProcessInstanceId: "source-runner", reason: "observer-unavailable" }), "utf8");
		expectCode(() => resolveManagedResumeSourceV1({ journal: source.journal, parentSessionIdentityDigest: parentDigest, consumerId: "pi-signal", sourceRunId: source.runId, index: 0 }), "operation_uncertain");
		assert.deepEqual(source.journal.read(parentDigest, "pi-signal", sourceOperationId), before);
		source.journal.close();
	});
});

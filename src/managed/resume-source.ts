import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import {
	SUBAGENT_MANAGED_DISPATCH_VERSION,
	assertManagedConsumerId,
	canonicalizeManagedJson,
	type ManagedConsumerId,
	type ManagedDispatchErrorCodeV1,
} from "../api/managed-dispatch.ts";
import type { ManagedOperationJournalRecordV1 } from "./operation-journal.ts";
import { ManagedOperationJournal } from "./operation-journal.ts";
import {
	computePreparedRunnerAdmissionTokenDigest,
	preparedRunnerAdmissionPaths,
	readPreparedRunnerAdmissionEvidenceForDispatch,
} from "../runs/background/prepared-runner-admission.ts";
import { computeManagedProcessTerminalProofDigest, readProcessTerminal } from "../runs/background/process-terminal.ts";
import { readAsyncRecoveryDescriptor } from "../runs/background/async-resume.ts";
import { canonicalSessionFilePath, canonicalSessionId, inspectSessionLease } from "../runs/shared/session-lease.ts";
import type { SteeringRecoveryDescriptor } from "../shared/types.ts";

const MAX_RECOVERY_DESCRIPTOR_BYTES = 1_048_576;

export class ManagedResumeSourceError extends Error {
	readonly code: ManagedDispatchErrorCodeV1;

	constructor(code: ManagedDispatchErrorCodeV1, message: string) {
		super(message);
		this.name = "ManagedResumeSourceError";
		this.code = code;
	}
}

export interface ManagedResumeSourceV1 {
	readonly version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	readonly consumerId: ManagedConsumerId;
	readonly sourceOperationId: string;
	readonly sourceRequestDigest: string;
	readonly sourceRunId: string;
	readonly sourceIndex: 0;
	readonly sourceTerminalProofDigest: string;
	readonly canonicalSessionFile: string;
	readonly canonicalSessionId: string;
	readonly sessionDevice: string;
	readonly sessionInode: string;
	readonly recoveryDescriptor: Readonly<SteeringRecoveryDescriptor>;
	readonly recoveryDescriptorDigest: string;
	readonly agent: string;
	readonly cwd: string;
	readonly model?: string;
	readonly thinking?: string;
}

function fail(code: ManagedDispatchErrorCodeV1, message: string): never {
	throw new ManagedResumeSourceError(code, message);
}

function digest(domain: string, value: unknown): string {
	return createHash("sha256")
		.update(domain, "utf8")
		.update("\0", "utf8")
		.update(canonicalizeManagedJson(value).serialization, "utf8")
		.digest("hex");
}

function assertBoundedDescriptor(asyncDir: string): void {
	const descriptorPath = path.join(asyncDir, "recovery-descriptor.json");
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(descriptorPath, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
		const stats = fs.fstatSync(descriptor);
		if (!stats.isFile() || stats.size < 1 || stats.size > MAX_RECOVERY_DESCRIPTOR_BYTES) {
			fail("operation_uncertain", "Managed resume recovery descriptor is unavailable.");
		}
	} catch (error) {
		if (error instanceof ManagedResumeSourceError) throw error;
		fail("operation_uncertain", "Managed resume recovery descriptor is unavailable.");
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
	}
}

function assertCanonicalSessionFile(sessionFile: string): { canonical: string; canonicalId: string; device: string; inode: string } {
	if (path.extname(sessionFile) !== ".jsonl" || !path.isAbsolute(sessionFile) || path.resolve(sessionFile) !== sessionFile) {
		return fail("operation_uncertain", "Managed resume canonical session identity is invalid.");
	}
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(sessionFile, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
		const stats = fs.fstatSync(descriptor, { bigint: true });
		if (!stats.isFile()) return fail("operation_uncertain", "Managed resume canonical session is not a regular file.");
		const canonical = canonicalSessionFilePath(sessionFile);
		if (canonical !== sessionFile) return fail("operation_uncertain", "Managed resume canonical session path changed.");
		return { canonical, canonicalId: canonicalSessionId(canonical), device: String(stats.dev), inode: String(stats.ino) };
	} catch (error) {
		if (error instanceof ManagedResumeSourceError) throw error;
		return fail("operation_uncertain", "Managed resume canonical session is unavailable.");
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
	}
}

function exactSourceRecord(
	journal: ManagedOperationJournal,
	parentSessionIdentityDigest: string,
	consumerId: ManagedConsumerId,
	sourceRunId: string,
): Readonly<ManagedOperationJournalRecordV1> {
	let record: Readonly<ManagedOperationJournalRecordV1> | undefined;
	try { record = journal.readByRun(parentSessionIdentityDigest, consumerId, sourceRunId); } catch {
		return fail("operation_uncertain", "Managed resume source lookup is uncertain.");
	}
	if (!record) return fail("not_found", "Managed resume source was not found.");
	if ((record.method !== "spawn" && record.method !== "resume") || record.state !== "terminal" || record.runId !== sourceRunId
		|| !record.terminalEvidence || !record.terminalEvidence.sessionDevice || !record.terminalEvidence.sessionInode
		|| !record.terminalAsyncDir || !record.canonicalSessionFile
		|| !record.runnerProcessInstanceId || !record.runnerAdmissionTokenDigest
		|| (record.method === "resume" && (!record.sourceRunId || !record.runnerSessionLeaseTokenDigest || !record.runnerCanonicalSessionId))) {
		return fail(record.state === "retired" ? "retired" : "invalid_state", "Managed resume source is not a resumable terminal actor.");
	}
	return record;
}

/** Non-mutating exact source inspection. It never performs prefix routing or stale-run repair. */
export function resolveManagedResumeSourceV1(input: {
	journal: ManagedOperationJournal;
	parentSessionIdentityDigest: string;
	consumerId: string;
	sourceRunId: string;
	index: number;
}): Readonly<ManagedResumeSourceV1> {
	const consumerId = assertManagedConsumerId(input.consumerId);
	if (input.index !== 0 || !/^[A-Za-z0-9][A-Za-z0-9._~:-]{0,255}$/.test(input.sourceRunId)) {
		return fail("invalid_request", "Managed resume requires exact child index 0 and a complete source run id.");
	}
	const record = exactSourceRecord(input.journal, input.parentSessionIdentityDigest, consumerId, input.sourceRunId);
	const managed = {
		version: 1 as const,
		parentSessionIdentityDigest: record.parentSessionIdentityDigest,
		consumerId: record.consumerId,
		operationId: record.operationId,
		requestDigest: record.requestDigest,
		candidateRunId: record.runId!,
		runnerAdmissionTokenDigest: record.runnerAdmissionTokenDigest!,
		...(record.runnerSessionLeaseTokenDigest ? { sessionLeaseTokenDigest: record.runnerSessionLeaseTokenDigest } : {}),
	};
	let admission: ReturnType<typeof readPreparedRunnerAdmissionEvidenceForDispatch>;
	try {
		admission = readPreparedRunnerAdmissionEvidenceForDispatch(
			preparedRunnerAdmissionPaths(record.terminalAsyncDir!).evidencePath,
			{
				runId: record.runId!,
				dispatchIdentityDigest: record.requestDigest,
				...(record.method === "resume" ? {
					resume: {
						version: 1 as const,
						sourceRunId: record.sourceRunId!,
						sourceIndex: 0 as const,
						canonicalSessionId: record.runnerCanonicalSessionId!,
					},
				} : {}),
			},
		);
	} catch {
		return fail("operation_uncertain", "Managed resume source admission evidence is unavailable or mismatched.");
	}
	if (admission?.state !== "committed" || admission.runnerProcessInstanceId !== record.runnerProcessInstanceId
		|| computePreparedRunnerAdmissionTokenDigest(admission.token) !== record.runnerAdmissionTokenDigest
		|| (record.method === "resume" && (!record.runnerSessionLeaseTokenDigest
			|| !record.runnerCanonicalSessionId
			|| admission.sessionLeaseTokenDigest !== record.runnerSessionLeaseTokenDigest))) {
		return fail("operation_uncertain", "Managed resume source admission evidence is unavailable or mismatched.");
	}
	const proof = readProcessTerminal(record.terminalAsyncDir!, {
		runId: record.runId,
		runnerProcessInstanceId: record.runnerProcessInstanceId,
		managed,
	});
	if (!proof || proof.state !== "observed" || proof.resumeDisposition !== "resumable"
		|| proof.canonicalSession?.freeAtObservation !== true
		|| computeManagedProcessTerminalProofDigest(proof) !== record.terminalEvidence!.proofDigest) {
		return fail("operation_uncertain", "Managed resume source terminal proof is unavailable or mismatched.");
	}
	const session = assertCanonicalSessionFile(record.canonicalSessionFile!);
	if (proof.canonicalSession.canonicalSessionId !== session.canonicalId
		|| record.terminalEvidence!.canonicalSessionId !== session.canonicalId
		|| !record.terminalEvidence!.sessionDevice || !record.terminalEvidence!.sessionInode
		|| proof.canonicalSession.sessionDevice !== record.terminalEvidence!.sessionDevice
		|| proof.canonicalSession.sessionInode !== record.terminalEvidence!.sessionInode
		|| session.device !== record.terminalEvidence!.sessionDevice
		|| session.inode !== record.terminalEvidence!.sessionInode) {
		return fail("operation_uncertain", "Managed resume source canonical session differs from terminal evidence.");
	}
	let lease: ReturnType<typeof inspectSessionLease>;
	try { lease = inspectSessionLease(session.canonical); } catch {
		return fail("operation_uncertain", "Managed resume source lease state is unreadable.");
	}
	if (lease.state !== "free") return fail(lease.state === "owned" ? "invalid_state" : "operation_uncertain", "Managed resume source session is not free.");
	assertBoundedDescriptor(record.terminalAsyncDir!);
	let recoveryDescriptor: SteeringRecoveryDescriptor | undefined;
	try { recoveryDescriptor = readAsyncRecoveryDescriptor(record.terminalAsyncDir); } catch {
		return fail("operation_uncertain", "Managed resume recovery descriptor is invalid.");
	}
	if (!recoveryDescriptor || recoveryDescriptor.sourceRunId !== record.runId || recoveryDescriptor.sessionFile !== session.canonical
		|| typeof recoveryDescriptor.agent !== "string" || !recoveryDescriptor.agent
		|| typeof recoveryDescriptor.cwd !== "string" || !path.isAbsolute(recoveryDescriptor.cwd)) {
		return fail("operation_uncertain", "Managed resume recovery descriptor identity is inconsistent.");
	}
	let frozenDescriptor: Readonly<SteeringRecoveryDescriptor>;
	try { frozenDescriptor = canonicalizeManagedJson(recoveryDescriptor).normalized as unknown as Readonly<SteeringRecoveryDescriptor>; } catch {
		return fail("operation_uncertain", "Managed resume recovery descriptor is not canonical JSON.");
	}
	const recoveryDescriptorDigest = digest("pi-subagents/managed-dispatch/v1/recovery-descriptor", frozenDescriptor);
	return Object.freeze({
		version: 1,
		consumerId,
		sourceOperationId: record.operationId,
		sourceRequestDigest: record.requestDigest,
		sourceRunId: record.runId,
		sourceIndex: 0,
		sourceTerminalProofDigest: record.terminalEvidence.proofDigest,
		canonicalSessionFile: session.canonical,
		canonicalSessionId: session.canonicalId,
		sessionDevice: session.device,
		sessionInode: session.inode,
		recoveryDescriptor: frozenDescriptor,
		recoveryDescriptorDigest,
		agent: recoveryDescriptor.agent,
		cwd: recoveryDescriptor.cwd,
		...(recoveryDescriptor.model ? { model: recoveryDescriptor.model } : {}),
		...(recoveryDescriptor.thinking ? { thinking: recoveryDescriptor.thinking } : {}),
	});
}

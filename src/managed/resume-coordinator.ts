import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	SUBAGENT_MANAGED_DISPATCH_VERSION,
	assertManagedConsumerId,
	assertManagedOperationId,
	computeManagedRequestDigest,
	parseManagedMutationRequestV1,
	type ManagedDispatchErrorCodeV1,
	type ManagedDispatchReceiptV1,
	type ManagedExpectedLaunchV1,
	type ManagedResumeRequestV1,
} from "../api/managed-dispatch.ts";
import { managedLaunchRootProjectionsAreCurrent } from "../api/preflight.ts";
import { loadOrCreateManagedDispatchHostId } from "../extension/managed-dispatch-preflight.ts";
import {
	computePreparedRunnerAdmissionTokenDigest,
	preparedRunnerAdmissionPaths,
	readPreparedRunnerAdmissionEvidenceForDispatch,
	type PreparedRunnerAdmissionEvidenceV1,
} from "../runs/background/prepared-runner-admission.ts";
import { computeManagedProcessTerminalProofDigest, readProcessTerminal } from "../runs/background/process-terminal.ts";
import { canonicalSessionId, inspectSessionLease } from "../runs/shared/session-lease.ts";
import type { ManagedProcessTerminalBindingV1 } from "../shared/types.ts";
import type {
	PreparedSubagentResumeOptions,
	PreparedSubagentResumePlan,
	SubagentParamsLike,
	createSubagentExecutor,
} from "../runs/foreground/subagent-executor.ts";
import { ManagedOperationJournal, type ManagedOperationJournalRecordV1 } from "./operation-journal.ts";
import { resolveManagedResumeLaunchV1, type ManagedResumeLaunchResolverOptions, type ResolvedManagedResumeLaunchV1 } from "./resume-contract.ts";
import { resolveManagedResumeSourceV1, type ManagedResumeSourceV1 } from "./resume-source.ts";

export interface ManagedResumeExecutor {
	executePreparedResume: ReturnType<typeof createSubagentExecutor>["executePreparedResume"];
}

interface ParentSnapshot {
	ctx: ExtensionContext;
	parentSessionId: string;
	parentSessionFile: string;
	generation: number;
}

export interface ManagedResumeCoordinatorOptions {
	journal: ManagedOperationJournal;
	executor: ManagedResumeExecutor;
	getContext: () => ExtensionContext | null;
	getSessionGeneration?: () => number;
	hostIdPath?: string;
	loadHostId?: (filePath?: string) => string;
	artifactDir?: "project" | "session" | "temp";
	resolveContract?: ManagedResumeLaunchResolverOptions["resolveContract"];
	resolveCapabilityCeiling?: ManagedResumeLaunchResolverOptions["resolveCapabilityCeiling"];
	resolveLaunch?: typeof resolveManagedResumeLaunchV1;
}

export class ManagedResumeCoordinatorError extends Error {
	readonly code: ManagedDispatchErrorCodeV1;
	constructor(code: ManagedDispatchErrorCodeV1, message: string) {
		super(message);
		this.name = "ManagedResumeCoordinatorError";
		this.code = code;
	}
}

function fail(code: ManagedDispatchErrorCodeV1, message: string): never {
	throw new ManagedResumeCoordinatorError(code, message);
}

function verifyExpected(expected: ManagedExpectedLaunchV1, resolved: ResolvedManagedResumeLaunchV1, hostId: string): void {
	if (expected.hostId !== hostId) fail("host_mismatch", "Managed resume host identity changed.");
	if (!expected.parentSessionIdentityDigest || expected.parentSessionIdentityDigest !== resolved.contract.parentSessionIdentityDigest) fail("host_mismatch", "Managed resume parent identity changed.");
	if (expected.candidateRunId !== resolved.contract.runId || expected.contractDigest !== resolved.contract.digest) fail("contract_changed", "Managed resume contract identity changed.");
	if (expected.profileIdentityDigest !== resolved.profileIdentityDigest) fail("profile_changed", "Managed resume profile identity changed.");
}

function receipt(record: Readonly<ManagedOperationJournalRecordV1>, replayed: boolean): ManagedDispatchReceiptV1 {
	const state = record.state === "dispatching" || record.state === "reconciling" ? "uncertain" : record.state;
	return Object.freeze({
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		consumerId: assertManagedConsumerId(record.consumerId),
		operationId: assertManagedOperationId(record.operationId),
		requestDigest: record.requestDigest,
		state,
		...(record.runId ? { runId: record.runId } : {}),
		...(record.sourceRunId ? { sourceRunId: record.sourceRunId } : {}),
		replayed,
	});
}

function sameSource(a: Readonly<ManagedResumeSourceV1>, b: Readonly<ManagedResumeSourceV1>): boolean {
	return a.sourceOperationId === b.sourceOperationId && a.sourceRequestDigest === b.sourceRequestDigest
		&& a.sourceRunId === b.sourceRunId && a.sourceTerminalProofDigest === b.sourceTerminalProofDigest
		&& a.canonicalSessionFile === b.canonicalSessionFile && a.canonicalSessionId === b.canonicalSessionId
		&& a.sessionDevice === b.sessionDevice && a.sessionInode === b.sessionInode
		&& a.recoveryDescriptorDigest === b.recoveryDescriptorDigest;
}

function planMatches(plan: Readonly<PreparedSubagentResumePlan>, resolved: ResolvedManagedResumeLaunchV1): boolean {
	const roots = resolved.contract.launchContract.roots;
	return plan.runId === resolved.contract.runId && plan.sourceRunId === resolved.source.sourceRunId
		&& plan.sourceOperationId === resolved.source.sourceOperationId && plan.sourceSessionFile === resolved.source.canonicalSessionFile
		&& plan.sourceCanonicalSessionId === resolved.source.canonicalSessionId
		&& plan.sourceTerminalProofDigest === resolved.source.sourceTerminalProofDigest
		&& plan.cwd === resolved.source.cwd && plan.asyncDir === roots.asyncDir && plan.resultPath === roots.resultPath
		&& plan.resultReservationPath === roots.resultReservationPath && plan.runnerConfigPath === roots.runnerConfigPath
		&& plan.runnerAdmissionPath === roots.runnerAdmissionPath && plan.runnerAdmissionProceedPath === roots.runnerAdmissionProceedPath
		&& plan.runnerAdmissionCommitPath === roots.runnerAdmissionCommitPath
		&& plan.artifactsDir === roots.artifactsDir && plan.outputPath === roots.outputPath;
}

function operationKey(parent: string, request: ManagedResumeRequestV1): string {
	return `${parent}\0${request.managed.consumerId}\0${request.managed.operationId}`;
}

/** Unregistered exact managed resume coordinator. Provider activation remains a later gate. */
export class ManagedResumeCoordinator {
	readonly #options: ManagedResumeCoordinatorOptions;
	readonly #inFlight = new Map<string, { digest: string; promise: Promise<ManagedDispatchReceiptV1> }>();

	constructor(options: ManagedResumeCoordinatorOptions) { this.#options = options; }

	/** Reconciles an already-claimed resume operation without ever launching it. */
	reconcileExisting(
		record: Readonly<ManagedOperationJournalRecordV1>,
		options: { observerLost?: boolean } = {},
	): Readonly<ManagedOperationJournalRecordV1> {
		if (record.method !== "resume") return record;
		return options.observerLost ? this.reconcileAfterObserverLoss(record) : this.#reconcile(record);
	}

	#snapshot(): ParentSnapshot {
		try {
			const ctx = this.#options.getContext();
			const parentSessionId = ctx?.sessionManager.getSessionId();
			const parentSessionFile = ctx?.sessionManager.getSessionFile();
			if (!ctx || !parentSessionId || !parentSessionFile) return fail("no_active_session", "Managed resume requires an active persisted parent session.");
			return { ctx, parentSessionId, parentSessionFile, generation: this.#options.getSessionGeneration?.() ?? 0 };
		} catch { return fail("no_active_session", "Managed resume requires an active persisted parent session."); }
	}

	#current(snapshot: ParentSnapshot): boolean {
		try {
			const ctx = this.#options.getContext();
			return ctx?.sessionManager.getSessionId() === snapshot.parentSessionId
				&& ctx?.sessionManager.getSessionFile() === snapshot.parentSessionFile
				&& (this.#options.getSessionGeneration?.() ?? 0) === snapshot.generation;
		} catch { return false; }
	}

	#source(request: ManagedResumeRequestV1, parentDigest: string): Readonly<ManagedResumeSourceV1> {
		return resolveManagedResumeSourceV1({
			journal: this.#options.journal,
			parentSessionIdentityDigest: parentDigest,
			consumerId: request.managed.consumerId,
			sourceRunId: request.input.sourceRunId,
			index: request.input.index,
		});
	}

	async #resolve(request: ManagedResumeRequestV1, source: Readonly<ManagedResumeSourceV1>, snapshot: ParentSnapshot): Promise<ResolvedManagedResumeLaunchV1> {
		const resolved = await (this.#options.resolveLaunch ?? resolveManagedResumeLaunchV1)(
			request.input.request, request.expectedLaunch.candidateRunId, source, snapshot.ctx,
			snapshot.parentSessionId, snapshot.parentSessionFile, {
				artifactDir: this.#options.artifactDir,
				resolveContract: this.#options.resolveContract,
				resolveCapabilityCeiling: this.#options.resolveCapabilityCeiling,
			},
		);
		if (!this.#current(snapshot)) return fail("no_active_session", "Managed resume parent session changed during authorization.");
		return resolved;
	}

	async dispatchResume(payload: unknown, signal: AbortSignal = new AbortController().signal): Promise<ManagedDispatchReceiptV1> {
		let request: ManagedResumeRequestV1;
		try {
			const parsed = parseManagedMutationRequestV1(payload);
			if (parsed.method !== "resume") return fail("unsupported_method", "Managed resume coordinator accepts resume only.");
			request = parsed;
		} catch (error) {
			if (error instanceof ManagedResumeCoordinatorError) throw error;
			return fail("invalid_request", "Managed resume request is invalid.");
		}
		const snapshot = this.#snapshot();
		if (!request.expectedLaunch.parentSessionIdentityDigest) return fail("host_mismatch", "Managed resume requires parent-session identity.");
		const parentDigest = request.expectedLaunch.parentSessionIdentityDigest;
		const source = this.#source(request, parentDigest);
		const resolved = await this.#resolve(request, source, snapshot);
		const hostId = (this.#options.loadHostId ?? loadOrCreateManagedDispatchHostId)(this.#options.hostIdPath);
		verifyExpected(request.expectedLaunch, resolved, hostId);
		const digest = computeManagedRequestDigest(request);
		const key = operationKey(parentDigest, request);
		const active = this.#inFlight.get(key);
		if (active) {
			if (active.digest !== digest) return fail("operation_conflict", "Managed resume operation is active with different semantics.");
			const joined = await active.promise;
			return Object.freeze({ ...joined, replayed: true });
		}
		const promise = this.#claimAndDispatch(request, source, resolved, snapshot, signal);
		const entry = { digest, promise };
		this.#inFlight.set(key, entry);
		try { return await promise; } finally { if (this.#inFlight.get(key) === entry) this.#inFlight.delete(key); }
	}

	async #claimAndDispatch(request: ManagedResumeRequestV1, initialSource: Readonly<ManagedResumeSourceV1>, initialResolved: ResolvedManagedResumeLaunchV1, snapshot: ParentSnapshot, signal: AbortSignal): Promise<ManagedDispatchReceiptV1> {
		const parent = initialResolved.contract.parentSessionIdentityDigest;
		const consumer = request.managed.consumerId;
		const operation = request.managed.operationId;
		const digest = computeManagedRequestDigest(request);
		const claim = this.#options.journal.claim(parent, request);
		let record = claim.record;
		if (claim.replayed) {
			record = this.#reconcile(record);
			if (record.state !== "claimed" && record.state !== "prepared") return receipt(record, true);
		}
		if (record.state === "claimed") {
			record = this.#options.journal.transition(parent, consumer, operation, digest, "prepared", {
				sourceOperationId: initialSource.sourceOperationId,
				sourceRequestDigest: initialSource.sourceRequestDigest,
				sourceTerminalProofDigest: initialSource.sourceTerminalProofDigest,
				sourceCanonicalSessionId: initialSource.canonicalSessionId,
				sourceRecoveryDescriptorDigest: initialSource.recoveryDescriptorDigest,
			});
		}
		if (record.state !== "prepared") return receipt(record, claim.replayed);
		let authorized: ResolvedManagedResumeLaunchV1 | undefined;
		const options: PreparedSubagentResumeOptions = {
			runId: request.expectedLaunch.candidateRunId,
			dispatchIdentityDigest: digest,
			source: initialSource,
			execution: initialResolved.execution,
			processTerminalBinding: { version: 1, parentSessionIdentityDigest: parent, consumerId: consumer, operationId: operation, requestDigest: digest, candidateRunId: request.expectedLaunch.candidateRunId },
			beforeLaunch: async (plan) => {
				const execution = this.#snapshot();
				if (execution.parentSessionId !== snapshot.parentSessionId || execution.parentSessionFile !== snapshot.parentSessionFile || execution.generation !== snapshot.generation) return fail("no_active_session", "Managed resume parent changed before dispatch.");
				const source = this.#source(request, parent);
				if (!sameSource(source, initialSource)) return fail("contract_changed", "Managed resume source identity changed.");
				const resolved = await this.#resolve(request, source, execution);
				verifyExpected(request.expectedLaunch, resolved, (this.#options.loadHostId ?? loadOrCreateManagedDispatchHostId)(this.#options.hostIdPath));
				if (plan.parentSessionId !== execution.parentSessionId || plan.parentSessionFile !== execution.parentSessionFile || !planMatches(plan, resolved)) return fail("contract_changed", "Prepared resume plan differs from authorization.");
				authorized = resolved;
			},
			afterAuthorization: (plan) => {
				if (!this.#current(snapshot) || !authorized || !managedLaunchRootProjectionsAreCurrent(authorized.contract.launchContract)) return fail("contract_changed", "Managed resume final fence failed.");
				const source = this.#source(request, parent);
				if (!sameSource(source, authorized.source) || !planMatches(plan, authorized)) return fail("contract_changed", "Managed resume source changed after authorization.");
				const lease = inspectSessionLease(source.canonicalSessionFile);
				if (lease.state !== "free") return fail("operation_uncertain", "Managed resume source lease is not free.");
				const current = this.#options.journal.read(parent, consumer, operation);
				if (!current || current.state !== "prepared" || current.runId !== undefined) return fail("operation_conflict", "Managed resume boundary state changed.");
				this.#options.journal.transition(parent, consumer, operation, digest, "dispatching", { runId: plan.runId, terminalAsyncDir: plan.asyncDir, canonicalSessionFile: plan.sourceSessionFile });
				return undefined;
			},
			onRunnerReady: (evidence) => {
				this.#assertEvidence(evidence, request, digest, initialSource);
				this.#options.journal.transition(parent, consumer, operation, digest, "runner-ready", {
					runId: request.expectedLaunch.candidateRunId,
					runnerProcessInstanceId: evidence.runnerProcessInstanceId,
					runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(evidence.token),
					runnerSessionLeaseTokenDigest: evidence.sessionLeaseTokenDigest,
					runnerCanonicalSessionId: initialSource.canonicalSessionId,
				});
				return undefined;
			},
			onRunnerAccepted: (evidence) => {
				this.#assertEvidence(evidence, request, digest, initialSource);
				const current = this.#options.journal.read(parent, consumer, operation);
				if (!current || current.runnerProcessInstanceId !== evidence.runnerProcessInstanceId || current.runnerSessionLeaseTokenDigest !== evidence.sessionLeaseTokenDigest) return fail("operation_conflict", "Managed resume admission correlation changed.");
				this.#options.journal.transition(parent, consumer, operation, digest, "accepted", { runId: request.expectedLaunch.candidateRunId });
				return undefined;
			},
			onProcessTerminal: () => { try { const current = this.#options.journal.read(parent, consumer, operation); if (current) this.#reconcile(current); } catch { /* replay owns recovery */ } return undefined; },
		};
		let result: Awaited<ReturnType<ManagedResumeExecutor["executePreparedResume"]>>;
		try { result = await this.#options.executor.executePreparedResume(request.requestId, request.input.request as unknown as SubagentParamsLike, signal, undefined, snapshot.ctx, options); }
		catch { result = { content: [], isError: true } as Awaited<ReturnType<ManagedResumeExecutor["executePreparedResume"]>>; }
		const current = this.#options.journal.read(parent, consumer, operation);
		if (!current) return fail("operation_uncertain", "Managed resume operation disappeared.");
		if (result.isError || (current.state !== "accepted" && current.state !== "terminal")) return receipt(this.#failCurrent(current), claim.replayed);
		if (current.state === "terminal") return receipt(current, claim.replayed);
		if (!this.#hasCommitted(current)) return receipt(this.#options.journal.transition(parent, consumer, operation, digest, "uncertain"), claim.replayed);
		return receipt(current, claim.replayed);
	}

	#assertEvidence(evidence: Readonly<PreparedRunnerAdmissionEvidenceV1>, request: ManagedResumeRequestV1, digest: string, source: ManagedResumeSourceV1): void {
		if (evidence.runId !== request.expectedLaunch.candidateRunId || evidence.dispatchIdentityDigest !== digest
			|| evidence.resume?.sourceRunId !== source.sourceRunId || evidence.resume.sourceIndex !== 0
			|| evidence.resume.canonicalSessionId !== source.canonicalSessionId || !evidence.sessionLeaseTokenDigest) {
			return fail("operation_conflict", "Managed resume runner admission identity changed.");
		}
	}

	#hasCommitted(record: Readonly<ManagedOperationJournalRecordV1>): boolean {
		if (!record.runId || !record.terminalAsyncDir || !record.runnerProcessInstanceId || !record.runnerAdmissionTokenDigest
			|| !record.runnerSessionLeaseTokenDigest || !record.runnerCanonicalSessionId || !record.sourceRunId) return false;
		try {
			const evidence = readPreparedRunnerAdmissionEvidenceForDispatch(preparedRunnerAdmissionPaths(record.terminalAsyncDir).evidencePath, {
				runId: record.runId,
				dispatchIdentityDigest: record.requestDigest,
				resume: { version: 1, sourceRunId: record.sourceRunId, sourceIndex: 0, canonicalSessionId: record.runnerCanonicalSessionId },
			});
			return evidence?.state === "committed" && evidence.runnerProcessInstanceId === record.runnerProcessInstanceId
				&& computePreparedRunnerAdmissionTokenDigest(evidence.token) === record.runnerAdmissionTokenDigest
				&& evidence.sessionLeaseTokenDigest === record.runnerSessionLeaseTokenDigest;
		} catch { return false; }
	}

	#terminal(record: Readonly<ManagedOperationJournalRecordV1>): Readonly<ManagedOperationJournalRecordV1> {
		if (record.state === "terminal" || !["accepted", "uncertain", "reconciling"].includes(record.state) || !this.#hasCommitted(record)
			|| !record.terminalAsyncDir || !record.canonicalSessionFile || !record.runnerProcessInstanceId || !record.runnerAdmissionTokenDigest) return record;
		const managed: ManagedProcessTerminalBindingV1 = { version: 1, parentSessionIdentityDigest: record.parentSessionIdentityDigest, consumerId: record.consumerId, operationId: record.operationId, requestDigest: record.requestDigest, candidateRunId: record.runId!, runnerAdmissionTokenDigest: record.runnerAdmissionTokenDigest, sessionLeaseTokenDigest: record.runnerSessionLeaseTokenDigest };
		const proof = readProcessTerminal(record.terminalAsyncDir, { runId: record.runId, runnerProcessInstanceId: record.runnerProcessInstanceId, managed });
		if (!proof) return record;
		let canonical: string | undefined;
		try { canonical = canonicalSessionId(record.canonicalSessionFile); } catch { canonical = undefined; }
		const exact = proof.state === "observed" && proof.canonicalSession?.canonicalSessionId === canonical
			&& proof.canonicalSession.sessionDevice !== undefined && proof.canonicalSession.sessionInode !== undefined
			&& proof.canonicalSession.canonicalSessionLeaseReleased === true && proof.resumeDisposition === "resumable";
		if (!exact || !canonical) {
			if (record.state === "accepted" || record.state === "reconciling") return this.#options.journal.transition(record.parentSessionIdentityDigest, record.consumerId, record.operationId, record.requestDigest, "uncertain");
			return record;
		}
		let current = record;
		if (current.state === "uncertain") current = this.#options.journal.transition(current.parentSessionIdentityDigest, current.consumerId, current.operationId, current.requestDigest, "reconciling");
		return this.#options.journal.transition(current.parentSessionIdentityDigest, current.consumerId, current.operationId, current.requestDigest, "terminal", {
			terminalEvidence: {
				version: 1,
				proofDigest: computeManagedProcessTerminalProofDigest(proof),
				observedAt: proof.observedAt!,
				canonicalSessionId: canonical,
				sessionDevice: proof.canonicalSession!.sessionDevice!,
				sessionInode: proof.canonicalSession!.sessionInode!,
			},
		});
	}

	reconcileAfterObserverLoss(record: Readonly<ManagedOperationJournalRecordV1>): Readonly<ManagedOperationJournalRecordV1> {
		const acceptedBeforeRecovery = record.state === "accepted";
		const reconciled = this.#reconcile(record);
		if (!acceptedBeforeRecovery || reconciled.state === "terminal") return reconciled;
		if (reconciled.state === "accepted") {
			return this.#options.journal.transition(
				reconciled.parentSessionIdentityDigest,
				reconciled.consumerId,
				reconciled.operationId,
				reconciled.requestDigest,
				"uncertain",
				{ observerLost: true },
			);
		}
		if (reconciled.state === "uncertain" && !reconciled.observerLost) {
			return this.#options.journal.transition(
				reconciled.parentSessionIdentityDigest,
				reconciled.consumerId,
				reconciled.operationId,
				reconciled.requestDigest,
				"uncertain",
				{ observerLost: true },
			);
		}
		return reconciled;
	}

	#reconcile(record: Readonly<ManagedOperationJournalRecordV1>): Readonly<ManagedOperationJournalRecordV1> {
		const terminal = this.#terminal(record);
		if (terminal.state === "terminal" || terminal.state !== record.state) return terminal;
		record = terminal;
		const { parentSessionIdentityDigest: parent, consumerId: consumer, operationId: operation, requestDigest: digest } = record;
		if (record.state === "dispatching" || record.state === "runner-ready") return this.#options.journal.transition(parent, consumer, operation, digest, "uncertain");
		if (record.state === "accepted" && !this.#hasCommitted(record)) {
			this.#options.journal.transition(parent, consumer, operation, digest, "reconciling");
			return this.#options.journal.transition(parent, consumer, operation, digest, "uncertain");
		}
		if (record.state === "uncertain") {
			if (record.observerLost) return record;
			if (record.terminalAsyncDir && readProcessTerminal(record.terminalAsyncDir, { runId: record.runId, runnerProcessInstanceId: record.runnerProcessInstanceId })) return record;
			if (this.#hasCommitted(record)) {
				this.#options.journal.transition(parent, consumer, operation, digest, "reconciling");
				return this.#options.journal.transition(parent, consumer, operation, digest, "accepted");
			}
		}
		if (record.state === "reconciling") return this.#options.journal.transition(parent, consumer, operation, digest, this.#hasCommitted(record) ? "accepted" : "uncertain");
		return record;
	}

	#failCurrent(record: Readonly<ManagedOperationJournalRecordV1>): Readonly<ManagedOperationJournalRecordV1> {
		const { parentSessionIdentityDigest: parent, consumerId: consumer, operationId: operation, requestDigest: digest } = record;
		if (record.state === "claimed" || record.state === "prepared") return this.#options.journal.transition(parent, consumer, operation, digest, "failed-before-launch");
		if (["dispatching", "runner-ready", "accepted", "reconciling"].includes(record.state)) return this.#options.journal.transition(parent, consumer, operation, digest, "uncertain");
		return record;
	}
}

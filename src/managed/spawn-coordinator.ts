import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	SUBAGENT_MANAGED_DISPATCH_VERSION,
	assertManagedConsumerId,
	assertManagedOperationId,
	computeManagedRequestDigest,
	parseManagedMutationRequestV1,
	type JsonObject,
	type ManagedDispatchErrorCodeV1,
	type ManagedDispatchReceiptV1,
	type ManagedExpectedLaunchV1,
	type ManagedSpawnRequestV1,
} from "../api/managed-dispatch.ts";
import {
	managedLaunchRootProjectionsAreCurrent,
	type SubagentLaunchContract,
} from "../api/preflight.ts";
import {
	loadOrCreateManagedDispatchHostId,
	resolveManagedSpawnLaunchV1,
	type ManagedSpawnLaunchResolverOptions,
	type ResolvedManagedSpawnLaunchV1,
} from "../extension/managed-dispatch-preflight.ts";
import {
	ManagedOperationJournal,
	type ManagedOperationJournalRecordV1,
	type ManagedOperationJournalStateV1,
} from "./operation-journal.ts";
import type { ManagedProcessTerminalBindingV1, ProcessTerminalV1 } from "../shared/types.ts";
import {
	computePreparedRunnerAdmissionTokenDigest,
	preparedRunnerAdmissionPaths,
	readPreparedRunnerAdmissionEvidenceForDispatch,
	type PreparedRunnerAdmissionEvidenceV1,
} from "../runs/background/prepared-runner-admission.ts";
import {
	computeManagedProcessTerminalProofDigest,
	readProcessTerminal,
} from "../runs/background/process-terminal.ts";
import { canonicalSessionId } from "../runs/shared/session-lease.ts";
import type {
	PreparedSubagentSpawnOptions,
	PreparedSubagentSpawnPlan,
	SubagentParamsLike,
	createSubagentExecutor,
} from "../runs/foreground/subagent-executor.ts";

export interface ManagedSpawnExecutor {
	executePreparedSpawn: ReturnType<typeof createSubagentExecutor>["executePreparedSpawn"];
}

interface ActiveParentSnapshot {
	ctx: ExtensionContext;
	parentSessionId: string;
	parentSessionFile: string;
	generation: number;
}

export interface ManagedSpawnCoordinatorOptions extends ManagedSpawnLaunchResolverOptions {
	journal: ManagedOperationJournal;
	executor: ManagedSpawnExecutor;
	getContext: () => ExtensionContext | null;
	getSessionGeneration?: () => number;
	hostIdPath?: string;
	loadHostId?: (filePath?: string) => string;
	resolveLaunch?: typeof resolveManagedSpawnLaunchV1;
}

export class ManagedSpawnCoordinatorError extends Error {
	readonly code: ManagedDispatchErrorCodeV1;

	constructor(code: ManagedDispatchErrorCodeV1, message: string) {
		super(message);
		this.name = "ManagedSpawnCoordinatorError";
		this.code = code;
	}
}

const PUBLIC_STATES = new Set<ManagedDispatchReceiptV1["state"]>([
	"claimed",
	"prepared",
	"runner-ready",
	"accepted",
	"terminal",
	"failed-before-launch",
	"uncertain",
	"retired",
]);

function fail(code: ManagedDispatchErrorCodeV1, message: string): never {
	throw new ManagedSpawnCoordinatorError(code, message);
}

function receipt(record: Readonly<ManagedOperationJournalRecordV1>, replayed: boolean): ManagedDispatchReceiptV1 {
	if (!PUBLIC_STATES.has(record.state as ManagedDispatchReceiptV1["state"])) {
		return fail("operation_uncertain", "Managed operation is in a private recovery state.");
	}
	return Object.freeze({
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		consumerId: assertManagedConsumerId(record.consumerId),
		operationId: assertManagedOperationId(record.operationId),
		requestDigest: record.requestDigest,
		state: record.state as ManagedDispatchReceiptV1["state"],
		...(record.runId ? { runId: record.runId } : {}),
		...(record.sourceRunId ? { sourceRunId: record.sourceRunId } : {}),
		replayed,
	});
}

function verifyExpectedLaunch(
	expected: ManagedExpectedLaunchV1,
	resolved: ResolvedManagedSpawnLaunchV1,
	hostId: string,
): void {
	if (expected.hostId !== hostId) fail("host_mismatch", "Managed launch host identity changed.");
	if (!expected.parentSessionIdentityDigest
		|| expected.parentSessionIdentityDigest !== resolved.contract.parentSessionIdentityDigest) {
		fail("host_mismatch", "Managed launch parent-session identity changed.");
	}
	if (expected.candidateRunId !== resolved.contract.runId) {
		fail("contract_changed", "Managed launch candidate identity changed.");
	}
	if (expected.profileIdentityDigest !== resolved.profileIdentityDigest) {
		fail("profile_changed", "Managed launch profile identity changed.");
	}
	if (expected.contractDigest !== resolved.contract.digest) {
		fail("contract_changed", "Managed launch contract identity changed.");
	}
}

function planMatchesContract(plan: Readonly<PreparedSubagentSpawnPlan>, contract: SubagentLaunchContract): boolean {
	const roots = contract.roots;
	return plan.runId === contract.runId
		&& plan.cwd === roots.cwd
		&& plan.sessionRoot === roots.sessionRoot
		&& plan.sessionDir === roots.sessionDir
		&& plan.sessionFile === roots.sessionFile
		&& plan.asyncDir === roots.asyncDir
		&& plan.resultPath === roots.resultPath
		&& plan.resultReservationPath === roots.resultReservationPath
		&& plan.runnerConfigPath === roots.runnerConfigPath
		&& plan.runnerAdmissionPath === roots.runnerAdmissionPath
		&& plan.runnerAdmissionProceedPath === roots.runnerAdmissionProceedPath
		&& plan.runnerAdmissionCommitPath === roots.runnerAdmissionCommitPath
		&& plan.artifactsDir === roots.artifactsDir;
}

function operationKey(parentSessionIdentityDigest: string, request: ManagedSpawnRequestV1): string {
	return `${parentSessionIdentityDigest}\0${request.managed.consumerId}\0${request.managed.operationId}`;
}

/**
 * Unregistered managed spawn coordinator. It is deliberately not an event-bus
 * provider and is not managed capability evidence.
 */
export class ManagedSpawnCoordinator {
	readonly #options: ManagedSpawnCoordinatorOptions;
	readonly #inFlight = new Map<string, {
		requestDigest: string;
		promise: Promise<ManagedDispatchReceiptV1>;
	}>();

	constructor(options: ManagedSpawnCoordinatorOptions) {
		this.#options = options;
	}

	#snapshot(): ActiveParentSnapshot {
		let ctx: ExtensionContext | null;
		let parentSessionId: string | null | undefined;
		let parentSessionFile: string | null | undefined;
		let generation: number;
		try {
			ctx = this.#options.getContext();
			parentSessionId = ctx?.sessionManager.getSessionId();
			parentSessionFile = ctx?.sessionManager.getSessionFile();
			generation = this.#options.getSessionGeneration?.() ?? 0;
		} catch {
			return fail("no_active_session", "Managed spawn requires an active persisted parent session.");
		}
		if (!ctx || !parentSessionId || !parentSessionFile) {
			return fail("no_active_session", "Managed spawn requires an active persisted parent session.");
		}
		return { ctx, parentSessionId, parentSessionFile, generation };
	}

	#snapshotIsCurrent(snapshot: ActiveParentSnapshot): boolean {
		try {
			const current = this.#options.getContext();
			return current?.sessionManager.getSessionId() === snapshot.parentSessionId
				&& current?.sessionManager.getSessionFile() === snapshot.parentSessionFile
				&& (this.#options.getSessionGeneration?.() ?? 0) === snapshot.generation;
		} catch {
			return false;
		}
	}

	async #resolve(
		request: JsonObject,
		candidateRunId: string,
		snapshot: ActiveParentSnapshot,
	): Promise<ResolvedManagedSpawnLaunchV1> {
		const resolveLaunch = this.#options.resolveLaunch ?? resolveManagedSpawnLaunchV1;
		const resolved = await resolveLaunch(
			request,
			candidateRunId,
			snapshot.ctx,
			snapshot.parentSessionId,
			snapshot.parentSessionFile,
			{
				artifactDir: this.#options.artifactDir,
				resolveContract: this.#options.resolveContract,
				resolveCapabilityCeiling: this.#options.resolveCapabilityCeiling,
			},
		);
		if (!this.#snapshotIsCurrent(snapshot)) {
			return fail("no_active_session", "Managed spawn parent session changed during authorization.");
		}
		return resolved;
	}

	async dispatchSpawn(
		payload: unknown,
		signal: AbortSignal = new AbortController().signal,
	): Promise<ManagedDispatchReceiptV1> {
		let request: ManagedSpawnRequestV1;
		try {
			const parsed = parseManagedMutationRequestV1(payload);
			if (parsed.method !== "spawn") return fail("unsupported_method", "Managed spawn coordinator accepts spawn only.");
			request = parsed;
		} catch (error) {
			if (error instanceof ManagedSpawnCoordinatorError) throw error;
			return fail("invalid_request", "Managed spawn request is invalid.");
		}
		const initialSnapshot = this.#snapshot();
		let resolved: ResolvedManagedSpawnLaunchV1;
		try {
			resolved = await this.#resolve(request.input.request, request.expectedLaunch.candidateRunId, initialSnapshot);
		} catch (error) {
			if (error instanceof ManagedSpawnCoordinatorError) throw error;
			return fail("invalid_request", "Managed spawn request or resolved launch profile is invalid.");
		}
		const loadHostId = this.#options.loadHostId ?? loadOrCreateManagedDispatchHostId;
		let hostId: string;
		try {
			hostId = loadHostId(this.#options.hostIdPath);
		} catch {
			return fail("unsupported_host", "Managed dispatch host identity is unavailable.");
		}
		verifyExpectedLaunch(request.expectedLaunch, resolved, hostId);
		const parentDigest = resolved.contract.parentSessionIdentityDigest!;
		const key = operationKey(parentDigest, request);
		const requestDigest = computeManagedRequestDigest(request);
		const active = this.#inFlight.get(key);
		if (active) {
			if (active.requestDigest !== requestDigest) {
				return fail("operation_conflict", "Managed operation identity is already active with different semantics.");
			}
			const activeReceipt = await active.promise;
			return Object.freeze({ ...activeReceipt, replayed: true });
		}
		const operation = this.#claimAndDispatch(request, resolved, initialSnapshot, signal);
		const inFlight = { requestDigest, promise: operation };
		this.#inFlight.set(key, inFlight);
		try {
			return await operation;
		} finally {
			if (this.#inFlight.get(key) === inFlight) this.#inFlight.delete(key);
		}
	}

	async #claimAndDispatch(
		request: ManagedSpawnRequestV1,
		initialResolved: ResolvedManagedSpawnLaunchV1,
		initialSnapshot: ActiveParentSnapshot,
		signal: AbortSignal,
	): Promise<ManagedDispatchReceiptV1> {
		const parentDigest = initialResolved.contract.parentSessionIdentityDigest!;
		const consumerId = request.managed.consumerId;
		const operationId = request.managed.operationId;
		const requestDigest = computeManagedRequestDigest(request);
		const claim = this.#options.journal.claim(parentDigest, request);
		let record = claim.record;
		if (claim.replayed) {
			const reconciled = this.#reconcileReplay(record, requestDigest);
			if (reconciled.state !== "claimed" && reconciled.state !== "prepared") return receipt(reconciled, true);
			record = reconciled;
		}
		if (record.state === "claimed") {
			record = this.#options.journal.transition(parentDigest, consumerId, operationId, requestDigest, "prepared");
		}
		if (record.state !== "prepared") return receipt(record, claim.replayed);

		let authorizedContract: SubagentLaunchContract | undefined;
		const preparedOptions: PreparedSubagentSpawnOptions = {
			runId: request.expectedLaunch.candidateRunId,
			dispatchIdentityDigest: requestDigest,
			beforeLaunch: async (plan) => {
				try {
					const executionSnapshot = this.#snapshot();
					if (executionSnapshot.parentSessionId !== initialSnapshot.parentSessionId
						|| executionSnapshot.parentSessionFile !== initialSnapshot.parentSessionFile
						|| executionSnapshot.generation !== initialSnapshot.generation) {
						return fail("no_active_session", "Managed spawn parent session changed before dispatch.");
					}
					const executionResolved = await this.#resolve(
						request.input.request,
						request.expectedLaunch.candidateRunId,
						executionSnapshot,
					);
					const hostId = (this.#options.loadHostId ?? loadOrCreateManagedDispatchHostId)(this.#options.hostIdPath);
					verifyExpectedLaunch(request.expectedLaunch, executionResolved, hostId);
					if (plan.parentSessionId !== executionSnapshot.parentSessionId
						|| plan.parentSessionFile !== executionSnapshot.parentSessionFile
						|| !planMatchesContract(plan, executionResolved.contract)) {
						return fail("contract_changed", "Prepared spawn plan differs from the authorized launch contract.");
					}
					authorizedContract = executionResolved.contract;
				} catch (error) {
					throw error instanceof ManagedSpawnCoordinatorError
						? error
						: new ManagedSpawnCoordinatorError("execution_failed", "Managed dispatch authorization failed closed.");
				}
			},
			afterAuthorization: (plan) => {
				if (!this.#snapshotIsCurrent(initialSnapshot)) {
					return fail("no_active_session", "Managed spawn parent session changed after authorization.");
				}
				if (!authorizedContract || !managedLaunchRootProjectionsAreCurrent(authorizedContract)) {
					return fail("contract_changed", "Managed launch root identity changed after authorization.");
				}
				const current = this.#options.journal.read(parentDigest, consumerId, operationId);
				if (!current || current.state !== "prepared" || current.runId !== undefined || plan.runId !== authorizedContract.runId) {
					return fail("operation_conflict", "Managed dispatch boundary state changed after authorization.");
				}
				this.#options.journal.transition(parentDigest, consumerId, operationId, requestDigest, "dispatching", {
					runId: request.expectedLaunch.candidateRunId,
					terminalAsyncDir: plan.asyncDir,
					canonicalSessionFile: plan.sessionFile,
				});
				// Recheck after the durable boundary write as well as before it. Node has
				// no portable openat-relative creation, so this narrows rather than
				// eliminates hostile same-UID path replacement (outside the trust model).
				if (!managedLaunchRootProjectionsAreCurrent(authorizedContract)) {
					return fail("contract_changed", "Managed launch root identity changed at the dispatch boundary.");
				}
				return undefined;
			},
			processTerminalBinding: {
				version: 1,
				parentSessionIdentityDigest: parentDigest,
				consumerId,
				operationId,
				requestDigest,
				candidateRunId: request.expectedLaunch.candidateRunId,
			},
			onRunnerReady: (evidence) => {
				this.#assertAdmissionEvidence(evidence, request.expectedLaunch.candidateRunId, requestDigest);
				this.#options.journal.transition(parentDigest, consumerId, operationId, requestDigest, "runner-ready", {
					runId: request.expectedLaunch.candidateRunId,
					runnerProcessInstanceId: evidence.runnerProcessInstanceId,
					runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(evidence.token),
				});
				return undefined;
			},
			onRunnerAccepted: (evidence) => {
				this.#assertAdmissionEvidence(evidence, request.expectedLaunch.candidateRunId, requestDigest);
				const current = this.#options.journal.read(parentDigest, consumerId, operationId);
				if (!current
					|| current.runnerProcessInstanceId !== evidence.runnerProcessInstanceId
					|| current.runnerAdmissionTokenDigest !== computePreparedRunnerAdmissionTokenDigest(evidence.token)) {
					throw new ManagedSpawnCoordinatorError("operation_conflict", "Managed runner admission correlation changed.");
				}
				this.#options.journal.transition(parentDigest, consumerId, operationId, requestDigest, "accepted", {
					runId: request.expectedLaunch.candidateRunId,
				});
				return undefined;
			},
			onProcessTerminal: () => {
				try {
					const current = this.#options.journal.read(parentDigest, consumerId, operationId);
					if (current) this.#reconcileTerminal(current);
				} catch {
					// Durable proof remains available for replay; never throw from a close callback.
				}
				return undefined;
			},
		};

		let executionResult: Awaited<ReturnType<ManagedSpawnExecutor["executePreparedSpawn"]>>;
		try {
			executionResult = await this.#options.executor.executePreparedSpawn(
				request.requestId,
				initialResolved.params as SubagentParamsLike,
				signal,
				undefined,
				initialSnapshot.ctx,
				preparedOptions,
			);
		} catch {
			executionResult = { content: [], isError: true } as Awaited<ReturnType<ManagedSpawnExecutor["executePreparedSpawn"]>>;
		}
		const current = this.#options.journal.read(parentDigest, consumerId, operationId);
		if (!current) return fail("operation_uncertain", "Managed operation disappeared during dispatch.");
		if (executionResult.isError === true) {
			return receipt(this.#failCurrentOperation(current, requestDigest), claim.replayed);
		}
		if (current.state === "terminal") return receipt(current, claim.replayed);
		if (current.state !== "accepted") {
			return receipt(this.#failCurrentOperation(current, requestDigest), claim.replayed);
		}
		if (!this.#hasCommittedAdmission(current)) {
			const uncertain = this.#options.journal.transition(parentDigest, consumerId, operationId, requestDigest, "uncertain");
			return receipt(uncertain, claim.replayed);
		}
		return receipt(current, claim.replayed);
	}

	#assertAdmissionEvidence(
		evidence: Readonly<PreparedRunnerAdmissionEvidenceV1>,
		runId: string,
		dispatchIdentityDigest: string,
	): void {
		if (evidence.runId !== runId || evidence.dispatchIdentityDigest !== dispatchIdentityDigest) {
			return fail("operation_conflict", "Managed runner admission evidence identity changed.");
		}
	}

	#hasCommittedAdmission(record: Readonly<ManagedOperationJournalRecordV1>): boolean {
		if (!record.runId || !record.runnerProcessInstanceId || !record.runnerAdmissionTokenDigest || !record.expectedLaunch) return false;
		try {
			const evidence = readPreparedRunnerAdmissionEvidenceForDispatch(
				record.expectedLaunch.candidateRunId === record.runId && record.terminalAsyncDir
					? preparedRunnerAdmissionPaths(record.terminalAsyncDir).evidencePath
					: "",
				{ runId: record.runId, dispatchIdentityDigest: record.requestDigest },
			);
			return evidence?.state === "committed"
				&& evidence.runnerProcessInstanceId === record.runnerProcessInstanceId
				&& computePreparedRunnerAdmissionTokenDigest(evidence.token) === record.runnerAdmissionTokenDigest;
		} catch {
			return false;
		}
	}

	#managedTerminalBinding(record: Readonly<ManagedOperationJournalRecordV1>): ManagedProcessTerminalBindingV1 | undefined {
		if (!record.runId || !record.runnerAdmissionTokenDigest) return undefined;
		return {
			version: 1,
			parentSessionIdentityDigest: record.parentSessionIdentityDigest,
			consumerId: record.consumerId,
			operationId: record.operationId,
			requestDigest: record.requestDigest,
			candidateRunId: record.runId,
			runnerAdmissionTokenDigest: record.runnerAdmissionTokenDigest,
		};
	}

	#readTerminalProof(record: Readonly<ManagedOperationJournalRecordV1>): ProcessTerminalV1 | undefined {
		if (!record.terminalAsyncDir || !record.runnerProcessInstanceId) return undefined;
		const managed = this.#managedTerminalBinding(record);
		if (!managed) return undefined;
		try {
			return readProcessTerminal(record.terminalAsyncDir, {
				runId: record.runId,
				runnerProcessInstanceId: record.runnerProcessInstanceId,
				managed,
			});
		} catch {
			return undefined;
		}
	}

	#reconcileTerminal(record: Readonly<ManagedOperationJournalRecordV1>): Readonly<ManagedOperationJournalRecordV1> {
		if (record.state === "terminal" || !["accepted", "uncertain", "reconciling"].includes(record.state)) return record;
		if (!record.terminalAsyncDir || !record.canonicalSessionFile || !record.runnerProcessInstanceId || !this.#hasCommittedAdmission(record)) return record;
		const proof = this.#readTerminalProof(record);
		if (!proof) return record;
		const proofIsExact = proof.state === "observed"
			&& proof.runId === record.runId
			&& proof.runnerProcessInstanceId === record.runnerProcessInstanceId
			&& proof.managed !== undefined
			&& proof.canonicalSession?.freeAtObservation === true;
		let expectedCanonicalSessionId: string | undefined;
		try {
			expectedCanonicalSessionId = canonicalSessionId(record.canonicalSessionFile);
		} catch {
			expectedCanonicalSessionId = undefined;
		}
		if (!proofIsExact || !expectedCanonicalSessionId || proof.canonicalSession?.canonicalSessionId !== expectedCanonicalSessionId) {
			if (record.state === "accepted") {
				return this.#options.journal.transition(record.parentSessionIdentityDigest, record.consumerId, record.operationId, record.requestDigest, "uncertain");
			}
			if (record.state === "reconciling") {
				return this.#options.journal.transition(record.parentSessionIdentityDigest, record.consumerId, record.operationId, record.requestDigest, "uncertain");
			}
			return record;
		}
		let current = record;
		if (current.state === "uncertain") {
			current = this.#options.journal.transition(current.parentSessionIdentityDigest, current.consumerId, current.operationId, current.requestDigest, "reconciling");
		}
		return this.#options.journal.transition(current.parentSessionIdentityDigest, current.consumerId, current.operationId, current.requestDigest, "terminal", {
			terminalEvidence: {
				version: 1,
				proofDigest: computeManagedProcessTerminalProofDigest(proof),
				observedAt: proof.observedAt!,
				canonicalSessionId: expectedCanonicalSessionId,
			},
		});
	}

	#reconcileReplay(
		record: Readonly<ManagedOperationJournalRecordV1>,
		requestDigest: string,
	): Readonly<ManagedOperationJournalRecordV1> {
		const durableTerminalProofExists = this.#readTerminalProof(record) !== undefined;
		const terminal = this.#reconcileTerminal(record);
		if (terminal.state === "terminal" || terminal.state !== record.state) return terminal;
		record = terminal;
		const { parentSessionIdentityDigest, consumerId, operationId } = record;
		if (record.state === "dispatching" || record.state === "runner-ready") {
			return this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "uncertain");
		}
		if (record.state === "accepted") {
			if (this.#hasCommittedAdmission(record)) return record;
			const reconciling = this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "reconciling");
			return this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "uncertain", {
				runId: reconciling.runId,
			});
		}
		if (record.state === "uncertain" && !durableTerminalProofExists
			&& record.runnerProcessInstanceId && record.runnerAdmissionTokenDigest && this.#hasCommittedAdmission(record)) {
			this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "reconciling");
			return this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "accepted");
		}
		if (record.state === "reconciling") {
			if (record.runnerProcessInstanceId && record.runnerAdmissionTokenDigest && this.#hasCommittedAdmission(record)) {
				return this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "accepted");
			}
			return this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "uncertain");
		}
		return record;
	}

	#failCurrentOperation(
		record: Readonly<ManagedOperationJournalRecordV1>,
		requestDigest: string,
	): Readonly<ManagedOperationJournalRecordV1> {
		const { parentSessionIdentityDigest, consumerId, operationId } = record;
		if (record.state === "claimed" || record.state === "prepared") {
			return this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "failed-before-launch");
		}
		if (record.state === "dispatching" || record.state === "runner-ready" || record.state === "accepted") {
			return this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "uncertain");
		}
		if (record.state === "reconciling") {
			return this.#options.journal.transition(parentSessionIdentityDigest, consumerId, operationId, requestDigest, "uncertain");
		}
		return record;
	}
}

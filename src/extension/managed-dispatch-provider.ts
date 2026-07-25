import { randomUUID } from "node:crypto";
import * as os from "node:os";
import * as path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT,
	SUBAGENT_MANAGED_DISPATCH_VERSION,
	assertManagedConsumerId,
	assertManagedOperationId,
	managedDispatchReplyEvent,
	parseManagedMutationRequestV1,
	parseManagedPreflightRequestV1,
	parseManagedReadRequestV1,
	type JsonObject,
	type ManagedDispatchCapabilitiesV1,
	type ManagedDispatchErrorCodeV1,
	type ManagedDispatchReplyV1,
	type ManagedOperationDetailsV1,
	type ManagedOperationStatusV1,
	type ManagedOperationTargetV1,
	type ManagedPreflightResultV1,
} from "../api/managed-dispatch.ts";
import { computeParentSessionIdentityDigest } from "../api/preflight.ts";
import { ManagedOperationJournal, ManagedOperationJournalError, type ManagedOperationJournalRecordV1 } from "../managed/operation-journal.ts";
import { ManagedSpawnCoordinator, ManagedSpawnCoordinatorError, type ManagedSpawnExecutor } from "../managed/spawn-coordinator.ts";
import { ManagedResumeCoordinator, ManagedResumeCoordinatorError, type ManagedResumeExecutor } from "../managed/resume-coordinator.ts";
import { resolveManagedResumeLaunchV1, type ManagedResumeLaunchResolverOptions } from "../managed/resume-contract.ts";
import { ManagedResumeSourceError, resolveManagedResumeSourceV1 } from "../managed/resume-source.ts";
import {
	loadOrCreateManagedDispatchHostId,
	performManagedSpawnPreflightV1,
	resolveManagedSpawnLaunchV1,
	type ManagedSpawnLaunchResolverOptions,
} from "./managed-dispatch-preflight.ts";
import { readProcessTerminal } from "../runs/background/process-terminal.ts";
import { canonicalSessionId } from "../runs/shared/session-lease.ts";

interface EventBus {
	on(event: string, handler: (data: unknown) => void): (() => void) | void;
	emit(event: string, data: unknown): void;
}

export interface ManagedDispatchProviderOptions extends ManagedSpawnLaunchResolverOptions, ManagedResumeLaunchResolverOptions {
	events: EventBus;
	executor: ManagedSpawnExecutor & ManagedResumeExecutor;
	getContext: () => ExtensionContext | null;
	getSessionGeneration: () => number;
	journalRoot?: string;
	hostIdPath?: string;
	maxRecoveryRecords?: number;
	resolveLaunch?: typeof resolveManagedSpawnLaunchV1;
	resolveResumeLaunch?: typeof resolveManagedResumeLaunchV1;
	createRunId?: () => string;
}

type ProviderState = "inactive" | "recovering" | "ready" | "unavailable" | "disposed";

function defaultJournalRoot(): string {
	const agentDir = process.env.PI_CODING_AGENT_DIR || path.join(os.homedir(), ".pi", "agent");
	return path.join(agentDir, "subagents", "managed-dispatch", "journal-v1");
}

function ownDataValue(value: unknown, key: string): unknown {
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	const descriptor = Object.getOwnPropertyDescriptor(value, key);
	return descriptor && "value" in descriptor ? descriptor.value : undefined;
}

function safeReplyEvent(payload: unknown): string | undefined {
	const requestId = ownDataValue(payload, "requestId");
	if (typeof requestId !== "string") return undefined;
	try { return managedDispatchReplyEvent(requestId); } catch { return undefined; }
}

function safeMethod(payload: unknown): string | undefined {
	const method = ownDataValue(payload, "method");
	return typeof method === "string" ? method : undefined;
}

function publicState(record: Readonly<ManagedOperationJournalRecordV1>): ManagedOperationStatusV1["state"] {
	return record.state === "dispatching" || record.state === "reconciling" ? "uncertain" : record.state;
}

function sanitizeProcessTerminal(record: Readonly<ManagedOperationJournalRecordV1>): JsonObject | undefined {
	if (!record.terminalAsyncDir || !record.runId || !record.runnerProcessInstanceId || !record.runnerAdmissionTokenDigest) return undefined;
	const managed = {
		version: 1 as const,
		parentSessionIdentityDigest: record.parentSessionIdentityDigest,
		consumerId: record.consumerId,
		operationId: record.operationId,
		requestDigest: record.requestDigest,
		candidateRunId: record.runId,
		runnerAdmissionTokenDigest: record.runnerAdmissionTokenDigest,
		...(record.runnerSessionLeaseTokenDigest ? { sessionLeaseTokenDigest: record.runnerSessionLeaseTokenDigest } : {}),
	};
	const proof = readProcessTerminal(record.terminalAsyncDir, {
		runId: record.runId,
		runnerProcessInstanceId: record.runnerProcessInstanceId,
		managed,
	});
	if (!proof) return undefined;
	return {
		version: 1,
		state: proof.state,
		runId: proof.runId,
		...(proof.observedAt !== undefined ? { observedAt: proof.observedAt } : {}),
		...(proof.resumeDisposition ? { resumeDisposition: proof.resumeDisposition } : {}),
		...(proof.reason ? { reason: proof.reason } : {}),
		...(proof.canonicalSession ? {
			canonicalSession: {
				canonicalSessionId: proof.canonicalSession.canonicalSessionId,
				leaseDisposition: proof.canonicalSession.leaseDisposition,
				freeAtObservation: proof.canonicalSession.freeAtObservation,
				...(proof.canonicalSession.canonicalSessionLeaseReleased ? { canonicalSessionLeaseReleased: true } : {}),
			},
		} : {}),
	};
}

function projectRecord(record: Readonly<ManagedOperationJournalRecordV1>, details: boolean): ManagedOperationStatusV1 | ManagedOperationDetailsV1 {
	const processTerminal = sanitizeProcessTerminal(record);
	let canonicalId = record.terminalEvidence?.canonicalSessionId;
	if (!canonicalId && record.canonicalSessionFile) {
		try { canonicalId = canonicalSessionId(record.canonicalSessionFile); } catch { canonicalId = undefined; }
	}
	const status: ManagedOperationStatusV1 = {
		version: 1,
		consumerId: assertManagedConsumerId(record.consumerId),
		operationId: assertManagedOperationId(record.operationId),
		requestDigest: record.requestDigest,
		state: publicState(record),
		...(record.runId ? { runId: record.runId } : {}),
		...(record.sourceRunId ? { sourceRunId: record.sourceRunId } : {}),
		replayed: true,
		...(["accepted", "terminal", "uncertain", "reconciling"].includes(record.state) ? { runOutcome: "unknown" as const } : {}),
		...(processTerminal ? { processTerminal } : {}),
		...(canonicalId ? {
			child: {
				version: 1,
				index: 0,
				canonicalSessionId: canonicalId,
				resumeDisposition: processTerminal?.resumeDisposition === "resumable" || processTerminal?.resumeDisposition === "non-resumable"
					? processTerminal.resumeDisposition
					: "unavailable",
			},
		} : {}),
	};
	if (!details) return Object.freeze(status);
	return Object.freeze({
		...status,
		...(record.expectedLaunch ? { contractDigest: record.expectedLaunch.contractDigest } : {}),
		createdAt: new Date(record.createdAt).toISOString(),
		updatedAt: new Date(record.updatedAt).toISOString(),
	});
}

function errorCode(error: unknown): ManagedDispatchErrorCodeV1 {
	if (error instanceof ManagedSpawnCoordinatorError || error instanceof ManagedResumeCoordinatorError || error instanceof ManagedResumeSourceError) return error.code;
	if (error instanceof TypeError) return "invalid_request";
	if (error instanceof ManagedOperationJournalError) {
		if (error.code === "busy" || error.code === "corrupt") return "operation_uncertain";
		return error.code;
	}
	return "execution_failed";
}

export class ManagedDispatchProvider {
	readonly #options: ManagedDispatchProviderOptions;
	readonly #unsubscribe?: () => void;
	#journal?: ManagedOperationJournal;
	#coordinator?: ManagedSpawnCoordinator;
	#resumeCoordinator?: ManagedResumeCoordinator;
	#state: ProviderState = "inactive";
	#parentDigest?: string;
	#sessionId?: string;
	#sessionFile?: string;
	#sessionGeneration?: number;
	#hostId?: string;
	#epoch = 0;
	readonly #seenRequestIds = new Set<string>();

	constructor(options: ManagedDispatchProviderOptions) {
		this.#options = options;
		const unsubscribe = options.events.on(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, (payload) => this.#handle(payload));
		if (typeof unsubscribe === "function") this.#unsubscribe = unsubscribe;
	}

	capabilities(): ManagedDispatchCapabilitiesV1 {
		const ready = this.#state === "ready";
		const state = ready ? "ready" : this.#state === "recovering" ? "recovering" : "unavailable";
		return Object.freeze({
			version: 1,
			state,
			available: ready,
			...(this.#hostId ? { hostId: this.#hostId } : {}),
			...(this.#parentDigest ? { parentSessionIdentityDigest: this.#parentDigest } : {}),
			...(this.#sessionGeneration !== undefined ? { sessionGeneration: this.#sessionGeneration } : {}),
			methods: Object.freeze({
				preflight: true,
				spawn: ready,
				status: ready,
				details: ready,
				resume: ready && this.#resumeCoordinator !== undefined,
				steer: false,
				interrupt: false,
				stop: false,
				retire: false,
			}),
			durability: "journal-v1",
			lifecycle: Object.freeze({ version: 3, managedTerminalCorrelation: ready }),
			effects: "not-exactly-once",
		});
	}

	async bindSession(ctx: ExtensionContext, generation: number): Promise<void> {
		this.#closeSession();
		if (this.#state === "disposed") return;
		const epoch = ++this.#epoch;
		let sessionId: string | null | undefined;
		let sessionFile: string | null | undefined;
		try {
			sessionId = ctx.sessionManager.getSessionId();
			sessionFile = ctx.sessionManager.getSessionFile();
		} catch {
			this.#state = "unavailable";
			return;
		}
		if (!sessionId || !sessionFile) {
			this.#state = "unavailable";
			return;
		}
		const parentDigest = computeParentSessionIdentityDigest(sessionId, sessionFile);
		this.#parentDigest = parentDigest;
		this.#sessionId = sessionId;
		this.#sessionFile = sessionFile;
		this.#sessionGeneration = generation;
		this.#state = "recovering";
		try {
			const journal = new ManagedOperationJournal({ root: this.#options.journalRoot ?? defaultJournalRoot() });
			const coordinator = new ManagedSpawnCoordinator({
				journal,
				executor: this.#options.executor,
				getContext: this.#options.getContext,
				getSessionGeneration: this.#options.getSessionGeneration,
				hostIdPath: this.#options.hostIdPath,
				artifactDir: this.#options.artifactDir,
				resolveContract: this.#options.resolveContract,
				resolveCapabilityCeiling: this.#options.resolveCapabilityCeiling,
				resolveLaunch: this.#options.resolveLaunch,
			});
			const resumeCoordinator = new ManagedResumeCoordinator({
				journal,
				executor: this.#options.executor,
				getContext: this.#options.getContext,
				getSessionGeneration: this.#options.getSessionGeneration,
				hostIdPath: this.#options.hostIdPath,
				artifactDir: this.#options.artifactDir,
				resolveContract: this.#options.resolveContract,
				resolveCapabilityCeiling: this.#options.resolveCapabilityCeiling,
				resolveLaunch: this.#options.resolveResumeLaunch,
			});
			this.#journal = journal;
			this.#coordinator = coordinator;
			this.#resumeCoordinator = resumeCoordinator;
			this.#hostId = loadOrCreateManagedDispatchHostId(this.#options.hostIdPath);
			await Promise.resolve();
			let cursor: { consumerId: string; operationId: string } | undefined;
			let recovered = 0;
			const runBindings = new Map<string, string>();
			const maximum = this.#options.maxRecoveryRecords ?? 10_000;
			do {
				if (!this.#isCurrent(epoch)) throw new Error("stale managed recovery");
				const page = journal.list(parentDigest, { limit: 256, ...(cursor ? { after: cursor } : {}) });
				for (const record of page.records) {
					if (++recovered > maximum) throw new ManagedOperationJournalError("busy", "Managed recovery exceeds the bounded active-session limit.");
					if (record.runId) {
						const key = `${record.consumerId}\0${record.runId}`;
						const boundOperationId = runBindings.get(key);
						if (boundOperationId && boundOperationId !== record.operationId) {
							throw new ManagedOperationJournalError("corrupt", "Managed run identity is bound to multiple operations.");
						}
						runBindings.set(key, record.operationId);
					}
					// A newly bound provider cannot inherit the former parent's live close
					// observer. Accepted-without-proof is therefore durably uncertain;
					// it is never inferred terminal and never relaunched.
					if (record.method === "resume") resumeCoordinator.reconcileExisting(record, { observerLost: true });
					else coordinator.reconcileExisting(record, { observerLost: true });
				}
				cursor = page.nextCursor;
				await Promise.resolve();
			} while (cursor);
			if (!this.#isCurrent(epoch)) throw new Error("stale managed recovery");
			this.#state = "ready";
		} catch {
			if (epoch === this.#epoch && this.#state !== "disposed") {
				this.#journal?.close();
				this.#journal = undefined;
				this.#coordinator = undefined;
				this.#resumeCoordinator = undefined;
				this.#state = "unavailable";
			}
		}
	}

	unbindSession(): void {
		if (this.#state === "disposed") return;
		++this.#epoch;
		this.#closeSession();
		this.#state = "inactive";
	}

	dispose(): void {
		if (this.#state === "disposed") return;
		++this.#epoch;
		this.#closeSession();
		this.#state = "disposed";
		this.#unsubscribe?.();
		this.#seenRequestIds.clear();
	}

	#isCurrent(epoch: number): boolean {
		if (epoch !== this.#epoch || this.#state === "disposed") return false;
		try {
			const current = this.#options.getContext();
			return this.#options.getSessionGeneration() === this.#sessionGeneration
				&& current?.sessionManager.getSessionId() === this.#sessionId
				&& current?.sessionManager.getSessionFile() === this.#sessionFile;
		} catch { return false; }
	}

	#closeSession(): void {
		this.#journal?.close();
		this.#journal = undefined;
		this.#coordinator = undefined;
		this.#resumeCoordinator = undefined;
		this.#parentDigest = undefined;
		this.#sessionId = undefined;
		this.#sessionFile = undefined;
		this.#sessionGeneration = undefined;
		this.#hostId = undefined;
		this.#seenRequestIds.clear();
	}

	#resolveTarget(target: ManagedOperationTargetV1): Readonly<ManagedOperationJournalRecordV1> | undefined {
		if (!this.#journal || !this.#parentDigest) return undefined;
		return "operationId" in target
			? this.#journal.read(this.#parentDigest, target.consumerId, target.operationId)
			: this.#journal.readByRun(this.#parentDigest, target.consumerId, target.runId);
	}

	#emit<T>(replyEvent: string, reply: ManagedDispatchReplyV1<T>): void {
		if (this.#state !== "disposed") this.#options.events.emit(replyEvent, reply);
	}

	#handle(payload: unknown): void {
		const method = safeMethod(payload);
		if (!method || !["preflight", "capabilities", "spawn", "resume", "status", "details"].includes(method)) return;
		const replyEvent = safeReplyEvent(payload);
		const requestId = ownDataValue(payload, "requestId");
		if (!replyEvent || typeof requestId !== "string" || this.#seenRequestIds.has(requestId)) return;
		this.#seenRequestIds.add(requestId);
		if (this.#seenRequestIds.size > 10_000) {
			const oldest = this.#seenRequestIds.values().next().value as string | undefined;
			if (oldest !== undefined) this.#seenRequestIds.delete(oldest);
		}
		const requestEpoch = this.#epoch;
		const dispatched = method === "preflight" ? this.#preflight(payload, requestEpoch) : this.#dispatch(payload, method);
		void dispatched
			.then((data) => {
				if (requestEpoch !== this.#epoch) return;
				if (method === "preflight") {
					this.#options.events.emit(replyEvent, data);
					return;
				}
				this.#emit(replyEvent, { version: 1, requestId, method: method as "capabilities" | "spawn" | "resume" | "status" | "details", success: true, data });
			})
			.catch((error) => {
				if (requestEpoch !== this.#epoch) return;
				if (method === "preflight") {
					this.#options.events.emit(replyEvent, {
						version: SUBAGENT_MANAGED_DISPATCH_VERSION,
						ok: false,
						code: errorCode(error),
						message: "Managed preflight failed closed.",
					} satisfies ManagedPreflightResultV1);
					return;
				}
				this.#emit(replyEvent, {
					version: 1,
					requestId,
					method: method as "capabilities" | "spawn" | "resume" | "status" | "details",
					success: false,
					error: { code: errorCode(error), message: "Managed dispatch request failed closed." },
				});
			});
	}

	async #preflight(payload: unknown, epoch: number): Promise<ManagedPreflightResultV1> {
		let request;
		try {
			request = parseManagedPreflightRequestV1(payload);
		} catch {
			return { version: 1, ok: false, code: "invalid_request", message: "Managed preflight request is invalid." };
		}
		if (request.input.kind === "spawn") {
			return performManagedSpawnPreflightV1(payload, {
				events: this.#options.events,
				getContext: this.#options.getContext,
				getSessionGeneration: this.#options.getSessionGeneration,
				artifactDir: this.#options.artifactDir,
				hostIdPath: this.#options.hostIdPath,
				createRunId: this.#options.createRunId,
				resolveContract: this.#options.resolveContract,
				resolveCapabilityCeiling: this.#options.resolveCapabilityCeiling,
			});
		}
		if (this.#state !== "ready" || !this.#journal || !this.#resumeCoordinator || !this.#parentDigest) {
			return { version: 1, ok: false, code: "unsupported_host", message: "Managed resume preflight is unavailable." };
		}
		let ctx: ExtensionContext | null;
		let parentSessionId: string | null | undefined;
		let parentSessionFile: string | null | undefined;
		try {
			ctx = this.#options.getContext();
			parentSessionId = ctx?.sessionManager.getSessionId();
			parentSessionFile = ctx?.sessionManager.getSessionFile();
		} catch {
			return { version: 1, ok: false, code: "no_active_session", message: "Managed resume preflight requires an active persisted parent session." };
		}
		if (!ctx || !parentSessionId || !parentSessionFile || !this.#isCurrent(epoch)) {
			return { version: 1, ok: false, code: "no_active_session", message: "Managed resume preflight requires an active persisted parent session." };
		}
		try {
			const source = resolveManagedResumeSourceV1({
				journal: this.#journal,
				parentSessionIdentityDigest: this.#parentDigest,
				consumerId: request.consumerId,
				sourceRunId: request.input.sourceRunId,
				index: request.input.index,
			});
			const candidateRunId = (this.#options.createRunId ?? randomUUID)();
			const resolved = await (this.#options.resolveResumeLaunch ?? resolveManagedResumeLaunchV1)(
				request.input.request,
				candidateRunId,
				source,
				ctx,
				parentSessionId,
				parentSessionFile,
				{
					artifactDir: this.#options.artifactDir,
					resolveContract: this.#options.resolveContract,
					resolveCapabilityCeiling: this.#options.resolveCapabilityCeiling,
				},
			);
			if (!this.#isCurrent(epoch)) {
				return { version: 1, ok: false, code: "no_active_session", message: "Managed resume preflight parent session changed before completion." };
			}
			return {
				version: 1,
				ok: true,
				host: { version: 1, hostId: loadOrCreateManagedDispatchHostId(this.#options.hostIdPath) },
				profile: resolved.profile,
				profileIdentityDigest: resolved.profileIdentityDigest,
				parentSessionIdentityDigest: resolved.contract.parentSessionIdentityDigest,
				candidateRunId,
				contractDigest: resolved.contract.digest,
			};
		} catch (error) {
			return { version: 1, ok: false, code: errorCode(error), message: "Managed resume preflight failed closed." };
		}
	}

	async #dispatch(payload: unknown, method: string): Promise<unknown> {
		if (method === "capabilities") {
			parseManagedReadRequestV1(payload);
			return this.capabilities();
		}
		if (this.#state !== "ready" || !this.#journal || !this.#coordinator || !this.#resumeCoordinator || !this.#parentDigest) {
			throw new ManagedSpawnCoordinatorError("unsupported_host", this.#state === "recovering" ? "Managed provider is recovering." : "Managed provider is unavailable.");
		}
		const epoch = this.#epoch;
		if (method === "spawn" || method === "resume") {
			const request = parseManagedMutationRequestV1(payload);
			const result = method === "spawn" && request.method === "spawn"
				? await this.#coordinator.dispatchSpawn(request)
				: method === "resume" && request.method === "resume"
					? await this.#resumeCoordinator.dispatchResume(request)
					: (() => { throw new ManagedSpawnCoordinatorError("unsupported_method", "Managed provider method is unavailable."); })();
			if (!this.#isCurrent(epoch)) throw new ManagedSpawnCoordinatorError("no_active_session", "Managed parent session changed during dispatch.");
			return result;
		}
		const request = parseManagedReadRequestV1(payload);
		if (request.method !== method || (request.method !== "status" && request.method !== "details")) {
			throw new ManagedSpawnCoordinatorError("invalid_request", "Managed read request is invalid.");
		}
		const record = this.#resolveTarget(request.target);
		if (!record) throw new ManagedSpawnCoordinatorError("not_found", "Managed operation was not found.");
		const reconciled = record.method === "resume"
			? this.#resumeCoordinator.reconcileExisting(record)
			: this.#coordinator.reconcileExisting(record);
		if (!this.#isCurrent(epoch)) throw new ManagedSpawnCoordinatorError("no_active_session", "Managed parent session changed during read.");
		return projectRecord(reconciled, method === "details");
	}
}

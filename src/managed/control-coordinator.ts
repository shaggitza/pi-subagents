import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	SUBAGENT_MANAGED_DISPATCH_VERSION,
	assertManagedConsumerId,
	assertManagedOperationId,
	computeManagedRequestDigest,
	parseManagedMutationRequestV1,
	type ManagedControlRequestV1,
	type ManagedDispatchErrorCodeV1,
	type ManagedDispatchReceiptV1,
} from "../api/managed-dispatch.ts";
import { computeParentSessionIdentityDigest } from "../api/preflight.ts";
import { readStatus } from "../shared/utils.ts";
import {
	inspectManagedControlTransport,
	managedControlAckPath,
	managedControlRequestPath,
	publishManagedControlRequest,
	readManagedControlAck,
} from "../runs/background/control-channel.ts";
import { ManagedOperationJournal, type ManagedOperationJournalRecordV1 } from "./operation-journal.ts";

interface ActiveParentSnapshot {
	ctx: ExtensionContext;
	parentSessionId: string;
	parentSessionFile: string;
	parentSessionIdentityDigest: string;
	generation: number;
}

export interface ManagedControlCoordinatorOptions {
	journal: ManagedOperationJournal;
	getContext: () => ExtensionContext | null;
	getSessionGeneration?: () => number;
	publishRequest?: typeof publishManagedControlRequest;
}

export class ManagedControlCoordinatorError extends Error {
	readonly code: ManagedDispatchErrorCodeV1;
	constructor(code: ManagedDispatchErrorCodeV1, message: string) {
		super(message);
		this.name = "ManagedControlCoordinatorError";
		this.code = code;
	}
}

function fail(code: ManagedDispatchErrorCodeV1, message: string): never {
	throw new ManagedControlCoordinatorError(code, message);
}

function receipt(record: Readonly<ManagedOperationJournalRecordV1>, replayed: boolean): ManagedDispatchReceiptV1 {
	const state = record.state === "dispatching" || record.state === "reconciling" ? "uncertain" : record.state;
	if (state !== "claimed" && state !== "prepared" && state !== "accepted" && state !== "terminal"
		&& state !== "failed-before-launch" && state !== "uncertain" && state !== "retired" && state !== "runner-ready") {
		return fail("operation_uncertain", "Managed command is in a private recovery state.");
	}
	return Object.freeze({
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		consumerId: assertManagedConsumerId(record.consumerId),
		operationId: assertManagedOperationId(record.operationId),
		requestDigest: record.requestDigest,
		state,
		...(record.actorRunId ? { runId: record.actorRunId } : {}),
		replayed,
	});
}

/**
 * Unregistered managed control/retirement authority. The event provider deliberately
 * does not expose this coordinator until a separate availability review enables it.
 */
export class ManagedControlCoordinator {
	readonly #options: ManagedControlCoordinatorOptions;
	readonly #inFlight = new Map<string, { digest: string; promise: Promise<ManagedDispatchReceiptV1> }>();
	readonly #actorQueues = new Map<string, Promise<void>>();

	constructor(options: ManagedControlCoordinatorOptions) {
		this.#options = options;
	}

	#snapshot(): ActiveParentSnapshot {
		try {
			const ctx = this.#options.getContext();
			const parentSessionId = ctx?.sessionManager.getSessionId();
			const parentSessionFile = ctx?.sessionManager.getSessionFile();
			if (!ctx || !parentSessionId || !parentSessionFile) return fail("no_active_session", "Managed control requires an active persisted parent session.");
			return {
				ctx,
				parentSessionId,
				parentSessionFile,
				parentSessionIdentityDigest: computeParentSessionIdentityDigest(parentSessionId, parentSessionFile),
				generation: this.#options.getSessionGeneration?.() ?? 0,
			};
		} catch (error) {
			if (error instanceof ManagedControlCoordinatorError) throw error;
			return fail("no_active_session", "Managed control requires an active persisted parent session.");
		}
	}

	#isCurrent(snapshot: ActiveParentSnapshot): boolean {
		try {
			const current = this.#options.getContext();
			return current?.sessionManager.getSessionId() === snapshot.parentSessionId
				&& current?.sessionManager.getSessionFile() === snapshot.parentSessionFile
				&& (this.#options.getSessionGeneration?.() ?? 0) === snapshot.generation;
		} catch { return false; }
	}

	#resolveActor(snapshot: ActiveParentSnapshot, request: ManagedControlRequestV1): Readonly<ManagedOperationJournalRecordV1> {
		const target = request.input.target;
		const actor = "operationId" in target
			? this.#options.journal.read(snapshot.parentSessionIdentityDigest, request.managed.consumerId, target.operationId)
			: this.#options.journal.readByRun(snapshot.parentSessionIdentityDigest, request.managed.consumerId, target.runId);
		if (!actor) return fail("not_found", "Managed actor was not found in this parent and consumer namespace.");
		if (actor.method !== "spawn" && actor.method !== "resume") return fail("invalid_state", "Managed commands cannot target command records.");
		if (actor.state === "retired") return fail("retired", "Managed actor is retired.");
		return actor;
	}

	#validateActiveControl(actor: Readonly<ManagedOperationJournalRecordV1>, method: "steer" | "interrupt" | "stop"): void {
		if (actor.state !== "accepted" || !actor.runId || !actor.terminalAsyncDir) return fail("invalid_state", "Managed control requires an accepted launch actor.");
		let status;
		try { status = readStatus(actor.terminalAsyncDir); } catch { return fail("operation_uncertain", "Managed actor status is unreadable."); }
		if (!status || status.runId !== actor.runId) return fail("operation_uncertain", "Managed actor status identity is missing or inconsistent.");
		if (method === "interrupt" && status.state !== "running") return fail("invalid_state", "Managed interrupt requires an exactly running actor.");
		if ((method === "steer" || method === "stop") && status.state !== "running" && status.state !== "queued") {
			return fail("invalid_state", `Managed ${method} requires a queued or running actor.`);
		}
	}

	async #serializeActor<T>(key: string, action: () => Promise<T>): Promise<T> {
		const previous = this.#actorQueues.get(key) ?? Promise.resolve();
		let release!: () => void;
		const current = new Promise<void>((resolve) => { release = resolve; });
		const queued = previous.then(() => current);
		this.#actorQueues.set(key, queued);
		await previous;
		try { return await action(); } finally {
			release();
			if (this.#actorQueues.get(key) === queued) this.#actorQueues.delete(key);
		}
	}

	async dispatchControl(payload: unknown): Promise<ManagedDispatchReceiptV1> {
		let request: ManagedControlRequestV1;
		try {
			const parsed = parseManagedMutationRequestV1(payload);
			if (parsed.method === "spawn" || parsed.method === "resume") return fail("unsupported_method", "Managed control coordinator accepts controls and retirement only.");
			request = parsed;
		} catch (error) {
			if (error instanceof ManagedControlCoordinatorError) throw error;
			return fail("invalid_request", "Managed control request is invalid.");
		}
		const snapshot = this.#snapshot();
		const digest = computeManagedRequestDigest(request);
		const operationKey = `${snapshot.parentSessionIdentityDigest}\0${request.managed.consumerId}\0${request.managed.operationId}`;
		const active = this.#inFlight.get(operationKey);
		if (active) {
			if (active.digest !== digest) return fail("operation_conflict", "Managed command is active with different semantics.");
			return Object.freeze({ ...(await active.promise), replayed: true });
		}
		const durable = this.#options.journal.read(snapshot.parentSessionIdentityDigest, request.managed.consumerId, request.managed.operationId);
		if (durable && (durable.method !== request.method || durable.requestDigest !== digest)) {
			return fail("operation_conflict", "Managed operation identity is already bound to different semantics.");
		}
		if (durable?.method === "retire" && durable.state === "terminal") return receipt(durable, true);
		const actorOperationId = durable?.method === "retire" && durable.state === "prepared" && durable.actorOperationId
			? durable.actorOperationId
			: this.#resolveActor(snapshot, request).operationId;
		const actorKey = `${snapshot.parentSessionIdentityDigest}\0${request.managed.consumerId}\0${actorOperationId}`;
		const promise = this.#serializeActor(actorKey, () => this.#dispatchSerialized(snapshot, request, digest));
		const entry = { digest, promise };
		this.#inFlight.set(operationKey, entry);
		try { return await promise; } finally { if (this.#inFlight.get(operationKey) === entry) this.#inFlight.delete(operationKey); }
	}

	async #dispatchSerialized(snapshot: ActiveParentSnapshot, request: ManagedControlRequestV1, digest: string): Promise<ManagedDispatchReceiptV1> {
		if (!this.#isCurrent(snapshot)) return fail("no_active_session", "Managed parent session changed before command claim.");
		const parent = snapshot.parentSessionIdentityDigest;
		const consumer = request.managed.consumerId;
		const commandId = request.managed.operationId;
		const claim = this.#options.journal.claim(parent, request);
		let command = claim.record;
		if (claim.replayed && command.state !== "claimed" && command.state !== "prepared") {
			return receipt(this.#reconcileTransport(command), true);
		}
		if (request.method === "retire" && command.state === "prepared") {
			return receipt(this.#options.journal.completeRetirement(parent, consumer, commandId, digest).command, true);
		}
		const actor = this.#resolveActor(snapshot, request);
		if (command.state === "claimed") {
			if (request.method !== "retire") this.#validateActiveControl(actor, request.method);
			command = this.#options.journal.prepareControl(parent, consumer, commandId, digest, {
				operationId: actor.operationId,
				requestDigest: actor.requestDigest,
				...(actor.runId ? { runId: actor.runId } : {}),
			}, request.method === "retire" && request.input.acknowledgeUncertain ? { acknowledgeUncertain: true } : {});
		}
		if (request.method === "retire") {
			const completed = this.#options.journal.completeRetirement(parent, consumer, commandId, digest);
			return receipt(completed.command, claim.replayed);
		}
		if (command.state !== "prepared" || !command.actorRunId || !actor.terminalAsyncDir) return receipt(command, claim.replayed);
		this.#validateActiveControl(actor, request.method);
		if (!this.#isCurrent(snapshot)) return fail("no_active_session", "Managed parent session changed before command publication.");
		const requestPath = managedControlRequestPath(actor.terminalAsyncDir, commandId);
		const ackPath = managedControlAckPath(actor.terminalAsyncDir, commandId);
		command = this.#options.journal.beginControlDispatch(parent, consumer, commandId, digest, { requestPath, ackPath });
		try {
			(this.#options.publishRequest ?? publishManagedControlRequest)(actor.terminalAsyncDir, {
				commandId,
				method: request.method,
				runId: command.actorRunId,
				...(request.method === "steer" ? { message: request.input.message } : {}),
			});
		} catch {
			return receipt(this.#reconcileTransport(command), claim.replayed);
		}
		command = this.#options.journal.acceptControl(parent, consumer, commandId, digest);
		if (!this.#isCurrent(snapshot)) return fail("no_active_session", "Managed parent session changed after command publication.");
		return receipt(this.#reconcileTransport(command), claim.replayed);
	}

	#reconcileTransport(command: Readonly<ManagedOperationJournalRecordV1>): Readonly<ManagedOperationJournalRecordV1> {
		if (command.method === "retire" || command.state === "terminal" || command.state === "uncertain" || command.state === "claimed" || command.state === "prepared") return command;
		if (!command.actorOperationId || !command.actorRequestDigest || !command.actorRunId || !command.controlRequestPath || !command.controlAckPath) {
			return this.#options.journal.markControlUncertain(command.parentSessionIdentityDigest, command.consumerId, command.operationId, command.requestDigest);
		}
		const actor = this.#options.journal.read(command.parentSessionIdentityDigest, command.consumerId, command.actorOperationId);
		if (!actor || (actor.method !== "spawn" && actor.method !== "resume") || actor.requestDigest !== command.actorRequestDigest
			|| actor.runId !== command.actorRunId || !actor.terminalAsyncDir
			|| command.controlRequestPath !== managedControlRequestPath(actor.terminalAsyncDir, command.operationId)
			|| command.controlAckPath !== managedControlAckPath(actor.terminalAsyncDir, command.operationId)) {
			return this.#options.journal.markControlUncertain(command.parentSessionIdentityDigest, command.consumerId, command.operationId, command.requestDigest);
		}
		const asyncDir = actor.terminalAsyncDir;
		const state = inspectManagedControlTransport(asyncDir, command.operationId);
		if (state === "acknowledged" || state === "failed") {
			const ack = readManagedControlAck(asyncDir, command.operationId);
			if (!ack || ack.commandId !== command.operationId || ack.method !== command.method || ack.runId !== command.actorRunId) {
				return this.#options.journal.markControlUncertain(command.parentSessionIdentityDigest, command.consumerId, command.operationId, command.requestDigest);
			}
			return this.#options.journal.completeControl(command.parentSessionIdentityDigest, command.consumerId, command.operationId, command.requestDigest, ack.outcome);
		}
		if (state === "published") {
			return command.state === "dispatching"
				? this.#options.journal.acceptControl(command.parentSessionIdentityDigest, command.consumerId, command.operationId, command.requestDigest)
				: command;
		}
		return this.#options.journal.markControlUncertain(command.parentSessionIdentityDigest, command.consumerId, command.operationId, command.requestDigest);
	}
}

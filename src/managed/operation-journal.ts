import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import {
	assertManagedConsumerId,
	assertManagedOperationId,
	computeManagedRequestDigest,
	parseManagedMutationRequestV1,
	type ManagedDispatchErrorCodeV1,
	type ManagedExpectedLaunchV1,
	type ManagedMutationMethodV1,
	type ManagedOperationStateV1,
} from "../api/managed-dispatch.ts";

export const MANAGED_OPERATION_JOURNAL_VERSION = 1 as const;

export type ManagedOperationJournalStateV1 = ManagedOperationStateV1 | "dispatching" | "reconciling";

export interface ManagedOperationJournalRecordV1 {
	version: typeof MANAGED_OPERATION_JOURNAL_VERSION;
	parentSessionIdentityDigest: string;
	consumerId: string;
	operationId: string;
	requestDigest: string;
	method: ManagedMutationMethodV1;
	state: ManagedOperationJournalStateV1;
	expectedLaunch?: ManagedExpectedLaunchV1;
	runId?: string;
	sourceRunId?: string;
	createdAt: number;
	updatedAt: number;
}

export interface ManagedOperationClaimResult {
	created: boolean;
	replayed: boolean;
	record: Readonly<ManagedOperationJournalRecordV1>;
}

export class ManagedOperationJournalError extends Error {
	readonly code: ManagedDispatchErrorCodeV1 | "busy" | "corrupt";

	constructor(code: ManagedOperationJournalError["code"], message: string) {
		super(message);
		this.name = "ManagedOperationJournalError";
		this.code = code;
	}
}

export interface ManagedOperationJournalOptions {
	root: string;
	now?: () => number;
	pid?: number;
	processStartFingerprint?: string;
}

interface OwnerRecordV1 {
	version: 1;
	token: string;
	pid: number;
	processStartFingerprint?: string;
	createdAt: number;
}

const SHA256 = /^[a-f0-9]{64}$/;
const SAFE_RUN_ID = /^[A-Za-z0-9][A-Za-z0-9._~:-]{0,255}$/;
const OWNER_DIR = ".owner";
const OWNER_FILE = "owner.json";
const RECORD_FILE = "record.json";

const states = (...values: ManagedOperationJournalStateV1[]): ReadonlySet<ManagedOperationJournalStateV1> => new Set(values);

const TRANSITIONS: Readonly<Record<ManagedOperationJournalStateV1, ReadonlySet<ManagedOperationJournalStateV1>>> = Object.freeze({
	claimed: states("prepared", "failed-before-launch", "retired"),
	prepared: states("dispatching", "failed-before-launch", "retired"),
	dispatching: states("runner-ready", "accepted", "uncertain"),
	"runner-ready": states("accepted", "uncertain"),
	accepted: states("terminal", "uncertain", "reconciling"),
	terminal: states("retired"),
	"failed-before-launch": states("retired"),
	uncertain: states("reconciling", "retired"),
	reconciling: states("accepted", "terminal", "uncertain"),
	retired: states(),
});

function assertDigest(value: unknown, label: string): string {
	if (typeof value !== "string" || !SHA256.test(value)) throw new ManagedOperationJournalError("invalid_request", `${label} is invalid.`);
	return value;
}

function assertTimestamp(value: unknown, label: string): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new ManagedOperationJournalError("corrupt", `${label} is invalid.`);
	return value;
}

function assertRunId(value: unknown, label: string): string {
	if (typeof value !== "string" || !SAFE_RUN_ID.test(value)) throw new ManagedOperationJournalError("invalid_request", `${label} is invalid.`);
	return value;
}

function processStartFingerprint(pid: number): string | undefined {
	try {
		const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8").trim();
		const commandEnd = stat.lastIndexOf(")");
		if (commandEnd < 0) return undefined;
		const fieldsAfterCommand = stat.slice(commandEnd + 1).trim().split(/\s+/);
		// Field 3 (state) is index 0 here; Linux proc field 22 (starttime) is index 19.
		return fieldsAfterCommand[19] || undefined;
	} catch {
		return undefined;
	}
}

function pidAppearsAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return (error as NodeJS.ErrnoException).code === "EPERM";
	}
}

function ownerIsActive(owner: OwnerRecordV1): boolean {
	if (!pidAppearsAlive(owner.pid)) return false;
	if (!owner.processStartFingerprint) return true;
	const observed = processStartFingerprint(owner.pid);
	return observed === undefined || observed === owner.processStartFingerprint;
}

function parseOwner(value: unknown): OwnerRecordV1 {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid owner");
	const record = value as Record<string, unknown>;
	const keys = Object.keys(record).sort();
	const allowed = record.processStartFingerprint === undefined
		? ["createdAt", "pid", "token", "version"]
		: ["createdAt", "pid", "processStartFingerprint", "token", "version"];
	if (keys.join("\0") !== allowed.sort().join("\0")) throw new Error("invalid owner keys");
	if (record.version !== 1 || typeof record.token !== "string" || !SAFE_RUN_ID.test(record.token)) throw new Error("invalid owner identity");
	if (typeof record.pid !== "number" || !Number.isSafeInteger(record.pid) || record.pid <= 0) throw new Error("invalid owner pid");
	if (record.processStartFingerprint !== undefined && typeof record.processStartFingerprint !== "string") throw new Error("invalid owner start");
	if (typeof record.createdAt !== "number" || !Number.isSafeInteger(record.createdAt) || record.createdAt < 0) throw new Error("invalid owner time");
	return record as unknown as OwnerRecordV1;
}

function readJson(filePath: string): unknown {
	const stats = fs.lstatSync(filePath);
	if (!stats.isFile() || stats.isSymbolicLink() || stats.size > 1_048_576) throw new Error("durable JSON file is not a bounded regular file");
	return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function fsyncDirectory(directory: string): void {
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(directory, "r");
		fs.fsyncSync(descriptor);
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
	}
}

function assertOwnedDirectory(directory: string, label: string, privateMode: boolean): void {
	const stats = fs.lstatSync(directory);
	if (!stats.isDirectory() || stats.isSymbolicLink()) throw new ManagedOperationJournalError("invalid_state", `${label} must be a real directory.`);
	const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
	if (uid !== undefined && stats.uid !== uid) throw new ManagedOperationJournalError("invalid_state", `${label} must be owned by the current user.`);
	const forbidden = privateMode ? 0o077 : 0o022;
	if ((stats.mode & forbidden) !== 0) throw new ManagedOperationJournalError("invalid_state", `${label} permissions are unsafe.`);
}

function assertNoSymlinkPathComponents(absolutePath: string): void {
	const parsed = path.parse(absolutePath);
	let current = parsed.root;
	for (const segment of absolutePath.slice(parsed.root.length).split(path.sep).filter(Boolean)) {
		current = path.join(current, segment);
		try {
			const stats = fs.lstatSync(current);
			if (stats.isSymbolicLink()) throw new ManagedOperationJournalError("invalid_state", "Managed journal path must not traverse symlinks.");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
			throw error;
		}
	}
}

function createDurableJournalRoot(rootInput: string): string {
	const absoluteRoot = path.resolve(rootInput);
	assertNoSymlinkPathComponents(absoluteRoot);
	const missing: string[] = [];
	let ancestor = absoluteRoot;
	for (;;) {
		try {
			fs.lstatSync(ancestor);
			break;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			const parent = path.dirname(ancestor);
			if (parent === ancestor) throw error;
			missing.unshift(path.basename(ancestor));
			ancestor = parent;
		}
	}
	assertOwnedDirectory(ancestor, "Managed journal ancestor", false);
	let current = ancestor;
	for (const segment of missing) {
		const next = path.join(current, segment);
		try {
			fs.mkdirSync(next, { mode: 0o700 });
			fsyncDirectory(current);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
		}
		assertOwnedDirectory(next, "Managed journal directory", true);
		fsyncDirectory(next);
		current = next;
	}
	assertOwnedDirectory(absoluteRoot, "Managed journal root", true);
	fsyncDirectory(path.dirname(absoluteRoot));
	fsyncDirectory(absoluteRoot);
	return fs.realpathSync(absoluteRoot);
}

function ensureDurablePrivateChild(parent: string, name: string): string {
	const child = path.join(parent, name);
	try {
		fs.mkdirSync(child, { mode: 0o700 });
		fsyncDirectory(parent);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
	}
	assertOwnedDirectory(child, "Managed journal namespace", true);
	fsyncDirectory(parent);
	fsyncDirectory(child);
	return child;
}

function existingPrivateChild(parent: string, name: string): string | undefined {
	const child = path.join(parent, name);
	try {
		assertOwnedDirectory(child, "Managed journal namespace", true);
		return child;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
}

function writeJsonDurable(filePath: string, value: unknown): void {
	const temporary = path.join(path.dirname(filePath), `.${path.basename(filePath)}.${process.pid}.${randomUUID()}.tmp`);
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(temporary, "wx", 0o600);
		fs.writeFileSync(descriptor, `${JSON.stringify(value)}\n`, "utf8");
		fs.fsyncSync(descriptor);
		fs.closeSync(descriptor);
		descriptor = undefined;
		fs.renameSync(temporary, filePath);
		fsyncDirectory(path.dirname(filePath));
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
		fs.rmSync(temporary, { force: true });
	}
}

function parseExpectedLaunch(value: unknown): ManagedExpectedLaunchV1 {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new ManagedOperationJournalError("corrupt", "Managed expected launch is corrupt.");
	const record = value as Record<string, unknown>;
	const required = ["candidateRunId", "contractDigest", "hostId", "parentSessionIdentityDigest", "profileIdentityDigest", "version"];
	if (Object.keys(record).sort().join("\0") !== required.sort().join("\0") || record.version !== 1) {
		throw new ManagedOperationJournalError("corrupt", "Managed expected launch is corrupt.");
	}
	return {
		version: 1,
		hostId: assertRunId(record.hostId, "Managed expected host"),
		candidateRunId: assertRunId(record.candidateRunId, "Managed expected candidate run"),
		profileIdentityDigest: assertDigest(record.profileIdentityDigest, "Managed expected profile digest"),
		parentSessionIdentityDigest: assertDigest(record.parentSessionIdentityDigest, "Managed expected parent-session digest"),
		contractDigest: assertDigest(record.contractDigest, "Managed expected contract digest"),
	};
}

function parseRecordUnchecked(value: unknown): ManagedOperationJournalRecordV1 {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new ManagedOperationJournalError("corrupt", "Managed operation record is corrupt.");
	const record = value as Record<string, unknown>;
	const allowed = new Set([
		"version", "parentSessionIdentityDigest", "consumerId", "operationId", "requestDigest", "method", "state",
		"expectedLaunch", "runId", "sourceRunId", "createdAt", "updatedAt",
	]);
	if (Object.keys(record).some((key) => !allowed.has(key))) throw new ManagedOperationJournalError("corrupt", "Managed operation record has unknown fields.");
	if (record.version !== MANAGED_OPERATION_JOURNAL_VERSION) throw new ManagedOperationJournalError("corrupt", "Managed operation record version is unsupported.");
	const consumerId = assertManagedConsumerId(record.consumerId);
	const operationId = assertManagedOperationId(record.operationId);
	const method = record.method;
	if (!(["spawn", "resume", "steer", "interrupt", "stop", "retire"] as unknown[]).includes(method)) {
		throw new ManagedOperationJournalError("corrupt", "Managed operation method is corrupt.");
	}
	const state = record.state;
	if (typeof state !== "string" || !(state in TRANSITIONS)) throw new ManagedOperationJournalError("corrupt", "Managed operation state is corrupt.");
	const parsed: ManagedOperationJournalRecordV1 = {
		version: MANAGED_OPERATION_JOURNAL_VERSION,
		parentSessionIdentityDigest: assertDigest(record.parentSessionIdentityDigest, "Managed parent-session digest"),
		consumerId,
		operationId,
		requestDigest: assertDigest(record.requestDigest, "Managed request digest"),
		method: method as ManagedMutationMethodV1,
		state: state as ManagedOperationJournalStateV1,
		createdAt: assertTimestamp(record.createdAt, "Managed operation createdAt"),
		updatedAt: assertTimestamp(record.updatedAt, "Managed operation updatedAt"),
	};
	if (record.expectedLaunch !== undefined) parsed.expectedLaunch = parseExpectedLaunch(record.expectedLaunch);
	if (record.runId !== undefined) parsed.runId = assertRunId(record.runId, "Managed run id");
	if (record.sourceRunId !== undefined) parsed.sourceRunId = assertRunId(record.sourceRunId, "Managed source run id");
	const launchMethod = parsed.method === "spawn" || parsed.method === "resume";
	if (launchMethod && !parsed.expectedLaunch) {
		throw new ManagedOperationJournalError("corrupt", "Managed launch operation lacks expected launch identity.");
	}
	if (!launchMethod && parsed.expectedLaunch) {
		throw new ManagedOperationJournalError("corrupt", "Managed control operation contains launch identity.");
	}
	if (parsed.expectedLaunch?.parentSessionIdentityDigest !== undefined
		&& parsed.expectedLaunch.parentSessionIdentityDigest !== parsed.parentSessionIdentityDigest) {
		throw new ManagedOperationJournalError("corrupt", "Managed launch parent identity differs from its namespace.");
	}
	if (parsed.runId && parsed.expectedLaunch && parsed.runId !== parsed.expectedLaunch.candidateRunId) {
		throw new ManagedOperationJournalError("corrupt", "Managed run identity differs from its preflight candidate.");
	}
	if (parsed.method === "resume" ? !parsed.sourceRunId : parsed.sourceRunId !== undefined) {
		throw new ManagedOperationJournalError("corrupt", "Managed source run identity is inconsistent with the operation method.");
	}
	if (parsed.updatedAt < parsed.createdAt) {
		throw new ManagedOperationJournalError("corrupt", "Managed operation chronology is invalid.");
	}
	const runRequired = ["dispatching", "runner-ready", "accepted", "terminal", "uncertain", "reconciling"].includes(parsed.state);
	const runForbidden = ["claimed", "prepared", "failed-before-launch"].includes(parsed.state);
	if ((runRequired && !parsed.runId) || (runForbidden && parsed.runId)) {
		throw new ManagedOperationJournalError("corrupt", "Managed run identity is inconsistent with the operation state.");
	}
	return Object.freeze(parsed);
}

function parseRecord(value: unknown): ManagedOperationJournalRecordV1 {
	try {
		return parseRecordUnchecked(value);
	} catch {
		throw new ManagedOperationJournalError("corrupt", "Managed operation record is corrupt.");
	}
}

function cloneExpectedLaunch(value: ManagedExpectedLaunchV1): ManagedExpectedLaunchV1 {
	if (!value.parentSessionIdentityDigest) throw new ManagedOperationJournalError("invalid_request", "Managed launch requires parent-session identity.");
	return parseExpectedLaunch({ ...value });
}

export class ManagedOperationJournal {
	readonly root: string;
	readonly ownerToken: string;
	readonly #now: () => number;
	readonly #pid: number;
	readonly #fingerprint?: string;
	#closed = false;

	constructor(options: ManagedOperationJournalOptions) {
		this.#now = options.now ?? Date.now;
		this.#pid = options.pid ?? process.pid;
		this.#fingerprint = options.processStartFingerprint ?? processStartFingerprint(this.#pid);
		this.root = createDurableJournalRoot(options.root);
		this.ownerToken = randomUUID();
		this.#acquireOwner();
	}

	#assertOpen(): void {
		if (this.#closed) throw new ManagedOperationJournalError("invalid_state", "Managed operation journal is closed.");
	}

	#acquireOwner(): void {
		const ownerDir = path.join(this.root, OWNER_DIR);
		for (let attempt = 0; attempt < 3; attempt++) {
			const temporary = path.join(this.root, `.owner.${this.#pid}.${randomUUID()}.tmp`);
			try {
				fs.mkdirSync(temporary, { mode: 0o700 });
				writeJsonDurable(path.join(temporary, OWNER_FILE), {
					version: 1,
					token: this.ownerToken,
					pid: this.#pid,
					...(this.#fingerprint ? { processStartFingerprint: this.#fingerprint } : {}),
					createdAt: this.#now(),
				} satisfies OwnerRecordV1);
				try {
					fs.renameSync(temporary, ownerDir);
					fsyncDirectory(this.root);
					return;
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code !== "EEXIST" && (error as NodeJS.ErrnoException).code !== "ENOTEMPTY") throw error;
				}
			} finally {
				fs.rmSync(temporary, { recursive: true, force: true });
			}
			let owner: OwnerRecordV1;
			try {
				owner = parseOwner(readJson(path.join(ownerDir, OWNER_FILE)));
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code === "ENOENT" && !fs.existsSync(ownerDir)) continue;
				// Without a valid PID/start fingerprint, automatic takeover could duplicate a live owner.
				throw new ManagedOperationJournalError("busy", "Managed journal owner record is corrupt; explicit operator repair is required.");
			}
			if (ownerIsActive(owner)) throw new ManagedOperationJournalError("busy", "Managed journal already has a live owner.");
			// The observed-token tombstone is retained. A second contender that inspected
			// the same dead owner cannot rename a newly installed live owner over it.
			const stale = path.join(this.root, `.owner.stale.${owner.token}`);
			try {
				fs.renameSync(ownerDir, stale);
				fsyncDirectory(this.root);
			} catch (error) {
				const code = (error as NodeJS.ErrnoException).code;
				if (code !== "ENOENT" && code !== "EEXIST" && code !== "ENOTEMPTY") throw error;
			}
		}
		throw new ManagedOperationJournalError("busy", "Managed journal ownership could not be acquired.");
	}

	#consumerDirectory(parentSessionIdentityDigest: string, consumerId: string): string {
		const operations = ensureDurablePrivateChild(this.root, "operations");
		const session = ensureDurablePrivateChild(operations, parentSessionIdentityDigest);
		return ensureDurablePrivateChild(session, consumerId);
	}

	#existingOperationDirectory(
		parentSessionIdentityDigest: string,
		consumerId: string,
		operationId: string,
	): string | undefined {
		const operations = existingPrivateChild(this.root, "operations");
		if (!operations) return undefined;
		const session = existingPrivateChild(operations, parentSessionIdentityDigest);
		if (!session) return undefined;
		const consumer = existingPrivateChild(session, consumerId);
		if (!consumer) return undefined;
		return existingPrivateChild(consumer, operationId);
	}

	#readOperationRecord(
		operationDirectory: string,
		parentSessionIdentityDigest: string,
		consumerId: string,
		operationId: string,
	): ManagedOperationJournalRecordV1 {
		const record = parseRecord(readJson(path.join(operationDirectory, RECORD_FILE)));
		if (
			record.parentSessionIdentityDigest !== parentSessionIdentityDigest
			|| record.consumerId !== consumerId
			|| record.operationId !== operationId
		) {
			throw new ManagedOperationJournalError("corrupt", "Managed operation record does not match its durable namespace.");
		}
		return record;
	}

	claim(parentSessionIdentityDigest: string, request: unknown): ManagedOperationClaimResult {
		this.#assertOpen();
		const sessionDigest = assertDigest(parentSessionIdentityDigest, "Managed parent-session identity digest");
		const parsedRequest = parseManagedMutationRequestV1(request);
		const consumerId = assertManagedConsumerId(parsedRequest.managed.consumerId);
		const operationId = assertManagedOperationId(parsedRequest.managed.operationId);
		const requestDigest = computeManagedRequestDigest(parsedRequest);
		const expectedLaunch = parsedRequest.method === "spawn" || parsedRequest.method === "resume"
			? cloneExpectedLaunch(parsedRequest.expectedLaunch)
			: undefined;
		if (expectedLaunch && expectedLaunch.parentSessionIdentityDigest !== sessionDigest) {
			throw new ManagedOperationJournalError("host_mismatch", "Managed launch parent-session identity does not match the active journal namespace.");
		}
		const consumerDirectory = this.#consumerDirectory(sessionDigest, consumerId);
		let operationDirectory = existingPrivateChild(consumerDirectory, operationId);
		if (!operationDirectory) {
			const temporaryPrefix = `.operation.${operationId}.`;
			for (const entry of fs.readdirSync(consumerDirectory)) {
				if (!entry.startsWith(temporaryPrefix) || !entry.endsWith(".tmp")) continue;
				fs.rmSync(path.join(consumerDirectory, entry), { recursive: true, force: true });
			}
			fsyncDirectory(consumerDirectory);
			const timestamp = this.#now();
			const record: ManagedOperationJournalRecordV1 = {
				version: MANAGED_OPERATION_JOURNAL_VERSION,
				parentSessionIdentityDigest: sessionDigest,
				consumerId,
				operationId,
				requestDigest,
				method: parsedRequest.method,
				state: "claimed",
				...(expectedLaunch ? { expectedLaunch } : {}),
				...(parsedRequest.method === "resume" ? { sourceRunId: parsedRequest.input.sourceRunId } : {}),
				createdAt: timestamp,
				updatedAt: timestamp,
			};
			const temporary = path.join(consumerDirectory, `${temporaryPrefix}${randomUUID()}.tmp`);
			try {
				fs.mkdirSync(temporary, { mode: 0o700 });
				writeJsonDurable(path.join(temporary, RECORD_FILE), record);
				try {
					fs.renameSync(temporary, path.join(consumerDirectory, operationId));
					fsyncDirectory(consumerDirectory);
					return { created: true, replayed: false, record: parseRecord(record) };
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code !== "EEXIST" && (error as NodeJS.ErrnoException).code !== "ENOTEMPTY") throw error;
				}
			} finally {
				fs.rmSync(temporary, { recursive: true, force: true });
			}
			operationDirectory = existingPrivateChild(consumerDirectory, operationId);
		}
		if (!operationDirectory) throw new ManagedOperationJournalError("operation_uncertain", "Managed operation publication outcome is uncertain.");
		let existing: ManagedOperationJournalRecordV1;
		try {
			existing = this.#readOperationRecord(operationDirectory, sessionDigest, consumerId, operationId);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") {
				throw new ManagedOperationJournalError("operation_uncertain", "Managed operation directory exists without a durable claim record.");
			}
			throw error;
		}
		if (
			existing.parentSessionIdentityDigest !== sessionDigest
			|| existing.consumerId !== consumerId
			|| existing.operationId !== operationId
			|| existing.requestDigest !== requestDigest
			|| existing.method !== parsedRequest.method
		) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed operation identity is already bound to different semantics.");
		}
		return { created: false, replayed: true, record: existing };
	}

	transition(
		parentSessionIdentityDigest: string,
		consumerIdInput: string,
		operationIdInput: string,
		requestDigestInput: string,
		nextState: ManagedOperationJournalStateV1,
		patch: { runId?: string; sourceRunId?: string } = {},
	): Readonly<ManagedOperationJournalRecordV1> {
		this.#assertOpen();
		const sessionDigest = assertDigest(parentSessionIdentityDigest, "Managed parent-session identity digest");
		const consumerId = assertManagedConsumerId(consumerIdInput);
		const operationId = assertManagedOperationId(operationIdInput);
		const requestDigest = assertDigest(requestDigestInput, "Managed request digest");
		if (!(nextState in TRANSITIONS)) throw new ManagedOperationJournalError("invalid_state", "Managed target state is invalid.");
		const operationDirectory = this.#existingOperationDirectory(sessionDigest, consumerId, operationId);
		if (!operationDirectory) throw new ManagedOperationJournalError("not_found", "Managed operation was not found.");
		const recordPath = path.join(operationDirectory, RECORD_FILE);
		let existing: ManagedOperationJournalRecordV1;
		try {
			existing = this.#readOperationRecord(operationDirectory, sessionDigest, consumerId, operationId);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") throw new ManagedOperationJournalError("not_found", "Managed operation was not found.");
			throw error;
		}
		if (existing.requestDigest !== requestDigest) throw new ManagedOperationJournalError("operation_conflict", "Managed request digest does not match the durable operation.");
		const patchedRunId = patch.runId !== undefined ? assertRunId(patch.runId, "Managed run id") : undefined;
		const patchedSourceRunId = patch.sourceRunId !== undefined ? assertRunId(patch.sourceRunId, "Managed source run id") : undefined;
		if (patchedSourceRunId !== undefined && existing.method !== "resume") {
			throw new ManagedOperationJournalError("invalid_state", "Managed source run identity is valid only for resume operations.");
		}
		if (patchedRunId !== undefined && existing.runId !== undefined && patchedRunId !== existing.runId) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed run identity is immutable.");
		}
		if (patchedRunId !== undefined && existing.expectedLaunch && patchedRunId !== existing.expectedLaunch.candidateRunId) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed run identity differs from the preflight candidate.");
		}
		if (patchedSourceRunId !== undefined && existing.sourceRunId !== undefined && patchedSourceRunId !== existing.sourceRunId) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed source run identity is immutable.");
		}
		if (existing.state === nextState) {
			if (patchedRunId !== undefined && existing.runId === undefined) {
				throw new ManagedOperationJournalError("invalid_state", "Managed run identity was not durably bound with the state transition.");
			}
			return existing;
		}
		const effectiveRunId = existing.runId ?? patchedRunId;
		if (["dispatching", "runner-ready", "accepted", "terminal", "uncertain", "reconciling"].includes(nextState) && !effectiveRunId) {
			throw new ManagedOperationJournalError("invalid_state", `Managed state ${nextState} requires a durable run identity.`);
		}
		if (["claimed", "prepared", "failed-before-launch"].includes(nextState) && effectiveRunId) {
			throw new ManagedOperationJournalError("invalid_state", `Managed state ${nextState} cannot contain a run identity.`);
		}
		if (!TRANSITIONS[existing.state].has(nextState)) {
			throw new ManagedOperationJournalError("invalid_state", `Managed operation cannot transition from ${existing.state} to ${nextState}.`);
		}
		const record: ManagedOperationJournalRecordV1 = {
			...existing,
			state: nextState,
			...(patchedRunId !== undefined ? { runId: patchedRunId } : {}),
			...(patchedSourceRunId !== undefined ? { sourceRunId: patchedSourceRunId } : {}),
			updatedAt: Math.max(existing.updatedAt, this.#now()),
		};
		const validatedRecord = parseRecord(record);
		writeJsonDurable(recordPath, validatedRecord);
		return validatedRecord;
	}

	read(parentSessionIdentityDigest: string, consumerIdInput: string, operationIdInput: string): Readonly<ManagedOperationJournalRecordV1> | undefined {
		this.#assertOpen();
		const sessionDigest = assertDigest(parentSessionIdentityDigest, "Managed parent-session identity digest");
		const consumerId = assertManagedConsumerId(consumerIdInput);
		const operationId = assertManagedOperationId(operationIdInput);
		const operationDirectory = this.#existingOperationDirectory(sessionDigest, consumerId, operationId);
		if (!operationDirectory) return undefined;
		try {
			return this.#readOperationRecord(operationDirectory, sessionDigest, consumerId, operationId);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
			throw error;
		}
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		const ownerDir = path.join(this.root, OWNER_DIR);
		try {
			const owner = parseOwner(readJson(path.join(ownerDir, OWNER_FILE)));
			if (owner.token !== this.ownerToken) return;
			fs.rmSync(ownerDir, { recursive: true, force: true });
			fsyncDirectory(this.root);
		} catch {
			// Fail closed: never remove an owner record that cannot be attributed to this instance.
		}
	}
}

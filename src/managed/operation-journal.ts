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

export interface ManagedOperationTerminalEvidenceV1 {
	version: 1;
	proofDigest: string;
	observedAt: number;
	canonicalSessionId: string;
}

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
	/** Host-derived immutable authority for exact managed resume. */
	sourceOperationId?: string;
	sourceRequestDigest?: string;
	sourceTerminalProofDigest?: string;
	sourceCanonicalSessionId?: string;
	sourceRecoveryDescriptorDigest?: string;
	runnerProcessInstanceId?: string;
	runnerAdmissionTokenDigest?: string;
	/** Bound only by the future combined prepared-resume admission seam. */
	runnerSessionLeaseTokenDigest?: string;
	runnerCanonicalSessionId?: string;
	/** Host-authorized paths retained for exact admission/terminal recovery. */
	terminalAsyncDir?: string;
	canonicalSessionFile?: string;
	terminalEvidence?: ManagedOperationTerminalEvidenceV1;
	createdAt: number;
	updatedAt: number;
}

export interface ManagedOperationClaimResult {
	created: boolean;
	replayed: boolean;
	record: Readonly<ManagedOperationJournalRecordV1>;
}

export interface ManagedOperationJournalCursorV1 {
	consumerId: string;
	operationId: string;
}

export interface ManagedOperationJournalPageV1 {
	records: ReadonlyArray<Readonly<ManagedOperationJournalRecordV1>>;
	nextCursor?: ManagedOperationJournalCursorV1;
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
	dispatching: states("runner-ready", "uncertain"),
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

function assertAbsolutePath(value: unknown, label: string): string {
	if (typeof value !== "string" || value.length === 0 || value.length > 4096 || value.includes("\0") || !path.isAbsolute(value) || path.resolve(value) !== value) {
		throw new ManagedOperationJournalError("invalid_request", `${label} is invalid.`);
	}
	return value;
}

function parseTerminalEvidence(value: unknown): ManagedOperationTerminalEvidenceV1 {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new ManagedOperationJournalError("corrupt", "Managed terminal evidence is corrupt.");
	const record = value as Record<string, unknown>;
	const keys = Object.keys(record).sort();
	if (keys.join("\0") !== ["canonicalSessionId", "observedAt", "proofDigest", "version"].sort().join("\0") || record.version !== 1) {
		throw new ManagedOperationJournalError("corrupt", "Managed terminal evidence is corrupt.");
	}
	return {
		version: 1,
		proofDigest: assertDigest(record.proofDigest, "Managed terminal proof digest"),
		observedAt: assertTimestamp(record.observedAt, "Managed terminal observedAt"),
		canonicalSessionId: assertDigest(record.canonicalSessionId, "Managed canonical session id"),
	};
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
		"expectedLaunch", "runId", "sourceRunId", "sourceOperationId", "sourceRequestDigest", "sourceTerminalProofDigest",
		"sourceCanonicalSessionId", "sourceRecoveryDescriptorDigest", "runnerProcessInstanceId", "runnerAdmissionTokenDigest",
		"runnerSessionLeaseTokenDigest", "runnerCanonicalSessionId", "terminalAsyncDir", "canonicalSessionFile", "terminalEvidence", "createdAt", "updatedAt",
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
	if (record.sourceOperationId !== undefined) parsed.sourceOperationId = assertManagedOperationId(record.sourceOperationId);
	if (record.sourceRequestDigest !== undefined) parsed.sourceRequestDigest = assertDigest(record.sourceRequestDigest, "Managed source request digest");
	if (record.sourceTerminalProofDigest !== undefined) parsed.sourceTerminalProofDigest = assertDigest(record.sourceTerminalProofDigest, "Managed source terminal proof digest");
	if (record.sourceCanonicalSessionId !== undefined) parsed.sourceCanonicalSessionId = assertDigest(record.sourceCanonicalSessionId, "Managed source canonical session id");
	if (record.sourceRecoveryDescriptorDigest !== undefined) parsed.sourceRecoveryDescriptorDigest = assertDigest(record.sourceRecoveryDescriptorDigest, "Managed source recovery descriptor digest");
	if (record.runnerProcessInstanceId !== undefined) {
		parsed.runnerProcessInstanceId = assertRunId(record.runnerProcessInstanceId, "Managed runner process instance id");
	}
	if (record.runnerAdmissionTokenDigest !== undefined) {
		parsed.runnerAdmissionTokenDigest = assertDigest(record.runnerAdmissionTokenDigest, "Managed runner admission token digest");
	}
	if (record.runnerSessionLeaseTokenDigest !== undefined) parsed.runnerSessionLeaseTokenDigest = assertDigest(record.runnerSessionLeaseTokenDigest, "Managed runner session lease token digest");
	if (record.runnerCanonicalSessionId !== undefined) parsed.runnerCanonicalSessionId = assertDigest(record.runnerCanonicalSessionId, "Managed runner canonical session id");
	if (record.terminalAsyncDir !== undefined) parsed.terminalAsyncDir = assertAbsolutePath(record.terminalAsyncDir, "Managed terminal async directory");
	if (record.canonicalSessionFile !== undefined) parsed.canonicalSessionFile = assertAbsolutePath(record.canonicalSessionFile, "Managed canonical session file");
	if (record.terminalEvidence !== undefined) parsed.terminalEvidence = parseTerminalEvidence(record.terminalEvidence);
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
	const resumeSourceFields = [parsed.sourceOperationId, parsed.sourceRequestDigest, parsed.sourceTerminalProofDigest, parsed.sourceCanonicalSessionId, parsed.sourceRecoveryDescriptorDigest];
	const hasCompleteResumeSource = resumeSourceFields.every((field) => field !== undefined);
	const hasAnyResumeSource = resumeSourceFields.some((field) => field !== undefined);
	const resumeSourceRequired = parsed.method === "resume" && !["claimed", "failed-before-launch"].includes(parsed.state);
	const resumeSourceOptional = parsed.method === "resume" && parsed.state === "failed-before-launch";
	if ((parsed.method !== "resume" && hasAnyResumeSource) || (resumeSourceRequired && !hasCompleteResumeSource)
		|| (!resumeSourceRequired && !resumeSourceOptional && hasAnyResumeSource) || (resumeSourceOptional && hasAnyResumeSource && !hasCompleteResumeSource)) {
		throw new ManagedOperationJournalError("corrupt", "Managed resume source correlation is inconsistent with the operation state.");
	}
	const hasLeaseDigest = parsed.runnerSessionLeaseTokenDigest !== undefined;
	const hasRunnerCanonical = parsed.runnerCanonicalSessionId !== undefined;
	if (hasLeaseDigest !== hasRunnerCanonical || (parsed.method !== "resume" && hasLeaseDigest)) {
		throw new ManagedOperationJournalError("corrupt", "Managed resume lease correlation is incomplete.");
	}
	if (hasRunnerCanonical && parsed.sourceCanonicalSessionId !== parsed.runnerCanonicalSessionId) {
		throw new ManagedOperationJournalError("corrupt", "Managed resume runner canonical session differs from its source.");
	}
	if (parsed.method === "resume" && ["runner-ready", "accepted", "terminal"].includes(parsed.state) && !hasLeaseDigest) {
		throw new ManagedOperationJournalError("corrupt", "Managed resume runner lease correlation is missing.");
	}
	if (["claimed", "prepared", "dispatching", "failed-before-launch"].includes(parsed.state) && hasLeaseDigest) {
		throw new ManagedOperationJournalError("corrupt", "Managed resume runner lease correlation is premature.");
	}
	if (parsed.updatedAt < parsed.createdAt) {
		throw new ManagedOperationJournalError("corrupt", "Managed operation chronology is invalid.");
	}
	const runRequired = ["dispatching", "runner-ready", "accepted", "terminal", "uncertain", "reconciling"].includes(parsed.state);
	const runForbidden = ["claimed", "prepared", "failed-before-launch"].includes(parsed.state);
	if ((runRequired && !parsed.runId) || (runForbidden && parsed.runId)) {
		throw new ManagedOperationJournalError("corrupt", "Managed run identity is inconsistent with the operation state.");
	}
	const hasRunnerInstance = parsed.runnerProcessInstanceId !== undefined;
	const hasAdmissionToken = parsed.runnerAdmissionTokenDigest !== undefined;
	if (hasRunnerInstance !== hasAdmissionToken) {
		throw new ManagedOperationJournalError("corrupt", "Managed runner admission correlation is incomplete.");
	}
	if (["runner-ready", "accepted", "terminal"].includes(parsed.state) && !hasRunnerInstance) {
		throw new ManagedOperationJournalError("corrupt", "Managed runner admission correlation is missing.");
	}
	if (["claimed", "prepared", "dispatching", "failed-before-launch"].includes(parsed.state) && hasRunnerInstance) {
		throw new ManagedOperationJournalError("corrupt", "Managed runner admission correlation is premature.");
	}
	const pathsRequired = ["dispatching", "runner-ready", "accepted", "terminal", "uncertain", "reconciling"].includes(parsed.state);
	const pathsForbidden = ["claimed", "prepared", "failed-before-launch"].includes(parsed.state);
	const hasBothPaths = parsed.terminalAsyncDir !== undefined && parsed.canonicalSessionFile !== undefined;
	if ((pathsRequired && !hasBothPaths) || (pathsForbidden && (parsed.terminalAsyncDir !== undefined || parsed.canonicalSessionFile !== undefined))) {
		throw new ManagedOperationJournalError("corrupt", "Managed terminal recovery paths are inconsistent with the operation state.");
	}
	if ((parsed.state === "terminal") !== (parsed.terminalEvidence !== undefined)) {
		throw new ManagedOperationJournalError("corrupt", "Managed terminal evidence is inconsistent with the operation state.");
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
		patch: {
			runId?: string;
			sourceRunId?: string;
			sourceOperationId?: string;
			sourceRequestDigest?: string;
			sourceTerminalProofDigest?: string;
			sourceCanonicalSessionId?: string;
			sourceRecoveryDescriptorDigest?: string;
			runnerProcessInstanceId?: string;
			runnerAdmissionTokenDigest?: string;
			runnerSessionLeaseTokenDigest?: string;
			runnerCanonicalSessionId?: string;
			terminalAsyncDir?: string;
			canonicalSessionFile?: string;
			terminalEvidence?: ManagedOperationTerminalEvidenceV1;
		} = {},
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
		const patchedSourceOperationId = patch.sourceOperationId !== undefined ? assertManagedOperationId(patch.sourceOperationId) : undefined;
		const patchedSourceRequestDigest = patch.sourceRequestDigest !== undefined ? assertDigest(patch.sourceRequestDigest, "Managed source request digest") : undefined;
		const patchedSourceTerminalProofDigest = patch.sourceTerminalProofDigest !== undefined ? assertDigest(patch.sourceTerminalProofDigest, "Managed source terminal proof digest") : undefined;
		const patchedSourceCanonicalSessionId = patch.sourceCanonicalSessionId !== undefined ? assertDigest(patch.sourceCanonicalSessionId, "Managed source canonical session id") : undefined;
		const patchedSourceRecoveryDescriptorDigest = patch.sourceRecoveryDescriptorDigest !== undefined ? assertDigest(patch.sourceRecoveryDescriptorDigest, "Managed source recovery descriptor digest") : undefined;
		const patchedRunnerInstance = patch.runnerProcessInstanceId !== undefined
			? assertRunId(patch.runnerProcessInstanceId, "Managed runner process instance id")
			: undefined;
		const patchedAdmissionTokenDigest = patch.runnerAdmissionTokenDigest !== undefined
			? assertDigest(patch.runnerAdmissionTokenDigest, "Managed runner admission token digest")
			: undefined;
		const patchedRunnerSessionLeaseTokenDigest = patch.runnerSessionLeaseTokenDigest !== undefined
			? assertDigest(patch.runnerSessionLeaseTokenDigest, "Managed runner session lease token digest")
			: undefined;
		const patchedRunnerCanonicalSessionId = patch.runnerCanonicalSessionId !== undefined
			? assertDigest(patch.runnerCanonicalSessionId, "Managed runner canonical session id")
			: undefined;
		const patchedTerminalAsyncDir = patch.terminalAsyncDir !== undefined
			? assertAbsolutePath(patch.terminalAsyncDir, "Managed terminal async directory")
			: undefined;
		const patchedCanonicalSessionFile = patch.canonicalSessionFile !== undefined
			? assertAbsolutePath(patch.canonicalSessionFile, "Managed canonical session file")
			: undefined;
		const patchedTerminalEvidence = patch.terminalEvidence !== undefined
			? parseTerminalEvidence(patch.terminalEvidence)
			: undefined;
		if (patchedSourceRunId !== undefined && existing.method !== "resume") {
			throw new ManagedOperationJournalError("invalid_state", "Managed source run identity is valid only for resume operations.");
		}
		const patchedResumeSource = [patchedSourceOperationId, patchedSourceRequestDigest, patchedSourceTerminalProofDigest, patchedSourceCanonicalSessionId, patchedSourceRecoveryDescriptorDigest];
		if (patchedResumeSource.some((field) => field !== undefined) && (existing.method !== "resume" || !patchedResumeSource.every((field) => field !== undefined) || nextState !== "prepared")) {
			throw new ManagedOperationJournalError("invalid_state", "Managed resume source correlation must bind atomically at prepared.");
		}
		if ((patchedRunnerSessionLeaseTokenDigest === undefined) !== (patchedRunnerCanonicalSessionId === undefined)
			|| (patchedRunnerSessionLeaseTokenDigest !== undefined && (existing.method !== "resume" || nextState !== "runner-ready"))) {
			throw new ManagedOperationJournalError("invalid_state", "Managed resume lease correlation must bind atomically at runner-ready.");
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
		for (const [existingValue, patchedValue] of [
			[existing.sourceOperationId, patchedSourceOperationId],
			[existing.sourceRequestDigest, patchedSourceRequestDigest],
			[existing.sourceTerminalProofDigest, patchedSourceTerminalProofDigest],
			[existing.sourceCanonicalSessionId, patchedSourceCanonicalSessionId],
			[existing.sourceRecoveryDescriptorDigest, patchedSourceRecoveryDescriptorDigest],
			[existing.runnerSessionLeaseTokenDigest, patchedRunnerSessionLeaseTokenDigest],
			[existing.runnerCanonicalSessionId, patchedRunnerCanonicalSessionId],
		] as const) {
			if (existingValue !== undefined && patchedValue !== undefined && existingValue !== patchedValue) {
				throw new ManagedOperationJournalError("operation_conflict", "Managed resume correlation is immutable.");
			}
		}
		if (patchedRunnerInstance !== undefined && existing.runnerProcessInstanceId !== undefined
			&& patchedRunnerInstance !== existing.runnerProcessInstanceId) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed runner process instance identity is immutable.");
		}
		if (patchedAdmissionTokenDigest !== undefined && existing.runnerAdmissionTokenDigest !== undefined
			&& patchedAdmissionTokenDigest !== existing.runnerAdmissionTokenDigest) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed runner admission token identity is immutable.");
		}
		if ((patchedRunnerInstance === undefined) !== (patchedAdmissionTokenDigest === undefined)) {
			throw new ManagedOperationJournalError("invalid_state", "Managed runner admission correlation must be bound atomically.");
		}
		if ((patchedTerminalAsyncDir === undefined) !== (patchedCanonicalSessionFile === undefined)) {
			throw new ManagedOperationJournalError("invalid_state", "Managed terminal recovery paths must be bound atomically.");
		}
		if (patchedTerminalAsyncDir !== undefined && existing.terminalAsyncDir !== undefined && patchedTerminalAsyncDir !== existing.terminalAsyncDir) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed terminal async directory is immutable.");
		}
		if (patchedCanonicalSessionFile !== undefined && existing.canonicalSessionFile !== undefined && patchedCanonicalSessionFile !== existing.canonicalSessionFile) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed canonical session file is immutable.");
		}
		if (patchedTerminalEvidence !== undefined && existing.terminalEvidence !== undefined
			&& JSON.stringify(patchedTerminalEvidence) !== JSON.stringify(existing.terminalEvidence)) {
			throw new ManagedOperationJournalError("operation_conflict", "Managed terminal evidence is immutable.");
		}
		if (existing.terminalAsyncDir === undefined && patchedTerminalAsyncDir !== undefined && nextState !== "dispatching") {
			throw new ManagedOperationJournalError("invalid_state", "Managed terminal recovery paths may first bind only at dispatching.");
		}
		if (patchedTerminalEvidence !== undefined && nextState !== "terminal") {
			throw new ManagedOperationJournalError("invalid_state", "Managed terminal evidence may bind only at terminal.");
		}
		if (existing.runnerProcessInstanceId === undefined && patchedRunnerInstance !== undefined && nextState !== "runner-ready") {
			throw new ManagedOperationJournalError("invalid_state", "Managed runner admission correlation may first bind only at runner-ready.");
		}
		if (existing.state === nextState) {
			if (patchedRunId !== undefined && existing.runId === undefined) {
				throw new ManagedOperationJournalError("invalid_state", "Managed run identity was not durably bound with the state transition.");
			}
			return existing;
		}
		const effectiveRunId = existing.runId ?? patchedRunId;
		const effectiveRunnerInstance = existing.runnerProcessInstanceId ?? patchedRunnerInstance;
		const effectiveAdmissionTokenDigest = existing.runnerAdmissionTokenDigest ?? patchedAdmissionTokenDigest;
		const effectiveSourceOperationId = existing.sourceOperationId ?? patchedSourceOperationId;
		const effectiveSourceRequestDigest = existing.sourceRequestDigest ?? patchedSourceRequestDigest;
		const effectiveSourceTerminalProofDigest = existing.sourceTerminalProofDigest ?? patchedSourceTerminalProofDigest;
		const effectiveSourceCanonicalSessionId = existing.sourceCanonicalSessionId ?? patchedSourceCanonicalSessionId;
		const effectiveSourceRecoveryDescriptorDigest = existing.sourceRecoveryDescriptorDigest ?? patchedSourceRecoveryDescriptorDigest;
		const effectiveRunnerSessionLeaseTokenDigest = existing.runnerSessionLeaseTokenDigest ?? patchedRunnerSessionLeaseTokenDigest;
		const effectiveRunnerCanonicalSessionId = existing.runnerCanonicalSessionId ?? patchedRunnerCanonicalSessionId;
		const effectiveTerminalAsyncDir = existing.terminalAsyncDir ?? patchedTerminalAsyncDir;
		const effectiveCanonicalSessionFile = existing.canonicalSessionFile ?? patchedCanonicalSessionFile;
		const effectiveTerminalEvidence = existing.terminalEvidence ?? patchedTerminalEvidence;
		if (existing.method === "resume" && !["claimed", "failed-before-launch"].includes(nextState)
			&& (!effectiveSourceOperationId || !effectiveSourceRequestDigest || !effectiveSourceTerminalProofDigest || !effectiveSourceCanonicalSessionId || !effectiveSourceRecoveryDescriptorDigest)) {
			throw new ManagedOperationJournalError("invalid_state", `Managed resume state ${nextState} requires source correlation.`);
		}
		if (existing.method !== "resume" && (effectiveSourceOperationId || effectiveSourceRequestDigest || effectiveSourceTerminalProofDigest || effectiveSourceCanonicalSessionId || effectiveSourceRecoveryDescriptorDigest)) {
			throw new ManagedOperationJournalError("invalid_state", "Managed resume source correlation is forbidden for this method.");
		}
		if (existing.method === "resume" && ["runner-ready", "accepted", "terminal"].includes(nextState)
			&& (!effectiveRunnerSessionLeaseTokenDigest || effectiveRunnerCanonicalSessionId !== effectiveSourceCanonicalSessionId)) {
			throw new ManagedOperationJournalError("invalid_state", `Managed resume state ${nextState} requires runner lease correlation.`);
		}
		if (["dispatching", "runner-ready", "accepted", "terminal", "uncertain", "reconciling"].includes(nextState) && !effectiveRunId) {
			throw new ManagedOperationJournalError("invalid_state", `Managed state ${nextState} requires a durable run identity.`);
		}
		if (["claimed", "prepared", "failed-before-launch"].includes(nextState) && effectiveRunId) {
			throw new ManagedOperationJournalError("invalid_state", `Managed state ${nextState} cannot contain a run identity.`);
		}
		if (["dispatching", "runner-ready", "accepted", "terminal", "uncertain", "reconciling"].includes(nextState)
			&& (!effectiveTerminalAsyncDir || !effectiveCanonicalSessionFile)) {
			throw new ManagedOperationJournalError("invalid_state", `Managed state ${nextState} requires terminal recovery paths.`);
		}
		if (["runner-ready", "accepted", "terminal"].includes(nextState)
			&& (!effectiveRunnerInstance || !effectiveAdmissionTokenDigest)) {
			throw new ManagedOperationJournalError("invalid_state", `Managed state ${nextState} requires runner admission correlation.`);
		}
		if (["claimed", "prepared", "dispatching", "failed-before-launch"].includes(nextState)
			&& (effectiveRunnerInstance || effectiveAdmissionTokenDigest)) {
			throw new ManagedOperationJournalError("invalid_state", `Managed state ${nextState} cannot contain runner admission correlation.`);
		}
		if (nextState === "terminal" && !effectiveTerminalEvidence) {
			throw new ManagedOperationJournalError("invalid_state", "Managed terminal state requires durable terminal evidence.");
		}
		if (nextState !== "terminal" && effectiveTerminalEvidence) {
			throw new ManagedOperationJournalError("invalid_state", "Managed terminal evidence is premature.");
		}
		if (!TRANSITIONS[existing.state].has(nextState)) {
			throw new ManagedOperationJournalError("invalid_state", `Managed operation cannot transition from ${existing.state} to ${nextState}.`);
		}
		if (existing.runId === undefined && effectiveRunId !== undefined) {
			const bound = this.readByRun(sessionDigest, consumerId, effectiveRunId);
			if (bound && bound.operationId !== operationId) {
				throw new ManagedOperationJournalError("operation_conflict", "Managed run identity is already bound to another operation.");
			}
		}
		const record: ManagedOperationJournalRecordV1 = {
			...existing,
			state: nextState,
			...(patchedRunId !== undefined ? { runId: patchedRunId } : {}),
			...(patchedSourceRunId !== undefined ? { sourceRunId: patchedSourceRunId } : {}),
			...(patchedSourceOperationId !== undefined ? { sourceOperationId: patchedSourceOperationId } : {}),
			...(patchedSourceRequestDigest !== undefined ? { sourceRequestDigest: patchedSourceRequestDigest } : {}),
			...(patchedSourceTerminalProofDigest !== undefined ? { sourceTerminalProofDigest: patchedSourceTerminalProofDigest } : {}),
			...(patchedSourceCanonicalSessionId !== undefined ? { sourceCanonicalSessionId: patchedSourceCanonicalSessionId } : {}),
			...(patchedSourceRecoveryDescriptorDigest !== undefined ? { sourceRecoveryDescriptorDigest: patchedSourceRecoveryDescriptorDigest } : {}),
			...(patchedRunnerInstance !== undefined ? { runnerProcessInstanceId: patchedRunnerInstance } : {}),
			...(patchedAdmissionTokenDigest !== undefined ? { runnerAdmissionTokenDigest: patchedAdmissionTokenDigest } : {}),
			...(patchedRunnerSessionLeaseTokenDigest !== undefined ? { runnerSessionLeaseTokenDigest: patchedRunnerSessionLeaseTokenDigest } : {}),
			...(patchedRunnerCanonicalSessionId !== undefined ? { runnerCanonicalSessionId: patchedRunnerCanonicalSessionId } : {}),
			...(patchedTerminalAsyncDir !== undefined ? { terminalAsyncDir: patchedTerminalAsyncDir } : {}),
			...(patchedCanonicalSessionFile !== undefined ? { canonicalSessionFile: patchedCanonicalSessionFile } : {}),
			...(patchedTerminalEvidence !== undefined ? { terminalEvidence: patchedTerminalEvidence } : {}),
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

	list(
		parentSessionIdentityDigest: string,
		options: { limit?: number; after?: ManagedOperationJournalCursorV1 } = {},
	): ManagedOperationJournalPageV1 {
		this.#assertOpen();
		const sessionDigest = assertDigest(parentSessionIdentityDigest, "Managed parent-session identity digest");
		const limit = options.limit ?? 256;
		if (!Number.isSafeInteger(limit) || limit < 1 || limit > 256) {
			throw new ManagedOperationJournalError("invalid_request", "Managed journal page limit is invalid.");
		}
		const after = options.after ? {
			consumerId: assertManagedConsumerId(options.after.consumerId),
			operationId: assertManagedOperationId(options.after.operationId),
		} : undefined;
		const operations = existingPrivateChild(this.root, "operations");
		if (!operations) return { records: [] };
		const session = existingPrivateChild(operations, sessionDigest);
		if (!session) return { records: [] };
		const entries: Array<{ consumerId: string; operationId: string; directory: string }> = [];
		for (const consumerName of fs.readdirSync(session).sort()) {
			const consumerId = assertManagedConsumerId(consumerName);
			const consumerDirectory = existingPrivateChild(session, consumerId);
			if (!consumerDirectory) continue;
			for (const operationName of fs.readdirSync(consumerDirectory).sort()) {
				if (operationName.startsWith(".operation.") && operationName.endsWith(".tmp")) continue;
				const operationId = assertManagedOperationId(operationName);
				if (after && (consumerId < after.consumerId || (consumerId === after.consumerId && operationId <= after.operationId))) continue;
				const directory = existingPrivateChild(consumerDirectory, operationId);
				if (!directory) continue;
				entries.push({ consumerId, operationId, directory });
				if (entries.length > limit) break;
			}
			if (entries.length > limit) break;
		}
		const selected = entries.slice(0, limit);
		const records = selected.map(({ consumerId, operationId, directory }) =>
			this.#readOperationRecord(directory, sessionDigest, consumerId, operationId));
		return {
			records: Object.freeze(records),
			...(entries.length > limit && selected.length > 0 ? {
				nextCursor: {
					consumerId: selected[selected.length - 1]!.consumerId,
					operationId: selected[selected.length - 1]!.operationId,
				},
			} : {}),
		};
	}

	readByRun(parentSessionIdentityDigest: string, consumerIdInput: string, runIdInput: string): Readonly<ManagedOperationJournalRecordV1> | undefined {
		this.#assertOpen();
		const sessionDigest = assertDigest(parentSessionIdentityDigest, "Managed parent-session identity digest");
		const consumerId = assertManagedConsumerId(consumerIdInput);
		const runId = assertRunId(runIdInput, "Managed run id");
		const operations = existingPrivateChild(this.root, "operations");
		if (!operations) return undefined;
		const session = existingPrivateChild(operations, sessionDigest);
		if (!session) return undefined;
		const consumer = existingPrivateChild(session, consumerId);
		if (!consumer) return undefined;
		let match: Readonly<ManagedOperationJournalRecordV1> | undefined;
		let inspected = 0;
		for (const operationName of fs.readdirSync(consumer).sort()) {
			if (operationName.startsWith(".operation.") && operationName.endsWith(".tmp")) continue;
			if (++inspected > 10_000) throw new ManagedOperationJournalError("busy", "Managed run lookup exceeds the bounded namespace limit.");
			const operationId = assertManagedOperationId(operationName);
			const directory = existingPrivateChild(consumer, operationId);
			if (!directory) continue;
			const record = this.#readOperationRecord(directory, sessionDigest, consumerId, operationId);
			if (record.runId !== runId) continue;
			if (match) throw new ManagedOperationJournalError("corrupt", "Managed run identity is bound to multiple operations.");
			match = record;
		}
		return match;
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

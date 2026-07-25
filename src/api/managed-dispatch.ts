import { createHash, randomBytes } from "node:crypto";
import { types as utilTypes } from "node:util";
import type { SubagentLaunchContractTools } from "./preflight.ts";

/** Host-neutral protocol vocabulary only. No provider is registered by this module. */
export const SUBAGENT_MANAGED_DISPATCH_VERSION = 1 as const;
export const SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT = "subagents:managed-dispatch:v1:request" as const;
export const SUBAGENT_MANAGED_DISPATCH_REPLY_EVENT_PREFIX = "subagents:managed-dispatch:v1:reply:" as const;

export const MANAGED_CONSUMER_ID_MAX_LENGTH = 64 as const;
export const MANAGED_OPERATION_ID_ENCODED_LENGTH = 43 as const;
export const MANAGED_REQUEST_ID_MAX_LENGTH = 128 as const;

export const MANAGED_JSON_DEFAULT_LIMITS = Object.freeze({
	maxDepth: 32,
	maxNodes: 10_000,
	maxUtf8Bytes: 1_048_576,
	maxSerializedBytes: 1_048_576,
});

export interface ManagedJsonLimits {
	maxDepth?: number;
	maxNodes?: number;
	maxUtf8Bytes?: number;
	maxSerializedBytes?: number;
}

export type JsonPrimitive = null | boolean | number | string;
export type JsonObject = { readonly [key: string]: JsonValue };
export type JsonValue = JsonPrimitive | readonly JsonValue[] | JsonObject;

export type ManagedConsumerId = string & { readonly __managedConsumerId: unique symbol };
export type ManagedOperationId = string & { readonly __managedOperationId: unique symbol };

export interface ManagedOperationScopeV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	consumerId: ManagedConsumerId;
	operationId: ManagedOperationId;
}

export interface ManagedRootIdentityV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	realPath: string;
	device?: string;
	inode?: string;
}

export interface ManagedHostIdentityV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	hostId: string;
}

/** Provider-derived launch profile material. It is never accepted from mutation callers. */
export interface ManagedProfileSnapshotV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	root: string;
	content: JsonValue;
}

export interface ManagedProfileIdentityV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	contentDigest: string;
	root: ManagedRootIdentityV1;
}

export interface ManagedExpectedLaunchV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	hostId: string;
	candidateRunId: string;
	profileIdentityDigest: string;
	/** Added compatibly to v1; managed mutation providers require it before claim. */
	parentSessionIdentityDigest?: string;
	contractDigest: string;
}

export interface ManagedMutationContextV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	consumerId: ManagedConsumerId;
	operationId: ManagedOperationId;
}

export interface ManagedSpawnInputV1 {
	/** Exact ordinary single-agent execution request; the future host provider validates its schema. */
	request: JsonObject;
}

export interface ManagedResumeInputV1 {
	sourceRunId: string;
	index: number;
	/** Exact ordinary resume request; the host validates source/index equality. */
	request: JsonObject;
}

/** Narrow host-accepted request for exact managed revival. */
export interface ManagedResumeExecutorRequestV1 extends JsonObject {
	action: "resume";
	runId: string;
	index: 0;
	message: string;
	async: true;
	clarify: false;
	context: "fresh";
}

export interface ManagedPreflightRequestV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	requestId: string;
	method: "preflight";
	consumerId: ManagedConsumerId;
	input: ({ kind: "spawn" } & ManagedSpawnInputV1) | ({ kind: "resume" } & ManagedResumeInputV1);
}

/** Bounded, name-free projection of the exact host-resolved child launch capabilities. */
export interface ManagedChildCapabilityV1 {
	readonly version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	readonly effectiveToolCount: number;
	readonly runtimeExtensionCount: number;
	readonly configuredExtensionCount: number;
	readonly disableAmbientExtensions: boolean;
	readonly fanoutAuthorized: boolean;
}

/** Projects counts and booleans only; tool/extension names and paths never cross this boundary. */
export function projectManagedChildCapabilityV1(
	tools: Readonly<SubagentLaunchContractTools>,
): Readonly<ManagedChildCapabilityV1> {
	const effectiveTools = new Set([...tools.declaredBuiltin, ...tools.effectiveMcpTools]);
	return Object.freeze({
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		effectiveToolCount: effectiveTools.size,
		runtimeExtensionCount: tools.runtimeExtensions.length,
		configuredExtensionCount: tools.configuredExtensions.length,
		disableAmbientExtensions: tools.disableAmbientExtensions,
		fanoutAuthorized: tools.fanoutAuthorized,
	});
}

export type ManagedPreflightResultV1 = {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	ok: true;
	host: ManagedHostIdentityV1;
	profile: ManagedProfileIdentityV1;
	profileIdentityDigest: string;
	childCapability: ManagedChildCapabilityV1;
	/** Present from the non-launching host provider; optional for inert early-v1 consumers. */
	parentSessionIdentityDigest?: string;
	candidateRunId: string;
	contractDigest: string;
} | {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	ok: false;
	code: ManagedDispatchErrorCodeV1;
	message: string;
};

interface ManagedMutationRequestBaseV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	requestId: string;
	managed: ManagedMutationContextV1;
}

export interface ManagedSpawnRequestV1 extends ManagedMutationRequestBaseV1 {
	method: "spawn";
	expectedLaunch: ManagedExpectedLaunchV1;
	input: ManagedSpawnInputV1;
}

export interface ManagedResumeRequestV1 extends ManagedMutationRequestBaseV1 {
	method: "resume";
	expectedLaunch: ManagedExpectedLaunchV1;
	input: ManagedResumeInputV1;
}

export type ManagedOperationTargetV1 =
	| { consumerId: ManagedConsumerId; operationId: ManagedOperationId; runId?: never }
	| { consumerId: ManagedConsumerId; runId: string; operationId?: never };

export interface ManagedCapabilitiesRequestV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	requestId: string;
	method: "capabilities";
}

export interface ManagedStatusRequestV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	requestId: string;
	method: "status";
	target: ManagedOperationTargetV1;
}

export interface ManagedDetailsRequestV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	requestId: string;
	method: "details";
	target: ManagedOperationTargetV1;
}

export type ManagedReadRequestV1 = ManagedCapabilitiesRequestV1 | ManagedStatusRequestV1 | ManagedDetailsRequestV1;

export interface ManagedSteerRequestV1 extends ManagedMutationRequestBaseV1 {
	method: "steer";
	input: { target: ManagedOperationTargetV1; message: string };
}

export interface ManagedInterruptRequestV1 extends ManagedMutationRequestBaseV1 {
	method: "interrupt";
	input: { target: ManagedOperationTargetV1 };
}

export interface ManagedStopRequestV1 extends ManagedMutationRequestBaseV1 {
	method: "stop";
	input: { target: ManagedOperationTargetV1 };
}

export interface ManagedRetireRequestV1 extends ManagedMutationRequestBaseV1 {
	method: "retire";
	input: { target: ManagedOperationTargetV1; acknowledgeUncertain?: boolean };
}

export type ManagedControlRequestV1 =
	| ManagedSteerRequestV1
	| ManagedInterruptRequestV1
	| ManagedStopRequestV1
	| ManagedRetireRequestV1;

export type ManagedMutationRequestV1 = ManagedSpawnRequestV1 | ManagedResumeRequestV1 | ManagedControlRequestV1;
export type ManagedDispatchRequestV1 = ManagedPreflightRequestV1 | ManagedReadRequestV1 | ManagedMutationRequestV1;
export type ManagedMutationMethodV1 = ManagedMutationRequestV1["method"];
export type ManagedReadMethodV1 = "preflight" | "capabilities" | "status" | "details";
export type ManagedDispatchMethodV1 = ManagedMutationMethodV1 | ManagedReadMethodV1;

export type ManagedOperationStateV1 =
	| "claimed"
	| "prepared"
	| "runner-ready"
	| "accepted"
	| "terminal"
	| "failed-before-launch"
	| "uncertain"
	| "retired";

export interface ManagedDispatchReceiptV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	consumerId: ManagedConsumerId;
	operationId: ManagedOperationId;
	requestDigest: string;
	state: ManagedOperationStateV1;
	runId?: string;
	sourceRunId?: string;
	replayed: boolean;
}

export interface ManagedChildIdentityV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	index: 0;
	canonicalSessionId: string;
	resumeDisposition: "resumable" | "non-resumable" | "unavailable";
}

export interface ManagedOperationStatusV1 extends ManagedDispatchReceiptV1 {
	method: ManagedMutationMethodV1;
	/** Exact caller-supplied command target; only one is present for command records. */
	targetOperationId?: ManagedOperationId;
	targetRunId?: string;
	/** Exact launch actor resolved by the provider for a command. */
	actorOperationId?: ManagedOperationId;
	actorRunId?: string;
	controlOutcome?: "acknowledged" | "failed" | "unknown";
	retirementAcknowledgedUncertain?: true;
	/** Command identity that durably retired this launch actor. */
	retiredByOperationId?: ManagedOperationId;
	runOutcome?: "running" | "completed" | "failed" | "interrupted" | "unknown";
	processTerminal?: JsonValue;
	child?: ManagedChildIdentityV1;
}

export interface ManagedOperationDetailsV1 extends ManagedOperationStatusV1 {
	contractDigest?: string;
	profile?: ManagedProfileIdentityV1;
	createdAt?: string;
	updatedAt?: string;
}

export interface ManagedDispatchCapabilitiesV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	state: "ready" | "recovering" | "unavailable";
	available: boolean;
	hostId?: string;
	parentSessionIdentityDigest?: string;
	sessionGeneration?: number;
	methods: {
		preflight: true;
		spawn: boolean;
		status: boolean;
		details: boolean;
		resume: boolean;
		steer: boolean;
		interrupt: boolean;
		stop: boolean;
		retire: boolean;
	};
	durability: "journal-v1";
	lifecycle: { version: 3; managedTerminalCorrelation: boolean };
	effects: "not-exactly-once";
}

export type ManagedDispatchErrorCodeV1 =
	| "invalid_request"
	| "unsupported_version"
	| "unsupported_method"
	| "no_active_session"
	| "not_found"
	| "invalid_state"
	| "operation_conflict"
	| "operation_uncertain"
	| "contract_changed"
	| "profile_changed"
	| "host_mismatch"
	| "retired"
	| "unsupported_host"
	| "execution_failed";

export type ManagedDispatchReplyV1<
	T = unknown,
	TMethod extends ManagedDispatchMethodV1 = ManagedDispatchMethodV1,
> = {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	requestId: string;
	method: TMethod;
	success: true;
	data: T;
} | {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	requestId: string;
	method: TMethod;
	success: false;
	error: { code: ManagedDispatchErrorCodeV1; message: string };
};

export type ManagedPreflightReplyV1 = ManagedDispatchReplyV1<ManagedPreflightResultV1, "preflight">;

export interface ManagedDispatchRequirementsV1 {
	version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	scope: "single-host";
	singleHost: true;
	namespace: "active-parent-session+consumerId+operationId";
	retention: "explicit";
	replay: "fail-closed";
	effects: "not-exactly-once";
}

/** Static protocol semantics only; this is not provider availability or durability metadata. */
export const SUBAGENT_MANAGED_DISPATCH_REQUIREMENTS_V1: ManagedDispatchRequirementsV1 = Object.freeze({
	version: SUBAGENT_MANAGED_DISPATCH_VERSION,
	scope: "single-host",
	singleHost: true,
	namespace: "active-parent-session+consumerId+operationId",
	retention: "explicit",
	replay: "fail-closed",
	effects: "not-exactly-once",
});

export interface CanonicalManagedJson {
	readonly normalized: JsonValue;
	readonly serialization: string;
}

const CONSUMER_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._~-]*$/;
const OPERATION_ID_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const REQUEST_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._~-]*$/;
const DIGEST_PATTERN = /^[a-f0-9]{64}$/;
const MUTATION_METHODS = new Set<string>(["spawn", "resume", "steer", "interrupt", "stop", "retire"]);
const PROFILE_CONTENT_DOMAIN = "pi-subagents/managed-dispatch/v1/profile-content";
const PROFILE_IDENTITY_DOMAIN = "pi-subagents/managed-dispatch/v1/profile-identity";
const REQUEST_DOMAIN = "pi-subagents/managed-dispatch/v1/request";
const ARRAY_INDEX_PATTERN = /^(0|[1-9][0-9]*)$/;

export function assertManagedConsumerId(value: unknown): ManagedConsumerId {
	if (typeof value !== "string" || value.length === 0 || value.length > MANAGED_CONSUMER_ID_MAX_LENGTH || !CONSUMER_ID_PATTERN.test(value)) {
		throw new TypeError(`Managed consumerId must be a 1-${MANAGED_CONSUMER_ID_MAX_LENGTH} character safe token.`);
	}
	return value as ManagedConsumerId;
}

export function createManagedOperationId(): ManagedOperationId {
	return randomBytes(32).toString("base64url") as ManagedOperationId;
}

export function assertManagedOperationId(value: unknown): ManagedOperationId {
	if (typeof value !== "string" || value.length !== MANAGED_OPERATION_ID_ENCODED_LENGTH || !OPERATION_ID_PATTERN.test(value)) {
		throw new TypeError("Managed operationId must be canonical 43-character base64url encoding of 32 bytes.");
	}
	let bytes: Buffer;
	try {
		bytes = Buffer.from(value, "base64url");
	} catch {
		throw new TypeError("Managed operationId must be canonical 43-character base64url encoding of 32 bytes.");
	}
	if (bytes.length !== 32 || bytes.toString("base64url") !== value) {
		throw new TypeError("Managed operationId must be canonical 43-character base64url encoding of 32 bytes.");
	}
	return value as ManagedOperationId;
}

/** Strict closed-schema validation for the ordinary request nested in managed resume. */
export function assertManagedResumeExecutorRequestV1(
	value: unknown,
	sourceRunId: string,
	index: number,
): Readonly<ManagedResumeExecutorRequestV1> {
	const normalized = canonicalizeManagedJson(value).normalized;
	if (!normalized || typeof normalized !== "object" || Array.isArray(normalized)) {
		throw new TypeError("Managed resume executor request must be an object.");
	}
	const record = normalized as Record<string, JsonValue>;
	const expected = ["action", "async", "clarify", "context", "index", "message", "runId"].sort();
	if (Object.keys(record).sort().join("\0") !== expected.join("\0")) {
		throw new TypeError("Managed resume executor request contains missing or unknown fields.");
	}
	if (record.action !== "resume" || record.async !== true || record.clarify !== false || record.context !== "fresh") {
		throw new TypeError("Managed resume executor request fixed fields are invalid.");
	}
	if (typeof sourceRunId !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._~:-]{0,255}$/.test(sourceRunId)
		|| record.runId !== sourceRunId || index !== 0 || record.index !== 0) {
		throw new TypeError("Managed resume executor request source identity is invalid.");
	}
	if (typeof record.message !== "string" || !record.message.trim() || record.message.includes("\0")
		|| hasUnpairedSurrogate(record.message) || Buffer.byteLength(record.message, "utf8") > 65_536) {
		throw new TypeError("Managed resume executor request message is invalid.");
	}
	return normalized as unknown as Readonly<ManagedResumeExecutorRequestV1>;
}

function rejectProxy(value: object, location: string): void {
	if (utilTypes.isProxy(value)) throw new TypeError(`Managed JSON rejects proxies at ${location}.`);
}

function plainDataRecord(value: object, label: string): { record: Record<string, unknown>; keys: string[] } {
	rejectProxy(value, label);
	if (Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) throw new TypeError(`${label} must be a plain object.`);
	const ownKeys = Reflect.ownKeys(value);
	if (ownKeys.some((key) => typeof key === "symbol")) throw new TypeError(`${label} contains an unknown symbol field.`);
	const keys = ownKeys as string[];
	for (const key of keys) {
		const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
		if (!descriptor.enumerable || !("value" in descriptor)) throw new TypeError(`${label} fields must be enumerable data properties.`);
	}
	return { record: value as Record<string, unknown>, keys };
}

function assertObjectKeys(value: object, required: readonly string[], optional: readonly string[], label: string): Record<string, unknown> {
	const { record, keys } = plainDataRecord(value, label);
	const allowed = new Set([...required, ...optional]);
	if (keys.some((key) => !allowed.has(key)) || required.some((key) => !keys.includes(key))) {
		throw new TypeError(`${label} contains missing or unknown fields.`);
	}
	return record;
}

function assertExactKeys(value: object, expected: readonly string[], label: string): Record<string, unknown> {
	return assertObjectKeys(value, expected, [], label);
}

function hasUnpairedSurrogate(value: string): boolean {
	for (let index = 0; index < value.length; index++) {
		const code = value.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			if (index + 1 >= value.length) return true;
			const next = value.charCodeAt(index + 1);
			if (next < 0xdc00 || next > 0xdfff) return true;
			index++;
		} else if (code >= 0xdc00 && code <= 0xdfff) {
			return true;
		}
	}
	return false;
}

function normalizeManagedJsonLimits(limits: unknown): Required<ManagedJsonLimits> {
	if (!limits || typeof limits !== "object") throw new TypeError("Managed JSON limits must be a plain object.");
	const record = assertObjectKeys(
		limits,
		[],
		["maxDepth", "maxNodes", "maxUtf8Bytes", "maxSerializedBytes"],
		"Managed JSON limits",
	);
	const normalized: Required<ManagedJsonLimits> = { ...MANAGED_JSON_DEFAULT_LIMITS };
	for (const name of ["maxDepth", "maxNodes", "maxUtf8Bytes", "maxSerializedBytes"] as const) {
		if (!Object.prototype.hasOwnProperty.call(record, name)) continue;
		const value = record[name];
		if (!Number.isSafeInteger(value) || (value as number) < 0) throw new TypeError(`${name} must be a non-negative safe integer.`);
		normalized[name] = value as number;
	}
	return normalized;
}

function quoteManagedJsonString(value: string): string {
	let serialization = '"';
	for (let index = 0; index < value.length; index++) {
		const code = value.charCodeAt(index);
		switch (code) {
			case 0x08: serialization += "\\b"; break;
			case 0x09: serialization += "\\t"; break;
			case 0x0a: serialization += "\\n"; break;
			case 0x0c: serialization += "\\f"; break;
			case 0x0d: serialization += "\\r"; break;
			case 0x22: serialization += '\\"'; break;
			case 0x5c: serialization += "\\\\"; break;
			default:
				serialization += code < 0x20 ? `\\u${code.toString(16).padStart(4, "0")}` : value[index];
		}
	}
	return `${serialization}"`;
}

function serializeNormalizedManagedJson(value: JsonValue, maxSerializedBytes: number): string {
	const chunks: string[] = [];
	let serializedBytes = 0;
	const append = (chunk: string): void => {
		serializedBytes += Buffer.byteLength(chunk, "utf8");
		if (serializedBytes > maxSerializedBytes) throw new TypeError(`Managed JSON exceeds maxSerializedBytes (${maxSerializedBytes}).`);
		chunks.push(chunk);
	};
	const serialize = (current: JsonValue): void => {
		if (current === null) {
			append("null");
		} else if (typeof current === "boolean" || typeof current === "number") {
			append(String(current));
		} else if (typeof current === "string") {
			append(quoteManagedJsonString(current));
		} else if (Array.isArray(current)) {
			append("[");
			for (let index = 0; index < current.length; index++) {
				if (index > 0) append(",");
				serialize(current[index]);
			}
			append("]");
		} else {
			append("{");
			const keys = (Reflect.ownKeys(current) as string[]).sort();
			for (let index = 0; index < keys.length; index++) {
				if (index > 0) append(",");
				const key = keys[index];
				append(quoteManagedJsonString(key));
				append(":");
				serialize(Object.getOwnPropertyDescriptor(current, key)!.value as JsonValue);
			}
			append("}");
		}
	};
	serialize(value);
	return chunks.join("");
}

export function canonicalizeManagedJson(input: unknown, limits: ManagedJsonLimits = {}): CanonicalManagedJson {
	const { maxDepth, maxNodes, maxUtf8Bytes, maxSerializedBytes } = normalizeManagedJsonLimits(limits);
	const seen = new WeakSet<object>();
	let nodes = 0;
	let utf8Bytes = 0;

	const countString = (value: string, location: string): void => {
		if (hasUnpairedSurrogate(value)) throw new TypeError(`Managed JSON rejects unpaired UTF-16 surrogates at ${location}.`);
		utf8Bytes += Buffer.byteLength(value, "utf8");
		if (utf8Bytes > maxUtf8Bytes) throw new TypeError(`Managed JSON exceeds maxUtf8Bytes (${maxUtf8Bytes}).`);
	};

	const visit = (value: unknown, depth: number, location: string): JsonValue => {
		if (++nodes > maxNodes) throw new TypeError(`Managed JSON exceeds maxNodes (${maxNodes}).`);
		if (depth > maxDepth) throw new TypeError(`Managed JSON exceeds maxDepth (${maxDepth}).`);
		if (value === null) return null;
		if (typeof value === "boolean") return value;
		if (typeof value === "string") {
			countString(value, location);
			return value;
		}
		if (typeof value === "number") {
			if (!Number.isFinite(value) || Object.is(value, -0)) throw new TypeError(`Managed JSON rejects non-finite and negative-zero numbers at ${location}.`);
			return value;
		}
		if (typeof value !== "object") throw new TypeError(`Managed JSON rejects unsupported ${typeof value} values at ${location}.`);
		rejectProxy(value, location);
		if (seen.has(value)) throw new TypeError(`Managed JSON rejects cycles and shared references at ${location}.`);
		seen.add(value);

		if (Array.isArray(value)) {
			if (Object.getPrototypeOf(value) !== Array.prototype) throw new TypeError(`Managed JSON rejects custom array prototypes at ${location}.`);
			const length = Object.getOwnPropertyDescriptor(value, "length")!.value as number;
			if (length > maxNodes - nodes) throw new TypeError(`Managed JSON exceeds maxNodes (${maxNodes}).`);
			const ownKeys = Reflect.ownKeys(value);
			if (ownKeys.length !== length + 1) throw new TypeError(`Managed JSON rejects sparse arrays at ${location}.`);
			for (const key of ownKeys) {
				if (typeof key === "symbol") throw new TypeError(`Managed JSON rejects symbol properties at ${location}.`);
				if (key === "length") continue;
				if (!ARRAY_INDEX_PATTERN.test(key) || Number(key) >= length) throw new TypeError(`Managed JSON rejects custom array fields at ${location}.`);
				const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
				if (!descriptor.enumerable) throw new TypeError(`Managed JSON rejects non-enumerable properties at ${location}.`);
				if (!("value" in descriptor)) throw new TypeError(`Managed JSON rejects accessors at ${location}.`);
			}
			const normalized: JsonValue[] = [];
			for (let index = 0; index < length; index++) {
				const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
				if (!descriptor) throw new TypeError(`Managed JSON rejects sparse arrays at ${location}.`);
				normalized.push(visit(descriptor.value, depth + 1, `${location}[${index}]`));
			}
			return Object.freeze(normalized);
		}

		if (Object.getPrototypeOf(value) !== Object.prototype) throw new TypeError(`Managed JSON rejects custom object prototypes at ${location}.`);
		const ownKeys = Reflect.ownKeys(value);
		if (ownKeys.length > maxNodes - nodes) throw new TypeError(`Managed JSON exceeds maxNodes (${maxNodes}).`);
		const stringKeys: string[] = [];
		for (const key of ownKeys) {
			if (typeof key === "symbol") throw new TypeError(`Managed JSON rejects symbol properties at ${location}.`);
			const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
			if (!descriptor.enumerable) throw new TypeError(`Managed JSON rejects non-enumerable properties at ${location}.${key}.`);
			if (!("value" in descriptor)) throw new TypeError(`Managed JSON rejects accessors at ${location}.${key}.`);
			countString(key, `${location} key`);
			stringKeys.push(key);
		}
		stringKeys.sort();
		const normalized: Record<string, JsonValue> = {};
		for (const key of stringKeys) {
			const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
			Object.defineProperty(normalized, key, {
				value: visit(descriptor.value, depth + 1, `${location}.${key}`),
				enumerable: true,
				configurable: true,
				writable: true,
			});
		}
		return Object.freeze(normalized);
	};

	const normalized = visit(input, 0, "$Root");
	const serialization = serializeNormalizedManagedJson(normalized, maxSerializedBytes);
	return Object.freeze({ normalized, serialization });
}

function sha256Domain(domain: string, serialization: string): string {
	return createHash("sha256").update(domain, "utf8").update("\0", "utf8").update(serialization, "utf8").digest("hex");
}

export function computeManagedProfileContentDigest(content: unknown): string {
	return sha256Domain(PROFILE_CONTENT_DOMAIN, canonicalizeManagedJson(content).serialization);
}

function assertVersion(value: unknown, label: string): void {
	if (value !== SUBAGENT_MANAGED_DISPATCH_VERSION) throw new TypeError(`${label} has an unsupported version.`);
}

function assertRequestId(value: unknown): string {
	if (typeof value !== "string" || value.length === 0 || value.length > MANAGED_REQUEST_ID_MAX_LENGTH || !REQUEST_ID_PATTERN.test(value)) {
		throw new TypeError(`Managed requestId must be a 1-${MANAGED_REQUEST_ID_MAX_LENGTH} character safe token.`);
	}
	return value;
}

function assertBoundedText(value: unknown, label: string, maxUtf8Bytes: number): string {
	if (
		typeof value !== "string"
		|| value.length === 0
		|| hasUnpairedSurrogate(value)
		|| Buffer.byteLength(value, "utf8") > maxUtf8Bytes
		|| value.includes("\0")
	) {
		throw new TypeError(`${label} must be a non-empty well-formed string of at most ${maxUtf8Bytes} UTF-8 bytes without NUL.`);
	}
	return value;
}

function assertSafeIdentifier(value: unknown, label: string): string {
	const text = assertBoundedText(value, label, 256);
	if (!/^[A-Za-z0-9][A-Za-z0-9._~:-]*$/.test(text)) throw new TypeError(`${label} must be a safe identifier.`);
	return text;
}

function assertRootPath(value: unknown, label: string): string {
	const text = assertBoundedText(value, label, 4_096);
	if ([...text].some((character) => {
		const code = character.codePointAt(0) ?? 0;
		return code <= 31 || (code >= 127 && code <= 159);
	})) throw new TypeError(`${label} must not contain control characters.`);
	return text;
}

function assertDigest(value: unknown, label: string): string {
	if (typeof value !== "string" || !DIGEST_PATTERN.test(value)) throw new TypeError(`${label} must be a lowercase SHA-256 hex digest.`);
	return value;
}

function normalizeRootIdentity(value: unknown): JsonValue {
	if (!value || typeof value !== "object") throw new TypeError("Managed root identity must be an object.");
	const root = assertObjectKeys(value, ["version", "realPath"], ["device", "inode"], "Managed root identity");
	assertVersion(root.version, "Managed root identity");
	const normalized: Record<string, JsonValue> = {
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		realPath: assertRootPath(root.realPath, "Managed root realPath"),
	};
	for (const key of ["device", "inode"] as const) {
		if (Object.prototype.hasOwnProperty.call(root, key)) normalized[key] = assertSafeIdentifier(root[key], `Managed root ${key}`);
	}
	return normalized;
}

export function computeManagedProfileIdentityDigest(identity: unknown): string {
	if (!identity || typeof identity !== "object") throw new TypeError("Managed profile identity must be an object.");
	const profile = assertExactKeys(identity, ["version", "contentDigest", "root"], "Managed profile identity");
	assertVersion(profile.version, "Managed profile identity");
	const normalized = {
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		contentDigest: assertDigest(profile.contentDigest, "Managed profile contentDigest"),
		root: normalizeRootIdentity(profile.root),
	};
	return sha256Domain(PROFILE_IDENTITY_DOMAIN, canonicalizeManagedJson(normalized).serialization);
}

function normalizeProfileSnapshot(value: unknown): JsonValue {
	if (!value || typeof value !== "object") throw new TypeError("Managed profile snapshot must be an object.");
	const profile = assertExactKeys(value, ["version", "root", "content"], "Managed profile snapshot");
	assertVersion(profile.version, "Managed profile snapshot");
	return {
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		root: assertRootPath(profile.root, "Managed profile snapshot root"),
		content: canonicalizeManagedJson(profile.content).normalized,
	};
}

function normalizeExpectedLaunch(value: unknown): JsonValue {
	if (!value || typeof value !== "object") throw new TypeError("Managed expected launch must be an object.");
	const expected = assertObjectKeys(
		value,
		["version", "hostId", "candidateRunId", "profileIdentityDigest", "contractDigest"],
		["parentSessionIdentityDigest"],
		"Managed expected launch",
	);
	assertVersion(expected.version, "Managed expected launch");
	const normalized: Record<string, JsonValue> = {
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		hostId: assertSafeIdentifier(expected.hostId, "Managed expected launch hostId"),
		candidateRunId: assertSafeIdentifier(expected.candidateRunId, "Managed expected launch candidateRunId"),
		profileIdentityDigest: assertDigest(expected.profileIdentityDigest, "Managed expected launch profileIdentityDigest"),
		contractDigest: assertDigest(expected.contractDigest, "Managed expected launch contractDigest"),
	};
	if (Object.prototype.hasOwnProperty.call(expected, "parentSessionIdentityDigest")) {
		normalized.parentSessionIdentityDigest = assertDigest(
			expected.parentSessionIdentityDigest,
			"Managed expected launch parentSessionIdentityDigest",
		);
	}
	return normalized;
}

function normalizeTarget(value: unknown, expectedConsumerId?: ManagedConsumerId): JsonValue {
	if (!value || typeof value !== "object") throw new TypeError("Managed operation target must be an object.");
	const { record, keys } = plainDataRecord(value, "Managed operation target");
	const operationTarget = keys.length === 2 && keys.includes("consumerId") && keys.includes("operationId");
	const runTarget = keys.length === 2 && keys.includes("consumerId") && keys.includes("runId");
	if (!operationTarget && !runTarget) throw new TypeError("Managed operation target must contain exactly consumerId and one of operationId or runId.");
	const consumerId = assertManagedConsumerId(record.consumerId);
	if (expectedConsumerId !== undefined && consumerId !== expectedConsumerId) {
		throw new TypeError("Managed control target consumerId must match the mutation namespace consumerId.");
	}
	return operationTarget
		? { consumerId, operationId: assertManagedOperationId(record.operationId) }
		: { consumerId, runId: assertSafeIdentifier(record.runId, "Managed target runId") };
}

function normalizeExecutorRequest(value: unknown, label: string): JsonObject {
	const normalized = canonicalizeManagedJson(value).normalized;
	if (normalized === null || typeof normalized !== "object" || Array.isArray(normalized)) {
		throw new TypeError(`${label} must be a plain JSON object.`);
	}
	return normalized as JsonObject;
}

function normalizeSpawnInput(value: unknown): JsonValue {
	if (!value || typeof value !== "object") throw new TypeError("Managed spawn input must be an object.");
	const input = assertExactKeys(value, ["request"], "Managed spawn input");
	return {
		request: normalizeExecutorRequest(input.request, "Managed spawn executor request"),
	};
}

function normalizeResumeInput(value: unknown): JsonValue {
	if (!value || typeof value !== "object") throw new TypeError("Managed resume input must be an object.");
	const input = assertExactKeys(value, ["sourceRunId", "index", "request"], "Managed resume input");
	if (!Number.isSafeInteger(input.index) || (input.index as number) < 0 || (input.index as number) > 1_000_000) {
		throw new TypeError("Managed resume index must be an integer between 0 and 1000000.");
	}
	return {
		sourceRunId: assertSafeIdentifier(input.sourceRunId, "Managed resume sourceRunId"),
		index: input.index as number,
		request: normalizeExecutorRequest(input.request, "Managed resume executor request"),
	};
}

function normalizeControlInput(
	method: string,
	value: unknown,
	expectedConsumerId: ManagedConsumerId,
): JsonValue {
	if (!value || typeof value !== "object") throw new TypeError(`Managed ${method} input must be an object.`);
	if (method === "steer") {
		const input = assertExactKeys(value, ["target", "message"], "Managed steer input");
		return {
			target: normalizeTarget(input.target, expectedConsumerId),
			message: assertBoundedText(input.message, "Managed steer message", 65_536),
		};
	}
	if (method === "retire") {
		const input = assertObjectKeys(value, ["target"], ["acknowledgeUncertain"], "Managed retire input");
		const normalized: Record<string, JsonValue> = {
			target: normalizeTarget(input.target, expectedConsumerId),
		};
		if (Object.prototype.hasOwnProperty.call(input, "acknowledgeUncertain")) {
			if (typeof input.acknowledgeUncertain !== "boolean") throw new TypeError("Managed retire acknowledgeUncertain must be a boolean.");
			normalized.acknowledgeUncertain = input.acknowledgeUncertain;
		}
		return normalized;
	}
	const input = assertExactKeys(value, ["target"], `Managed ${method} input`);
	return { target: normalizeTarget(input.target, expectedConsumerId) };
}

function normalizeManagedMutationRequest(request: unknown): JsonObject {
	if (!request || typeof request !== "object") throw new TypeError("Managed mutation request must be an object.");
	const inspected = plainDataRecord(request, "Managed mutation request");
	const method = inspected.record.method;
	if (typeof method !== "string" || !MUTATION_METHODS.has(method)) throw new TypeError("Managed mutation request has an unsupported method.");
	const launchMethod = method === "spawn" || method === "resume";
	const envelope = assertExactKeys(
		request,
		launchMethod
			? ["version", "requestId", "method", "managed", "expectedLaunch", "input"]
			: ["version", "requestId", "method", "managed", "input"],
		"Managed mutation request",
	);
	assertVersion(envelope.version, "Managed mutation request");
	const requestId = assertRequestId(envelope.requestId);
	if (!envelope.managed || typeof envelope.managed !== "object") throw new TypeError("Managed mutation request managed discriminator must be an object.");
	const managed = assertExactKeys(envelope.managed, ["version", "consumerId", "operationId"], "Managed mutation discriminator");
	assertVersion(managed.version, "Managed mutation discriminator");
	const consumerId = assertManagedConsumerId(managed.consumerId);
	const operationId = assertManagedOperationId(managed.operationId);
	const input = method === "spawn"
		? normalizeSpawnInput(envelope.input)
		: method === "resume"
			? normalizeResumeInput(envelope.input)
			: normalizeControlInput(method, envelope.input, consumerId);
	const normalized: Record<string, JsonValue> = {
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		requestId,
		method,
		managed: { version: SUBAGENT_MANAGED_DISPATCH_VERSION, consumerId, operationId },
		input,
	};
	if (launchMethod) normalized.expectedLaunch = normalizeExpectedLaunch(envelope.expectedLaunch);
	return normalized;
}

/** Strictly validates capability/status/details transport envelopes. */
export function parseManagedReadRequestV1(request: unknown): Readonly<ManagedReadRequestV1> {
	if (!request || typeof request !== "object") throw new TypeError("Managed read request must be an object.");
	const inspected = plainDataRecord(request, "Managed read request").record;
	const method = inspected.method;
	if (method === "capabilities") {
		const envelope = assertExactKeys(request, ["version", "requestId", "method"], "Managed capabilities request");
		assertVersion(envelope.version, "Managed capabilities request");
		if (envelope.method !== "capabilities") throw new TypeError("Managed capabilities request method is invalid.");
		return canonicalizeManagedJson({
			version: SUBAGENT_MANAGED_DISPATCH_VERSION,
			requestId: assertRequestId(envelope.requestId),
			method,
		}).normalized as unknown as Readonly<ManagedCapabilitiesRequestV1>;
	}
	if (method !== "status" && method !== "details") throw new TypeError("Managed read request has an unsupported method.");
	const envelope = assertExactKeys(request, ["version", "requestId", "method", "target"], `Managed ${method} request`);
	assertVersion(envelope.version, `Managed ${method} request`);
	return canonicalizeManagedJson({
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		requestId: assertRequestId(envelope.requestId),
		method,
		target: normalizeTarget(envelope.target),
	}).normalized as unknown as Readonly<ManagedStatusRequestV1 | ManagedDetailsRequestV1>;
}

/** Strictly validates and returns a deeply frozen mutation transport envelope. */
export function parseManagedMutationRequestV1(request: unknown): Readonly<ManagedMutationRequestV1> {
	return canonicalizeManagedJson(normalizeManagedMutationRequest(request)).normalized as unknown as Readonly<ManagedMutationRequestV1>;
}

/** Strictly validates and returns a deeply frozen preflight transport envelope. */
export function parseManagedPreflightRequestV1(request: unknown): Readonly<ManagedPreflightRequestV1> {
	if (!request || typeof request !== "object") throw new TypeError("Managed preflight request must be an object.");
	const envelope = assertExactKeys(request, ["version", "requestId", "method", "consumerId", "input"], "Managed preflight request");
	assertVersion(envelope.version, "Managed preflight request");
	const requestId = assertRequestId(envelope.requestId);
	if (envelope.method !== "preflight") throw new TypeError("Managed preflight request method must be 'preflight'.");
	const consumerId = assertManagedConsumerId(envelope.consumerId);
	if (!envelope.input || typeof envelope.input !== "object") throw new TypeError("Managed preflight input must be an object.");
	const inspectedInput = plainDataRecord(envelope.input, "Managed preflight input").record;
	const kind = inspectedInput.kind;
	let input: JsonObject;
	if (kind === "spawn") {
		const exact = assertExactKeys(envelope.input, ["kind", "request"], "Managed spawn preflight input");
		input = { kind, ...normalizeSpawnInput({ request: exact.request }) as JsonObject };
	} else if (kind === "resume") {
		const exact = assertExactKeys(envelope.input, ["kind", "sourceRunId", "index", "request"], "Managed resume preflight input");
		input = {
			kind,
			...normalizeResumeInput({ sourceRunId: exact.sourceRunId, index: exact.index, request: exact.request }) as JsonObject,
		};
	} else {
		throw new TypeError("Managed preflight input kind must be 'spawn' or 'resume'.");
	}
	return canonicalizeManagedJson({
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		requestId,
		method: "preflight",
		consumerId,
		input,
	}).normalized as unknown as Readonly<ManagedPreflightRequestV1>;
}

/**
 * Hashes only validated semantic mutation identity. The transport requestId is
 * validated but deliberately excluded; operationId and preflight expectations
 * are deliberately included.
 */
export function computeManagedRequestDigest(request: unknown): string {
	const parsed = parseManagedMutationRequestV1(request);
	const semantic: Record<string, JsonValue> = {
		protocolVersion: SUBAGENT_MANAGED_DISPATCH_VERSION,
		method: parsed.method,
		consumerId: parsed.managed.consumerId,
		operationId: parsed.managed.operationId,
		input: parsed.input as unknown as JsonValue,
	};
	if (parsed.method === "spawn" || parsed.method === "resume") {
		semantic.expectedLaunch = parsed.expectedLaunch as unknown as JsonValue;
	}
	return sha256Domain(REQUEST_DOMAIN, canonicalizeManagedJson(semantic).serialization);
}

export function managedDispatchReplyEvent(requestId: string): string {
	return `${SUBAGENT_MANAGED_DISPATCH_REPLY_EVENT_PREFIX}${assertRequestId(requestId)}`;
}

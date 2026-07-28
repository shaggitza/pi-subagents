// Generated runtime companion for managed-dispatch.ts. Keep exports in sync.
import { createHash, randomBytes } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { types as utilTypes } from "node:util";
export const SUBAGENT_MANAGED_DISPATCH_VERSION = 1;
export const SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT = "subagents:managed-dispatch:v1:request";
export const SUBAGENT_MANAGED_DISPATCH_REPLY_EVENT_PREFIX = "subagents:managed-dispatch:v1:reply:";
export const MANAGED_OPAQUE_TASK_TRANSPORT_PREFIX = "[pi-subagents managed opaque task v1]\n";
/** Encodes an exact managed task so Pi's print-mode stdin trimming cannot alter it. */
export function encodeManagedOpaqueTaskTransportV1(task) {
    if (typeof task !== "string") throw new TypeError("Managed opaque task must be a string.");
    return `${MANAGED_OPAQUE_TASK_TRANSPORT_PREFIX}${Buffer.from(JSON.stringify(task), "utf8").toString("base64url")}`;
}
/** Decodes the canonical managed stdin envelope, or returns undefined for unrelated input. */
export function decodeManagedOpaqueTaskTransportV1(input) {
    if (typeof input !== "string" || !input.startsWith(MANAGED_OPAQUE_TASK_TRANSPORT_PREFIX)) return undefined;
    const payload = input.slice(MANAGED_OPAQUE_TASK_TRANSPORT_PREFIX.length);
    if (!payload || !/^[A-Za-z0-9_-]+$/u.test(payload)) throw new TypeError("Managed opaque task transport is invalid.");
    const serialized = Buffer.from(payload, "base64url").toString("utf8");
    if (Buffer.from(serialized, "utf8").toString("base64url") !== payload) throw new TypeError("Managed opaque task transport is not canonical.");
    let task;
    try {
        task = JSON.parse(serialized);
    } catch {
        throw new TypeError("Managed opaque task transport is invalid.");
    }
    if (typeof task !== "string" || encodeManagedOpaqueTaskTransportV1(task) !== input) throw new TypeError("Managed opaque task transport is not canonical.");
    return task;
}
export const MANAGED_CONSUMER_ID_MAX_LENGTH = 64;
export const MANAGED_OPERATION_ID_ENCODED_LENGTH = 43;
export const MANAGED_REQUEST_ID_MAX_LENGTH = 128;
export const MANAGED_JSON_DEFAULT_LIMITS = Object.freeze({
    maxDepth: 32,
    maxNodes: 10_000,
    maxUtf8Bytes: 1_048_576,
    maxSerializedBytes: 1_048_576
});
const MANAGED_EXTENSION_SET_MAX_FILES = 128;
const MANAGED_EXTENSION_FILE_MAX_BYTES = 16 * 1024 * 1024;
const MANAGED_EXTENSION_SET_MAX_BYTES = 64 * 1024 * 1024;
const EXTENSION_CONTENT_DOMAIN = "pi-subagents/managed-dispatch/v1/extension-content";
const EXTENSION_SET_DOMAIN = "pi-subagents/managed-dispatch/v1/extension-set";
function hashManagedExtensionFile(filePath, maxBytes) {
    const noFollow = typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0;
    let descriptor;
    try {
        const pathStats = fs.lstatSync(filePath, {
            bigint: true
        });
        if (!pathStats.isFile()) throw new TypeError("Managed extension path must identify a no-follow regular file.");
        descriptor = fs.openSync(filePath, fs.constants.O_RDONLY | noFollow);
        const initial = fs.fstatSync(descriptor, {
            bigint: true
        });
        if (!initial.isFile() || initial.dev !== pathStats.dev || initial.ino !== pathStats.ino || initial.size < 0n || initial.size > BigInt(Math.min(MANAGED_EXTENSION_FILE_MAX_BYTES, maxBytes))) {
            throw new TypeError("Managed extension file is unavailable, replaced, or exceeds the per-file byte limit.");
        }
        const expectedBytes = Number(initial.size);
        const digest = createHash("sha256").update(EXTENSION_CONTENT_DOMAIN, "utf8").update("\0", "utf8");
        const buffer = Buffer.allocUnsafe(Math.min(64 * 1024, Math.max(1, expectedBytes)));
        let bytes = 0;
        while(bytes < expectedBytes){
            const bytesRead = fs.readSync(descriptor, buffer, 0, Math.min(buffer.length, expectedBytes - bytes), null);
            if (bytesRead === 0) throw new TypeError("Managed extension file changed while it was being attested.");
            digest.update(buffer.subarray(0, bytesRead));
            bytes += bytesRead;
        }
        if (fs.readSync(descriptor, buffer, 0, 1, null) !== 0) {
            throw new TypeError("Managed extension file changed while it was being attested.");
        }
        const final = fs.fstatSync(descriptor, {
            bigint: true
        });
        if (!final.isFile() || final.dev !== initial.dev || final.ino !== initial.ino || final.size !== initial.size || final.mtimeNs !== initial.mtimeNs || final.ctimeNs !== initial.ctimeNs) {
            throw new TypeError("Managed extension file changed while it was being attested.");
        }
        return {
            digest: digest.digest("hex"),
            bytes
        };
    } catch (error) {
        if (error instanceof TypeError) throw error;
        throw new TypeError("Managed extension file cannot be safely attested.");
    } finally{
        if (descriptor !== undefined) fs.closeSync(descriptor);
    }
}
export function computeManagedExtensionSetDigest(extensionFilePaths, cwd = process.cwd()) {
    if (!Array.isArray(extensionFilePaths) || utilTypes.isProxy(extensionFilePaths) || Object.getPrototypeOf(extensionFilePaths) !== Array.prototype) {
        throw new TypeError("Managed extension paths must be a plain array.");
    }
    const ownKeys = Reflect.ownKeys(extensionFilePaths);
    if (ownKeys.some((key)=>typeof key === "symbol" || key !== "length" && !/^(0|[1-9][0-9]*)$/.test(key))) {
        throw new TypeError("Managed extension paths must not contain custom fields.");
    }
    if (extensionFilePaths.length === 0) return null;
    if (extensionFilePaths.length > MANAGED_EXTENSION_SET_MAX_FILES) {
        throw new TypeError("Managed extension set exceeds the file-count limit.");
    }
    if (typeof cwd !== "string" || !path.isAbsolute(cwd) || path.resolve(cwd) !== cwd) {
        throw new TypeError("Managed extension cwd must be an absolute normalized path.");
    }
    const digests = [];
    let totalBytes = 0;
    for(let index = 0; index < extensionFilePaths.length; index++){
        const descriptor = Object.getOwnPropertyDescriptor(extensionFilePaths, String(index));
        if (!descriptor?.enumerable || !("value" in descriptor) || typeof descriptor.value !== "string" || descriptor.value.length === 0 || descriptor.value.includes("\0")) {
            throw new TypeError("Managed extension paths must contain non-empty string data entries.");
        }
        const attested = hashManagedExtensionFile(path.resolve(cwd, descriptor.value), MANAGED_EXTENSION_SET_MAX_BYTES - totalBytes);
        totalBytes += attested.bytes;
        digests.push(attested.digest);
    }
    digests.sort();
    const setHash = createHash("sha256").update(EXTENSION_SET_DOMAIN, "utf8").update("\0", "utf8");
    for (const digest of digests)setHash.update(digest, "ascii").update("\0", "utf8");
    return setHash.digest("hex");
}
export function projectManagedChildCapabilityV1(tools, extensionCwd = process.cwd()) {
    const effectiveTools = new Set([
        ...tools.declaredBuiltin,
        ...tools.effectiveMcpTools
    ]);
    return Object.freeze({
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        effectiveToolCount: effectiveTools.size,
        runtimeExtensionCount: tools.runtimeExtensions.length,
        configuredExtensionCount: tools.configuredExtensions.length,
        configuredExtensionSetDigest: computeManagedExtensionSetDigest(tools.configuredExtensions, extensionCwd),
        runtimeExtensionSetDigest: computeManagedExtensionSetDigest(tools.runtimeExtensions, extensionCwd),
        disableAmbientExtensions: tools.disableAmbientExtensions,
        fanoutAuthorized: tools.fanoutAuthorized
    });
}
export function parseManagedChildCapabilityV1(value) {
    if (!value || typeof value !== "object") throw new TypeError("Managed child capability must be an object.");
    const capability = assertExactKeys(value, [
        "version",
        "effectiveToolCount",
        "runtimeExtensionCount",
        "configuredExtensionCount",
        "configuredExtensionSetDigest",
        "runtimeExtensionSetDigest",
        "disableAmbientExtensions",
        "fanoutAuthorized"
    ], "Managed child capability");
    assertVersion(capability.version, "Managed child capability");
    const count = (name)=>{
        const candidate = capability[name];
        if (!Number.isSafeInteger(candidate) || candidate < 0) {
            throw new TypeError(`Managed child capability ${name} must be a non-negative safe integer.`);
        }
        return candidate;
    };
    const effectiveToolCount = count("effectiveToolCount");
    const runtimeExtensionCount = count("runtimeExtensionCount");
    const configuredExtensionCount = count("configuredExtensionCount");
    const extensionDigest = (name, extensionCount)=>{
        const candidate = capability[name];
        if (candidate === null && extensionCount === 0) return null;
        if (extensionCount > 0) return assertDigest(candidate, `Managed child capability ${name}`);
        throw new TypeError(`Managed child capability ${name} must be null exactly when its extension count is zero.`);
    };
    if (typeof capability.disableAmbientExtensions !== "boolean" || typeof capability.fanoutAuthorized !== "boolean") {
        throw new TypeError("Managed child capability flags must be booleans.");
    }
    return Object.freeze({
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        effectiveToolCount,
        runtimeExtensionCount,
        configuredExtensionCount,
        configuredExtensionSetDigest: extensionDigest("configuredExtensionSetDigest", configuredExtensionCount),
        runtimeExtensionSetDigest: extensionDigest("runtimeExtensionSetDigest", runtimeExtensionCount),
        disableAmbientExtensions: capability.disableAmbientExtensions,
        fanoutAuthorized: capability.fanoutAuthorized
    });
}
export const SUBAGENT_MANAGED_DISPATCH_REQUIREMENTS_V1 = Object.freeze({
    version: SUBAGENT_MANAGED_DISPATCH_VERSION,
    scope: "single-host",
    singleHost: true,
    namespace: "active-parent-session+consumerId+operationId",
    retention: "explicit",
    replay: "fail-closed",
    effects: "not-exactly-once"
});
const CONSUMER_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._~-]*$/;
const OPERATION_ID_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const REQUEST_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._~-]*$/;
const DIGEST_PATTERN = /^[a-f0-9]{64}$/;
const MUTATION_METHODS = new Set([
    "spawn",
    "resume",
    "steer",
    "interrupt",
    "stop",
    "retire"
]);
const PROFILE_CONTENT_DOMAIN = "pi-subagents/managed-dispatch/v1/profile-content";
const PROFILE_IDENTITY_DOMAIN = "pi-subagents/managed-dispatch/v1/profile-identity";
const REQUEST_DOMAIN = "pi-subagents/managed-dispatch/v1/request";
const ARRAY_INDEX_PATTERN = /^(0|[1-9][0-9]*)$/;
export function assertManagedConsumerId(value) {
    if (typeof value !== "string" || value.length === 0 || value.length > MANAGED_CONSUMER_ID_MAX_LENGTH || !CONSUMER_ID_PATTERN.test(value)) {
        throw new TypeError(`Managed consumerId must be a 1-${MANAGED_CONSUMER_ID_MAX_LENGTH} character safe token.`);
    }
    return value;
}
export function createManagedOperationId() {
    return randomBytes(32).toString("base64url");
}
export function assertManagedOperationId(value) {
    if (typeof value !== "string" || value.length !== MANAGED_OPERATION_ID_ENCODED_LENGTH || !OPERATION_ID_PATTERN.test(value)) {
        throw new TypeError("Managed operationId must be canonical 43-character base64url encoding of 32 bytes.");
    }
    let bytes;
    try {
        bytes = Buffer.from(value, "base64url");
    } catch  {
        throw new TypeError("Managed operationId must be canonical 43-character base64url encoding of 32 bytes.");
    }
    if (bytes.length !== 32 || bytes.toString("base64url") !== value) {
        throw new TypeError("Managed operationId must be canonical 43-character base64url encoding of 32 bytes.");
    }
    return value;
}
export function assertManagedResumeExecutorRequestV1(value, sourceRunId, index) {
    const normalized = canonicalizeManagedJson(value).normalized;
    if (!normalized || typeof normalized !== "object" || Array.isArray(normalized)) {
        throw new TypeError("Managed resume executor request must be an object.");
    }
    const record = normalized;
    const expected = [
        "action",
        "async",
        "clarify",
        "context",
        "index",
        "message",
        "runId"
    ].sort();
    if (Object.keys(record).sort().join("\0") !== expected.join("\0")) {
        throw new TypeError("Managed resume executor request contains missing or unknown fields.");
    }
    if (record.action !== "resume" || record.async !== true || record.clarify !== false || record.context !== "fresh") {
        throw new TypeError("Managed resume executor request fixed fields are invalid.");
    }
    if (typeof sourceRunId !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._~:-]{0,255}$/.test(sourceRunId) || record.runId !== sourceRunId || index !== 0 || record.index !== 0) {
        throw new TypeError("Managed resume executor request source identity is invalid.");
    }
    if (typeof record.message !== "string" || !record.message.trim() || record.message.includes("\0") || hasUnpairedSurrogate(record.message) || Buffer.byteLength(record.message, "utf8") > 65_536) {
        throw new TypeError("Managed resume executor request message is invalid.");
    }
    return normalized;
}
function rejectProxy(value, location) {
    if (utilTypes.isProxy(value)) throw new TypeError(`Managed JSON rejects proxies at ${location}.`);
}
function plainDataRecord(value, label) {
    rejectProxy(value, label);
    if (Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) throw new TypeError(`${label} must be a plain object.`);
    const ownKeys = Reflect.ownKeys(value);
    if (ownKeys.some((key)=>typeof key === "symbol")) throw new TypeError(`${label} contains an unknown symbol field.`);
    const keys = ownKeys;
    for (const key of keys){
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (!descriptor.enumerable || !("value" in descriptor)) throw new TypeError(`${label} fields must be enumerable data properties.`);
    }
    return {
        record: value,
        keys
    };
}
function assertObjectKeys(value, required, optional, label) {
    const { record, keys } = plainDataRecord(value, label);
    const allowed = new Set([
        ...required,
        ...optional
    ]);
    if (keys.some((key)=>!allowed.has(key)) || required.some((key)=>!keys.includes(key))) {
        throw new TypeError(`${label} contains missing or unknown fields.`);
    }
    return record;
}
function assertExactKeys(value, expected, label) {
    return assertObjectKeys(value, expected, [], label);
}
function hasUnpairedSurrogate(value) {
    for(let index = 0; index < value.length; index++){
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
function normalizeManagedJsonLimits(limits) {
    if (!limits || typeof limits !== "object") throw new TypeError("Managed JSON limits must be a plain object.");
    const record = assertObjectKeys(limits, [], [
        "maxDepth",
        "maxNodes",
        "maxUtf8Bytes",
        "maxSerializedBytes"
    ], "Managed JSON limits");
    const normalized = {
        ...MANAGED_JSON_DEFAULT_LIMITS
    };
    for (const name of [
        "maxDepth",
        "maxNodes",
        "maxUtf8Bytes",
        "maxSerializedBytes"
    ]){
        if (!Object.prototype.hasOwnProperty.call(record, name)) continue;
        const value = record[name];
        if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${name} must be a non-negative safe integer.`);
        normalized[name] = value;
    }
    return normalized;
}
function quoteManagedJsonString(value) {
    let serialization = '"';
    for(let index = 0; index < value.length; index++){
        const code = value.charCodeAt(index);
        switch(code){
            case 0x08:
                serialization += "\\b";
                break;
            case 0x09:
                serialization += "\\t";
                break;
            case 0x0a:
                serialization += "\\n";
                break;
            case 0x0c:
                serialization += "\\f";
                break;
            case 0x0d:
                serialization += "\\r";
                break;
            case 0x22:
                serialization += '\\"';
                break;
            case 0x5c:
                serialization += "\\\\";
                break;
            default:
                serialization += code < 0x20 ? `\\u${code.toString(16).padStart(4, "0")}` : value[index];
        }
    }
    return `${serialization}"`;
}
function serializeNormalizedManagedJson(value, maxSerializedBytes) {
    const chunks = [];
    let serializedBytes = 0;
    const append = (chunk)=>{
        serializedBytes += Buffer.byteLength(chunk, "utf8");
        if (serializedBytes > maxSerializedBytes) throw new TypeError(`Managed JSON exceeds maxSerializedBytes (${maxSerializedBytes}).`);
        chunks.push(chunk);
    };
    const serialize = (current)=>{
        if (current === null) {
            append("null");
        } else if (typeof current === "boolean" || typeof current === "number") {
            append(String(current));
        } else if (typeof current === "string") {
            append(quoteManagedJsonString(current));
        } else if (Array.isArray(current)) {
            append("[");
            for(let index = 0; index < current.length; index++){
                if (index > 0) append(",");
                serialize(current[index]);
            }
            append("]");
        } else {
            append("{");
            const keys = Reflect.ownKeys(current).sort();
            for(let index = 0; index < keys.length; index++){
                if (index > 0) append(",");
                const key = keys[index];
                append(quoteManagedJsonString(key));
                append(":");
                serialize(Object.getOwnPropertyDescriptor(current, key).value);
            }
            append("}");
        }
    };
    serialize(value);
    return chunks.join("");
}
export function canonicalizeManagedJson(input, limits = {}) {
    const { maxDepth, maxNodes, maxUtf8Bytes, maxSerializedBytes } = normalizeManagedJsonLimits(limits);
    const seen = new WeakSet();
    let nodes = 0;
    let utf8Bytes = 0;
    const countString = (value, location)=>{
        if (hasUnpairedSurrogate(value)) throw new TypeError(`Managed JSON rejects unpaired UTF-16 surrogates at ${location}.`);
        utf8Bytes += Buffer.byteLength(value, "utf8");
        if (utf8Bytes > maxUtf8Bytes) throw new TypeError(`Managed JSON exceeds maxUtf8Bytes (${maxUtf8Bytes}).`);
    };
    const visit = (value, depth, location)=>{
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
            const length = Object.getOwnPropertyDescriptor(value, "length").value;
            if (length > maxNodes - nodes) throw new TypeError(`Managed JSON exceeds maxNodes (${maxNodes}).`);
            const ownKeys = Reflect.ownKeys(value);
            if (ownKeys.length !== length + 1) throw new TypeError(`Managed JSON rejects sparse arrays at ${location}.`);
            for (const key of ownKeys){
                if (typeof key === "symbol") throw new TypeError(`Managed JSON rejects symbol properties at ${location}.`);
                if (key === "length") continue;
                if (!ARRAY_INDEX_PATTERN.test(key) || Number(key) >= length) throw new TypeError(`Managed JSON rejects custom array fields at ${location}.`);
                const descriptor = Object.getOwnPropertyDescriptor(value, key);
                if (!descriptor.enumerable) throw new TypeError(`Managed JSON rejects non-enumerable properties at ${location}.`);
                if (!("value" in descriptor)) throw new TypeError(`Managed JSON rejects accessors at ${location}.`);
            }
            const normalized = [];
            for(let index = 0; index < length; index++){
                const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
                if (!descriptor) throw new TypeError(`Managed JSON rejects sparse arrays at ${location}.`);
                normalized.push(visit(descriptor.value, depth + 1, `${location}[${index}]`));
            }
            return Object.freeze(normalized);
        }
        if (Object.getPrototypeOf(value) !== Object.prototype) throw new TypeError(`Managed JSON rejects custom object prototypes at ${location}.`);
        const ownKeys = Reflect.ownKeys(value);
        if (ownKeys.length > maxNodes - nodes) throw new TypeError(`Managed JSON exceeds maxNodes (${maxNodes}).`);
        const stringKeys = [];
        for (const key of ownKeys){
            if (typeof key === "symbol") throw new TypeError(`Managed JSON rejects symbol properties at ${location}.`);
            const descriptor = Object.getOwnPropertyDescriptor(value, key);
            if (!descriptor.enumerable) throw new TypeError(`Managed JSON rejects non-enumerable properties at ${location}.${key}.`);
            if (!("value" in descriptor)) throw new TypeError(`Managed JSON rejects accessors at ${location}.${key}.`);
            countString(key, `${location} key`);
            stringKeys.push(key);
        }
        stringKeys.sort();
        const normalized = {};
        for (const key of stringKeys){
            const descriptor = Object.getOwnPropertyDescriptor(value, key);
            Object.defineProperty(normalized, key, {
                value: visit(descriptor.value, depth + 1, `${location}.${key}`),
                enumerable: true,
                configurable: true,
                writable: true
            });
        }
        return Object.freeze(normalized);
    };
    const normalized = visit(input, 0, "$Root");
    const serialization = serializeNormalizedManagedJson(normalized, maxSerializedBytes);
    return Object.freeze({
        normalized,
        serialization
    });
}
function sha256Domain(domain, serialization) {
    return createHash("sha256").update(domain, "utf8").update("\0", "utf8").update(serialization, "utf8").digest("hex");
}
export function computeManagedProfileContentDigest(content) {
    return sha256Domain(PROFILE_CONTENT_DOMAIN, canonicalizeManagedJson(content).serialization);
}
function assertVersion(value, label) {
    if (value !== SUBAGENT_MANAGED_DISPATCH_VERSION) throw new TypeError(`${label} has an unsupported version.`);
}
function assertRequestId(value) {
    if (typeof value !== "string" || value.length === 0 || value.length > MANAGED_REQUEST_ID_MAX_LENGTH || !REQUEST_ID_PATTERN.test(value)) {
        throw new TypeError(`Managed requestId must be a 1-${MANAGED_REQUEST_ID_MAX_LENGTH} character safe token.`);
    }
    return value;
}
function assertBoundedText(value, label, maxUtf8Bytes) {
    if (typeof value !== "string" || value.length === 0 || hasUnpairedSurrogate(value) || Buffer.byteLength(value, "utf8") > maxUtf8Bytes || value.includes("\0")) {
        throw new TypeError(`${label} must be a non-empty well-formed string of at most ${maxUtf8Bytes} UTF-8 bytes without NUL.`);
    }
    return value;
}
function assertSafeIdentifier(value, label) {
    const text = assertBoundedText(value, label, 256);
    if (!/^[A-Za-z0-9][A-Za-z0-9._~:-]*$/.test(text)) throw new TypeError(`${label} must be a safe identifier.`);
    return text;
}
function assertRootPath(value, label) {
    const text = assertBoundedText(value, label, 4_096);
    if ([
        ...text
    ].some((character)=>{
        const code = character.codePointAt(0) ?? 0;
        return code <= 31 || code >= 127 && code <= 159;
    })) throw new TypeError(`${label} must not contain control characters.`);
    return text;
}
function assertDigest(value, label) {
    if (typeof value !== "string" || !DIGEST_PATTERN.test(value)) throw new TypeError(`${label} must be a lowercase SHA-256 hex digest.`);
    return value;
}
function normalizeRootIdentity(value) {
    if (!value || typeof value !== "object") throw new TypeError("Managed root identity must be an object.");
    const root = assertObjectKeys(value, [
        "version",
        "realPath"
    ], [
        "device",
        "inode"
    ], "Managed root identity");
    assertVersion(root.version, "Managed root identity");
    const normalized = {
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        realPath: assertRootPath(root.realPath, "Managed root realPath")
    };
    for (const key of [
        "device",
        "inode"
    ]){
        if (Object.prototype.hasOwnProperty.call(root, key)) normalized[key] = assertSafeIdentifier(root[key], `Managed root ${key}`);
    }
    return normalized;
}
export function computeManagedProfileIdentityDigest(identity) {
    if (!identity || typeof identity !== "object") throw new TypeError("Managed profile identity must be an object.");
    const profile = assertExactKeys(identity, [
        "version",
        "contentDigest",
        "root"
    ], "Managed profile identity");
    assertVersion(profile.version, "Managed profile identity");
    const normalized = {
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        contentDigest: assertDigest(profile.contentDigest, "Managed profile contentDigest"),
        root: normalizeRootIdentity(profile.root)
    };
    return sha256Domain(PROFILE_IDENTITY_DOMAIN, canonicalizeManagedJson(normalized).serialization);
}
function normalizeProfileSnapshot(value) {
    if (!value || typeof value !== "object") throw new TypeError("Managed profile snapshot must be an object.");
    const profile = assertExactKeys(value, [
        "version",
        "root",
        "content"
    ], "Managed profile snapshot");
    assertVersion(profile.version, "Managed profile snapshot");
    return {
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        root: assertRootPath(profile.root, "Managed profile snapshot root"),
        content: canonicalizeManagedJson(profile.content).normalized
    };
}
function normalizeExpectedLaunch(value) {
    if (!value || typeof value !== "object") throw new TypeError("Managed expected launch must be an object.");
    const expected = assertObjectKeys(value, [
        "version",
        "hostId",
        "candidateRunId",
        "profileIdentityDigest",
        "contractDigest"
    ], [
        "parentSessionIdentityDigest"
    ], "Managed expected launch");
    assertVersion(expected.version, "Managed expected launch");
    const normalized = {
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        hostId: assertSafeIdentifier(expected.hostId, "Managed expected launch hostId"),
        candidateRunId: assertSafeIdentifier(expected.candidateRunId, "Managed expected launch candidateRunId"),
        profileIdentityDigest: assertDigest(expected.profileIdentityDigest, "Managed expected launch profileIdentityDigest"),
        contractDigest: assertDigest(expected.contractDigest, "Managed expected launch contractDigest")
    };
    if (Object.prototype.hasOwnProperty.call(expected, "parentSessionIdentityDigest")) {
        normalized.parentSessionIdentityDigest = assertDigest(expected.parentSessionIdentityDigest, "Managed expected launch parentSessionIdentityDigest");
    }
    return normalized;
}
function normalizeTarget(value, expectedConsumerId) {
    if (!value || typeof value !== "object") throw new TypeError("Managed operation target must be an object.");
    const { record, keys } = plainDataRecord(value, "Managed operation target");
    const operationTarget = keys.length === 2 && keys.includes("consumerId") && keys.includes("operationId");
    const runTarget = keys.length === 2 && keys.includes("consumerId") && keys.includes("runId");
    if (!operationTarget && !runTarget) throw new TypeError("Managed operation target must contain exactly consumerId and one of operationId or runId.");
    const consumerId = assertManagedConsumerId(record.consumerId);
    if (expectedConsumerId !== undefined && consumerId !== expectedConsumerId) {
        throw new TypeError("Managed control target consumerId must match the mutation namespace consumerId.");
    }
    return operationTarget ? {
        consumerId,
        operationId: assertManagedOperationId(record.operationId)
    } : {
        consumerId,
        runId: assertSafeIdentifier(record.runId, "Managed target runId")
    };
}
function normalizeExecutorRequest(value, label) {
    const normalized = canonicalizeManagedJson(value).normalized;
    if (normalized === null || typeof normalized !== "object" || Array.isArray(normalized)) {
        throw new TypeError(`${label} must be a plain JSON object.`);
    }
    return normalized;
}
function normalizeSpawnInput(value) {
    if (!value || typeof value !== "object") throw new TypeError("Managed spawn input must be an object.");
    const input = assertExactKeys(value, [
        "request"
    ], "Managed spawn input");
    return {
        request: normalizeExecutorRequest(input.request, "Managed spawn executor request")
    };
}
function normalizeResumeInput(value) {
    if (!value || typeof value !== "object") throw new TypeError("Managed resume input must be an object.");
    const input = assertExactKeys(value, [
        "sourceRunId",
        "index",
        "request"
    ], "Managed resume input");
    if (!Number.isSafeInteger(input.index) || input.index < 0 || input.index > 1_000_000) {
        throw new TypeError("Managed resume index must be an integer between 0 and 1000000.");
    }
    return {
        sourceRunId: assertSafeIdentifier(input.sourceRunId, "Managed resume sourceRunId"),
        index: input.index,
        request: normalizeExecutorRequest(input.request, "Managed resume executor request")
    };
}
function normalizeControlInput(method, value, expectedConsumerId) {
    if (!value || typeof value !== "object") throw new TypeError(`Managed ${method} input must be an object.`);
    if (method === "steer") {
        const input = assertExactKeys(value, [
            "target",
            "message"
        ], "Managed steer input");
        return {
            target: normalizeTarget(input.target, expectedConsumerId),
            message: assertBoundedText(input.message, "Managed steer message", 65_536)
        };
    }
    if (method === "retire") {
        const input = assertObjectKeys(value, [
            "target"
        ], [
            "acknowledgeUncertain"
        ], "Managed retire input");
        const normalized = {
            target: normalizeTarget(input.target, expectedConsumerId)
        };
        if (Object.prototype.hasOwnProperty.call(input, "acknowledgeUncertain")) {
            if (typeof input.acknowledgeUncertain !== "boolean") throw new TypeError("Managed retire acknowledgeUncertain must be a boolean.");
            normalized.acknowledgeUncertain = input.acknowledgeUncertain;
        }
        return normalized;
    }
    const input = assertExactKeys(value, [
        "target"
    ], `Managed ${method} input`);
    return {
        target: normalizeTarget(input.target, expectedConsumerId)
    };
}
function normalizeManagedMutationRequest(request) {
    if (!request || typeof request !== "object") throw new TypeError("Managed mutation request must be an object.");
    const inspected = plainDataRecord(request, "Managed mutation request");
    const method = inspected.record.method;
    if (typeof method !== "string" || !MUTATION_METHODS.has(method)) throw new TypeError("Managed mutation request has an unsupported method.");
    const launchMethod = method === "spawn" || method === "resume";
    const envelope = assertExactKeys(request, launchMethod ? [
        "version",
        "requestId",
        "method",
        "managed",
        "expectedLaunch",
        "input"
    ] : [
        "version",
        "requestId",
        "method",
        "managed",
        "input"
    ], "Managed mutation request");
    assertVersion(envelope.version, "Managed mutation request");
    const requestId = assertRequestId(envelope.requestId);
    if (!envelope.managed || typeof envelope.managed !== "object") throw new TypeError("Managed mutation request managed discriminator must be an object.");
    const managed = assertExactKeys(envelope.managed, [
        "version",
        "consumerId",
        "operationId"
    ], "Managed mutation discriminator");
    assertVersion(managed.version, "Managed mutation discriminator");
    const consumerId = assertManagedConsumerId(managed.consumerId);
    const operationId = assertManagedOperationId(managed.operationId);
    const input = method === "spawn" ? normalizeSpawnInput(envelope.input) : method === "resume" ? normalizeResumeInput(envelope.input) : normalizeControlInput(method, envelope.input, consumerId);
    const normalized = {
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        requestId,
        method,
        managed: {
            version: SUBAGENT_MANAGED_DISPATCH_VERSION,
            consumerId,
            operationId
        },
        input
    };
    if (launchMethod) normalized.expectedLaunch = normalizeExpectedLaunch(envelope.expectedLaunch);
    return normalized;
}
export function parseManagedReadRequestV1(request) {
    if (!request || typeof request !== "object") throw new TypeError("Managed read request must be an object.");
    const inspected = plainDataRecord(request, "Managed read request").record;
    const method = inspected.method;
    if (method === "capabilities") {
        const envelope = assertExactKeys(request, [
            "version",
            "requestId",
            "method"
        ], "Managed capabilities request");
        assertVersion(envelope.version, "Managed capabilities request");
        if (envelope.method !== "capabilities") throw new TypeError("Managed capabilities request method is invalid.");
        return canonicalizeManagedJson({
            version: SUBAGENT_MANAGED_DISPATCH_VERSION,
            requestId: assertRequestId(envelope.requestId),
            method
        }).normalized;
    }
    if (method !== "status" && method !== "details") throw new TypeError("Managed read request has an unsupported method.");
    const envelope = assertExactKeys(request, [
        "version",
        "requestId",
        "method",
        "target"
    ], `Managed ${method} request`);
    assertVersion(envelope.version, `Managed ${method} request`);
    return canonicalizeManagedJson({
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        requestId: assertRequestId(envelope.requestId),
        method,
        target: normalizeTarget(envelope.target)
    }).normalized;
}
export function parseManagedMutationRequestV1(request) {
    return canonicalizeManagedJson(normalizeManagedMutationRequest(request)).normalized;
}
export function parseManagedPreflightRequestV1(request) {
    if (!request || typeof request !== "object") throw new TypeError("Managed preflight request must be an object.");
    const envelope = assertExactKeys(request, [
        "version",
        "requestId",
        "method",
        "consumerId",
        "input"
    ], "Managed preflight request");
    assertVersion(envelope.version, "Managed preflight request");
    const requestId = assertRequestId(envelope.requestId);
    if (envelope.method !== "preflight") throw new TypeError("Managed preflight request method must be 'preflight'.");
    const consumerId = assertManagedConsumerId(envelope.consumerId);
    if (!envelope.input || typeof envelope.input !== "object") throw new TypeError("Managed preflight input must be an object.");
    const inspectedInput = plainDataRecord(envelope.input, "Managed preflight input").record;
    const kind = inspectedInput.kind;
    let input;
    if (kind === "spawn") {
        const exact = assertExactKeys(envelope.input, [
            "kind",
            "request"
        ], "Managed spawn preflight input");
        input = {
            kind,
            ...normalizeSpawnInput({
                request: exact.request
            })
        };
    } else if (kind === "resume") {
        const exact = assertExactKeys(envelope.input, [
            "kind",
            "sourceRunId",
            "index",
            "request"
        ], "Managed resume preflight input");
        input = {
            kind,
            ...normalizeResumeInput({
                sourceRunId: exact.sourceRunId,
                index: exact.index,
                request: exact.request
            })
        };
    } else {
        throw new TypeError("Managed preflight input kind must be 'spawn' or 'resume'.");
    }
    return canonicalizeManagedJson({
        version: SUBAGENT_MANAGED_DISPATCH_VERSION,
        requestId,
        method: "preflight",
        consumerId,
        input
    }).normalized;
}
export function computeManagedRequestDigest(request) {
    const parsed = parseManagedMutationRequestV1(request);
    const semantic = {
        protocolVersion: SUBAGENT_MANAGED_DISPATCH_VERSION,
        method: parsed.method,
        consumerId: parsed.managed.consumerId,
        operationId: parsed.managed.operationId,
        input: parsed.input
    };
    if (parsed.method === "spawn" || parsed.method === "resume") {
        semantic.expectedLaunch = parsed.expectedLaunch;
    }
    return sha256Domain(REQUEST_DOMAIN, canonicalizeManagedJson(semantic).serialization);
}
export function managedDispatchReplyEvent(requestId) {
    return `${SUBAGENT_MANAGED_DISPATCH_REPLY_EVENT_PREFIX}${assertRequestId(requestId)}`;
}

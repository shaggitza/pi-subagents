import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { describe, it } from "node:test";
import {
	MANAGED_CONSUMER_ID_MAX_LENGTH,
	MANAGED_OPERATION_ID_ENCODED_LENGTH,
	MANAGED_REQUEST_ID_MAX_LENGTH,
	SUBAGENT_MANAGED_DISPATCH_REPLY_EVENT_PREFIX,
	SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT,
	SUBAGENT_MANAGED_DISPATCH_REQUIREMENTS_V1,
	SUBAGENT_MANAGED_DISPATCH_VERSION,
	assertManagedConsumerId,
	assertManagedOperationId,
	assertManagedResumeExecutorRequestV1,
	canonicalizeManagedJson,
	computeManagedProfileContentDigest,
	computeManagedProfileIdentityDigest,
	computeManagedRequestDigest,
	createManagedOperationId,
	managedDispatchReplyEvent,
	parseManagedMutationRequestV1,
	parseManagedPreflightRequestV1,
	parseManagedReadRequestV1,
	type ManagedPreflightRequestV1,
	type ManagedPreflightResultV1,
} from "../../src/api/managed-dispatch.ts";

function operationId(byte = 7): string {
	return Buffer.alloc(32, byte).toString("base64url");
}

function profile() {
	return {
		version: 1,
		root: "/repo",
		content: { flags: [true, null], retries: 2 },
	} as const;
}

function expectedLaunch(overrides: Record<string, unknown> = {}): Record<string, unknown> {
	return {
		version: 1,
		hostId: "host-1",
		candidateRunId: "candidate-1",
		profileIdentityDigest: "a".repeat(64),
		parentSessionIdentityDigest: "e".repeat(64),
		contractDigest: "b".repeat(64),
		...overrides,
	};
}

function mutationBase(method: string, input: unknown, overrides: Record<string, unknown> = {}): Record<string, unknown> {
	return {
		version: 1,
		requestId: "transport-1",
		method,
		managed: { version: 1, consumerId: "pi-signal", operationId: operationId() },
		input,
		...overrides,
	};
}

function initialExecutorRequest() {
	return {
		agent: "worker",
		task: "inspect",
		context: "fresh",
		async: true,
		clarify: false,
		cwd: "/repo",
		model: "provider/model",
		artifacts: false,
		output: false,
		sessionDir: "/private/sessions/operation-1",
	};
}

function spawnRequest(overrides: Record<string, unknown> = {}): Record<string, unknown> {
	return mutationBase(
		"spawn",
		{ request: initialExecutorRequest() },
		{ expectedLaunch: expectedLaunch(), ...overrides },
	);
}

function resumeRequest(overrides: Record<string, unknown> = {}): Record<string, unknown> {
	return mutationBase(
		"resume",
		{
			sourceRunId: "source-1",
			index: 0,
			request: { action: "resume", runId: "source-1", index: 0, message: "continue", async: true, clarify: false, context: "fresh" },
		},
		{ expectedLaunch: expectedLaunch(), ...overrides },
	);
}

function controlRequest(method: "steer" | "interrupt" | "stop" | "retire", overrides: Record<string, unknown> = {}): Record<string, unknown> {
	const target = { consumerId: "pi-signal", operationId: operationId(9) };
	const input = method === "steer"
		? { target, message: "adjust course" }
		: method === "retire"
			? { target, acknowledgeUncertain: true }
			: { target };
	return mutationBase(method, input, overrides);
}

function restoreProperty(target: object, key: PropertyKey, descriptor: PropertyDescriptor | undefined): void {
	if (descriptor) Object.defineProperty(target, key, descriptor);
	else delete (target as Record<PropertyKey, unknown>)[key];
}

describe("managed-dispatch public protocol foundation", () => {
	it("freezes exact protocol, event, and non-advertising requirements vocabulary", () => {
		assert.equal(SUBAGENT_MANAGED_DISPATCH_VERSION, 1);
		assert.equal(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, "subagents:managed-dispatch:v1:request");
		assert.equal(SUBAGENT_MANAGED_DISPATCH_REPLY_EVENT_PREFIX, "subagents:managed-dispatch:v1:reply:");
		assert.equal(managedDispatchReplyEvent("abc-123"), "subagents:managed-dispatch:v1:reply:abc-123");
		assert.deepEqual(SUBAGENT_MANAGED_DISPATCH_REQUIREMENTS_V1, {
			version: 1,
			scope: "single-host",
			singleHost: true,
			namespace: "active-parent-session+consumerId+operationId",
			retention: "explicit",
			replay: "fail-closed",
			effects: "not-exactly-once",
		});
		assert.equal(Object.isFrozen(SUBAGENT_MANAGED_DISPATCH_REQUIREMENTS_V1), true);
		const vocabulary = JSON.stringify(SUBAGENT_MANAGED_DISPATCH_REQUIREMENTS_V1);
		assert.doesNotMatch(vocabulary, /available|durable|exactlyOnce/);
	});

	it("generates and asserts canonical 32-byte operation IDs", () => {
		const generated = new Set(Array.from({ length: 64 }, () => createManagedOperationId()));
		assert.equal(generated.size, 64);
		for (const id of generated) {
			assert.equal(id.length, MANAGED_OPERATION_ID_ENCODED_LENGTH);
			assert.match(id, /^[A-Za-z0-9_-]{43}$/);
			assert.equal(Buffer.from(id, "base64url").length, 32);
			assert.equal(assertManagedOperationId(id), id);
		}

		const canonical = Buffer.alloc(32).toString("base64url");
		const nonCanonicalTrailingBits = `${canonical.slice(0, -1)}B`;
		for (const invalid of ["", canonical.slice(1), `${canonical}=`, `${canonical}A`, "a".repeat(43), "0".repeat(64)]) {
			assert.throws(() => assertManagedOperationId(invalid), /canonical 43-character base64url/);
		}
		assert.throws(() => assertManagedOperationId(nonCanonicalTrailingBits), /canonical 43-character base64url/);
	});

	it("accepts only a closed exact managed resume executor request", () => {
		const valid = { action: "resume", runId: "source-1", index: 0, message: "continue", async: true, clarify: false, context: "fresh" };
		const parsed = assertManagedResumeExecutorRequestV1(valid, "source-1", 0);
		assert.deepEqual(parsed, valid);
		assert.equal(Object.isFrozen(parsed), true);
		for (const invalid of [
			{ ...valid, id: "source-1" },
			{ ...valid, runId: "source-prefix" },
			{ ...valid, index: 1 },
			{ ...valid, message: "" },
			{ ...valid, context: "fork" },
			{ ...valid, chain: [] },
			{ ...valid, dir: "/tmp/run" },
		]) assert.throws(() => assertManagedResumeExecutorRequestV1(invalid, "source-1", 0));
		assert.throws(() => assertManagedResumeExecutorRequestV1(valid, "source-1", 1));
	});

	it("bounds consumer IDs to safe namespace-only tokens", () => {
		assert.equal(assertManagedConsumerId("pi-signal.v1_~"), "pi-signal.v1_~");
		assert.equal(assertManagedConsumerId("a".repeat(MANAGED_CONSUMER_ID_MAX_LENGTH)), "a".repeat(MANAGED_CONSUMER_ID_MAX_LENGTH));
		for (const invalid of ["", "-starts-with-punctuation", "has/slash", "has space", "line\nbreak", "a".repeat(MANAGED_CONSUMER_ID_MAX_LENGTH + 1)]) {
			assert.throws(() => assertManagedConsumerId(invalid), /safe token/);
		}
	});

	it("restricts dynamic reply request IDs to bounded safe tokens", () => {
		assert.equal(managedDispatchReplyEvent("A._~-9"), `${SUBAGENT_MANAGED_DISPATCH_REPLY_EVENT_PREFIX}A._~-9`);
		assert.equal(managedDispatchReplyEvent("a".repeat(MANAGED_REQUEST_ID_MAX_LENGTH)).length, SUBAGENT_MANAGED_DISPATCH_REPLY_EVENT_PREFIX.length + MANAGED_REQUEST_ID_MAX_LENGTH);
		for (const invalid of [
			"",
			"-prefix",
			"has space",
			"has/slash",
			"has:separator",
			"line\nbreak",
			"tab\there",
			"control\u0000",
			"surrogate\ud800",
			"emoji-😀",
			"a".repeat(MANAGED_REQUEST_ID_MAX_LENGTH + 1),
		]) {
			assert.throws(() => managedDispatchReplyEvent(invalid), /requestId.*safe token/);
			assert.throws(() => computeManagedRequestDigest(spawnRequest({ requestId: invalid })), /requestId.*safe token/);
		}
	});

	it("canonicalizes reordered plain JSON identically and deeply freezes a normalized copy", () => {
		const left = canonicalizeManagedJson({ z: [3, { beta: true, alpha: null }], a: "value" });
		const right = canonicalizeManagedJson({ a: "value", z: [3, { alpha: null, beta: true }] });
		assert.equal(left.serialization, '{"a":"value","z":[3,{"alpha":null,"beta":true}]}');
		assert.equal(right.serialization, left.serialization);
		assert.notEqual(left.normalized, right.normalized);
		const normalized = left.normalized as { a: string; z: Array<number | Record<string, unknown>> };
		assert.equal(Object.isFrozen(left), true);
		assert.equal(Object.isFrozen(normalized), true);
		assert.equal(Object.isFrozen(normalized.z), true);
		assert.equal(Object.isFrozen(normalized.z[1]), true);
		assert.throws(() => { normalized.a = "changed"; }, TypeError);
		assert.throws(() => { normalized.z.push(4); }, TypeError);
		assert.throws(() => { delete (normalized.z[1] as Record<string, unknown>).alpha; }, TypeError);
	});

	it("serializes explicitly despite prototype toJSON pollution and orders integer-like keys lexically", () => {
		const objectToJson = Object.getOwnPropertyDescriptor(Object.prototype, "toJSON");
		const arrayToJson = Object.getOwnPropertyDescriptor(Array.prototype, "toJSON");
		try {
			Object.defineProperty(Object.prototype, "toJSON", { configurable: true, value: () => "polluted-object" });
			Object.defineProperty(Array.prototype, "toJSON", { configurable: true, value: () => "polluted-array" });
			assert.equal(canonicalizeManagedJson({ nested: [1, { ok: true }] }).serialization, '{"nested":[1,{"ok":true}]}');
			assert.equal(canonicalizeManagedJson({ "2": "two", "10": "ten" }).serialization, '{"10":"ten","2":"two"}');
		} finally {
			restoreProperty(Object.prototype, "toJSON", objectToJson);
			restoreProperty(Array.prototype, "toJSON", arrayToJson);
		}
	});

	it("rejects every hostile or lossy JSON shape, including proxies without invoking traps", () => {
		const accessor = {} as Record<string, unknown>;
		Object.defineProperty(accessor, "value", { enumerable: true, get: () => 1 });
		const nonEnumerable = { visible: true } as Record<string, unknown>;
		Object.defineProperty(nonEnumerable, "hidden", { value: true });
		const symbolProperty = { visible: true } as Record<PropertyKey, unknown>;
		symbolProperty[Symbol("hidden")] = true;
		const sparse = Array(2);
		sparse[1] = "present";
		const customArray = [1] as unknown[] & { extra?: boolean };
		customArray.extra = true;
		const customPrototype = Object.create({ inherited: true }) as Record<string, unknown>;
		customPrototype.value = 1;
		const cycle: Record<string, unknown> = {};
		cycle.self = cycle;
		const shared = { value: true };
		let trapCalls = 0;
		const trapped = new Proxy({ value: 1 }, {
			get: () => { trapCalls++; throw new Error("get trap invoked"); },
			getOwnPropertyDescriptor: () => { trapCalls++; throw new Error("descriptor trap invoked"); },
			getPrototypeOf: () => { trapCalls++; throw new Error("prototype trap invoked"); },
			ownKeys: () => { trapCalls++; throw new Error("ownKeys trap invoked"); },
		});
		const revoked = Proxy.revocable({ value: 1 }, {});
		revoked.revoke();

		const invalid: Array<{ label: string; value: unknown }> = [
			{ label: "proxy", value: trapped },
			{ label: "revoked proxy", value: revoked.proxy },
			{ label: "accessor", value: accessor },
			{ label: "symbol property", value: symbolProperty },
			{ label: "non-enumerable", value: nonEnumerable },
			{ label: "undefined object value", value: { value: undefined } },
			{ label: "undefined array value", value: [undefined] },
			{ label: "sparse array", value: sparse },
			{ label: "custom array field", value: customArray },
			{ label: "custom prototype", value: customPrototype },
			{ label: "null prototype", value: Object.assign(Object.create(null), { value: 1 }) },
			{ label: "date", value: new Date(0) },
			{ label: "cycle", value: cycle },
			{ label: "shared reference", value: { a: shared, b: shared } },
			{ label: "NaN", value: Number.NaN },
			{ label: "positive infinity", value: Number.POSITIVE_INFINITY },
			{ label: "negative infinity", value: Number.NEGATIVE_INFINITY },
			{ label: "negative zero", value: -0 },
			{ label: "bigint", value: 1n },
			{ label: "function", value: () => undefined },
			{ label: "symbol", value: Symbol("value") },
			{ label: "unpaired surrogate", value: "\ud800" },
		];
		for (const entry of invalid) assert.throws(() => canonicalizeManagedJson(entry.value), undefined, entry.label);
		assert.equal(trapCalls, 0);
	});

	it("validates hostile limits without invoking traps or getters", () => {
		let trapCalls = 0;
		const trappedLimits = new Proxy({ maxNodes: 1 }, {
			get: () => { trapCalls++; throw new Error("get trap invoked"); },
			getOwnPropertyDescriptor: () => { trapCalls++; throw new Error("descriptor trap invoked"); },
			getPrototypeOf: () => { trapCalls++; throw new Error("prototype trap invoked"); },
			ownKeys: () => { trapCalls++; throw new Error("ownKeys trap invoked"); },
		});
		assert.throws(() => canonicalizeManagedJson(null, trappedLimits), /proxies/);
		assert.equal(trapCalls, 0);
		const revoked = Proxy.revocable({ maxNodes: 1 }, {});
		revoked.revoke();
		assert.throws(() => canonicalizeManagedJson(null, revoked.proxy), /proxies/);

		let getterCalls = 0;
		const accessorLimits = {};
		Object.defineProperty(accessorLimits, "maxNodes", { enumerable: true, get: () => { getterCalls++; return 1; } });
		assert.throws(() => canonicalizeManagedJson(null, accessorLimits), /enumerable data properties/);
		assert.equal(getterCalls, 0);

		const hidden = {};
		Object.defineProperty(hidden, "maxNodes", { value: 1 });
		const symbol = { [Symbol("maxNodes")]: 1 };
		for (const limits of [
			Object.assign(Object.create({ maxNodes: 1 }), {}),
			hidden,
			symbol,
			{ unknown: 1 },
			{ maxNodes: undefined },
		]) {
			assert.throws(() => canonicalizeManagedJson(null, limits as never));
		}
	});

	it("enforces independent limits and prechecks container cardinality", () => {
		assert.throws(() => canonicalizeManagedJson({ a: { b: 1 } }, { maxDepth: 1 }), /maxDepth/);
		assert.throws(() => canonicalizeManagedJson([1, 2], { maxNodes: 2 }), /maxNodes/);
		assert.throws(() => canonicalizeManagedJson(Array.from({ length: 100_000 }, () => null), { maxNodes: 2 }), /maxNodes/);
		assert.throws(() => canonicalizeManagedJson(new Array(1_000_000), { maxNodes: 2 }), /maxNodes/);
		const tooManyKeys: Record<string, unknown> = {};
		for (let index = 0; index < 100; index++) Object.defineProperty(tooManyKeys, `key${index}`, { enumerable: true, get: () => assert.fail("getter must not run") });
		assert.throws(() => canonicalizeManagedJson(tooManyKeys, { maxNodes: 2 }), /maxNodes/);
		assert.throws(() => canonicalizeManagedJson("😀", { maxUtf8Bytes: 3 }), /maxUtf8Bytes/);
		assert.throws(() => canonicalizeManagedJson("\u0000", { maxUtf8Bytes: 1, maxSerializedBytes: 3 }), /maxSerializedBytes/);
		assert.throws(() => canonicalizeManagedJson(null, { maxNodes: -1 }), /non-negative safe integer/);
	});

	it("domain-separates deterministic profile content and root-bound identity hashes", () => {
		const contentA = computeManagedProfileContentDigest({ mode: "strict", nested: { b: 2, a: 1 } });
		const reordered = computeManagedProfileContentDigest({ nested: { a: 1, b: 2 }, mode: "strict" });
		const changed = computeManagedProfileContentDigest({ mode: "strict", nested: { a: 1, b: 3 } });
		assert.equal(contentA, reordered);
		assert.equal(contentA, "e42d09f28a9f73b65427cf8ef30147e0278d7604c4ca1818b7c14d8d7ab8ca97");
		assert.notEqual(contentA, changed);
		assert.match(contentA, /^[a-f0-9]{64}$/);

		const identityA = { version: 1, contentDigest: contentA, root: { version: 1, realPath: "/repo", device: "1", inode: "2" } };
		const identityB = { ...identityA, root: { ...identityA.root, realPath: "/other" } };
		const identityWithChangedContent = { ...identityA, contentDigest: changed };
		const rootA = computeManagedProfileIdentityDigest(identityA);
		assert.equal(rootA, "ab41f14e15bff4e9a1a3b653cbca7dff5b7a9bb50d3866d207515a019d52458d");
		assert.notEqual(rootA, computeManagedProfileIdentityDigest(identityB));
		assert.notEqual(rootA, computeManagedProfileIdentityDigest(identityWithChangedContent));
		assert.notEqual(rootA, contentA, "profile-content and profile-identity domains must differ");
		assert.throws(() => computeManagedProfileIdentityDigest({ ...identityA, root: { ...identityA.root, device: undefined } }));
		assert.throws(() => computeManagedProfileIdentityDigest({ ...identityA, root: { ...identityA.root, realPath: "/repo\nother" } }));
	});

	it("exports preflight-to-launch binding vocabulary", () => {
		const preflight: ManagedPreflightRequestV1 = {
			version: 1,
			requestId: "preflight-1",
			method: "preflight",
			consumerId: assertManagedConsumerId("pi-signal"),
			input: { kind: "spawn", request: initialExecutorRequest() },
		};
		const result: ManagedPreflightResultV1 = {
			version: 1,
			ok: true,
			host: { version: 1, hostId: "host-1" },
			profile: { version: 1, contentDigest: "c".repeat(64), root: { version: 1, realPath: "/repo" } },
			profileIdentityDigest: "a".repeat(64),
			parentSessionIdentityDigest: "e".repeat(64),
			candidateRunId: "candidate-1",
			contractDigest: "b".repeat(64),
		};
		const parsed = parseManagedPreflightRequestV1(preflight);
		assert.equal(parsed.consumerId, "pi-signal");
		assert.equal(Object.isFrozen(parsed), true);
		assert.equal(Object.isFrozen(parsed.input.request), true);
		assert.equal(result.ok && result.profileIdentityDigest, "a".repeat(64));
		assert.throws(() => parseManagedPreflightRequestV1({ ...preflight, method: "spawn" }));
		assert.throws(() => parseManagedPreflightRequestV1({ ...preflight, extra: true }));
		assert.throws(() => parseManagedPreflightRequestV1({ ...preflight, input: { kind: "spawn", request: {}, extra: true } }));
		assert.throws(() => parseManagedPreflightRequestV1({ ...preflight, input: { kind: "resume", sourceRunId: "run", index: 0, request: [] } }));
	});

	it("strictly parses capability and exact status/details targets", () => {
		const capabilities = parseManagedReadRequestV1({ version: 1, requestId: "cap-1", method: "capabilities" });
		assert.equal(capabilities.method, "capabilities");
		assert.equal(Object.isFrozen(capabilities), true);
		const status = parseManagedReadRequestV1({
			version: 1,
			requestId: "status-1",
			method: "status",
			target: { consumerId: "pi-signal", operationId: operationId() },
		});
		assert.equal(status.method, "status");
		const details = parseManagedReadRequestV1({
			version: 1,
			requestId: "details-1",
			method: "details",
			target: { consumerId: "pi-signal", runId: "exact-run" },
		});
		assert.equal(details.method, "details");
		for (const invalid of [
			{ version: 1, requestId: "cap-1", method: "capabilities", extra: true },
			{ version: 1, requestId: "status-1", method: "status", target: { consumerId: "pi-signal", operationId: operationId(), runId: "run" } },
			{ version: 1, requestId: "status-1", method: "status", target: { consumerId: "pi-signal", operationId: "prefix" } },
			{ version: 1, requestId: "details-1", method: "details", target: { consumerId: "other", runId: "bad/run" } },
		]) assert.throws(() => parseManagedReadRequestV1(invalid));
		let getterCalls = 0;
		const hostile = { version: 1, requestId: "status-1", method: "status" } as Record<string, unknown>;
		Object.defineProperty(hostile, "target", { enumerable: true, get: () => { getterCalls++; return {}; } });
		assert.throws(() => parseManagedReadRequestV1(hostile));
		assert.equal(getterCalls, 0);
	});

	it("hashes exact spawn identity while excluding transport requestId", () => {
		const first = spawnRequest();
		const parsed = parseManagedMutationRequestV1(first);
		assert.equal(Object.isFrozen(parsed), true);
		assert.equal(parsed.method, "spawn");
		if (parsed.method !== "spawn") assert.fail("expected parsed spawn request");
		assert.equal(Object.isFrozen(parsed.input.request), true);
		const requestIdChanged = spawnRequest({ requestId: "transport-retry" });
		const inputReordered = spawnRequest({
			input: {
				request: {
					sessionDir: "/private/sessions/operation-1",
					output: false,
					artifacts: false,
					model: "provider/model",
					cwd: "/repo",
					clarify: false,
					async: true,
					context: "fresh",
					task: "inspect",
					agent: "worker",
				},
			},
		});
		const operationChanged = spawnRequest({
			managed: { version: 1, consumerId: "pi-signal", operationId: operationId(8) },
		});
		assert.equal(computeManagedRequestDigest(first), "5ad64622c38b858c1bae976d120fb486154b773f7cc7573008867012b07040d1");
		const earlyV1 = spawnRequest();
		delete (earlyV1.expectedLaunch as Record<string, unknown>).parentSessionIdentityDigest;
		assert.equal(
			computeManagedRequestDigest(earlyV1),
			"d56ca3a6832fe9de4c1a932ae397d67e45e0c923367bc5e64ffa3a7b0bac9b0b",
			"inert early-v1 envelopes remain parseable with stable semantic identity",
		);
		assert.equal(computeManagedRequestDigest(first), computeManagedRequestDigest(requestIdChanged));
		assert.equal(computeManagedRequestDigest(first), computeManagedRequestDigest(inputReordered));
		assert.notEqual(computeManagedRequestDigest(first), computeManagedRequestDigest(operationChanged));
		assert.notEqual(
			computeManagedRequestDigest(first),
			computeManagedRequestDigest(
				spawnRequest({ input: { request: { ...initialExecutorRequest(), model: "provider/other" } } }),
			),
		);
		assert.notEqual(
			computeManagedRequestDigest(first),
			computeManagedRequestDigest(
				spawnRequest({ input: { request: { ...initialExecutorRequest(), sessionDir: "/private/sessions/other" } } }),
			),
		);
		assert.match(computeManagedRequestDigest(first), /^[a-f0-9]{64}$/);
	});

	it("uses a contract-valid resume input and exact contracts for every mutation method", () => {
		assert.equal(computeManagedRequestDigest(resumeRequest()), "6f0ee02699fc3b136d065eb4060d0f518abc0fec9f2903a59022b24370f14cc7");
		for (const method of ["steer", "interrupt", "stop", "retire"] as const) {
			assert.match(computeManagedRequestDigest(controlRequest(method)), /^[a-f0-9]{64}$/);
		}
		assert.notEqual(computeManagedRequestDigest(spawnRequest()), computeManagedRequestDigest(resumeRequest()));
	});

	it("binds spawn and resume digests to every expected-launch field", () => {
		for (const makeRequest of [spawnRequest, resumeRequest]) {
			const baseline = computeManagedRequestDigest(makeRequest());
			for (const [field, value] of [
				["hostId", "host-2"],
				["candidateRunId", "candidate-2"],
				["profileIdentityDigest", "c".repeat(64)],
				["parentSessionIdentityDigest", "f".repeat(64)],
				["contractDigest", "d".repeat(64)],
			] as const) {
				assert.notEqual(computeManagedRequestDigest(makeRequest({ expectedLaunch: expectedLaunch({ [field]: value }) })), baseline, field);
			}
			const missingExpectedLaunch = makeRequest();
			delete missingExpectedLaunch.expectedLaunch;
			assert.throws(() => computeManagedRequestDigest(missingExpectedLaunch), /missing or unknown/i);
			assert.throws(() => computeManagedRequestDigest(makeRequest({ expectedLaunch: undefined })), /expected launch/i);
			assert.throws(() => computeManagedRequestDigest(makeRequest({ expectedLaunch: { ...expectedLaunch(), extra: true } })), /missing or unknown/);
			assert.throws(() => computeManagedRequestDigest(makeRequest({ expectedLaunch: expectedLaunch({ version: 2 }) })), /unsupported version/);
			assert.throws(() => computeManagedRequestDigest(makeRequest({ expectedLaunch: expectedLaunch({ contractDigest: "wrong" }) })), /SHA-256/);
			assert.throws(() => computeManagedRequestDigest(makeRequest({ expectedLaunch: expectedLaunch({ hostId: "host/unsafe" }) })), /safe identifier/);
			assert.throws(() => computeManagedRequestDigest(makeRequest({ expectedLaunch: expectedLaunch({ candidateRunId: "run/unsafe" }) })), /safe identifier/);
		}
	});

	it("strictly rejects malformed method-specific mutation contracts", () => {
		const malformed: unknown[] = [
			{ ...spawnRequest(), unknown: true },
			spawnRequest({ version: 2 }),
			spawnRequest({ method: "status" }),
			spawnRequest({ managed: { version: 2, consumerId: "pi-signal", operationId: operationId() } }),
			spawnRequest({ managed: { version: 1, consumerId: "pi-signal", operationId: operationId(), extra: true } }),
			spawnRequest({ input: {} }),
			spawnRequest({ input: { request: {}, profile: profile() } }),
			spawnRequest({ input: { request: {}, contractDigest: "b".repeat(64) } }),
			spawnRequest({ input: { request: null } }),
			spawnRequest({ input: { request: [] } }),
			spawnRequest({ input: { request: undefined } }),
			resumeRequest({ input: { sourceRunId: "source-1", index: -1, request: {} } }),
			resumeRequest({ input: { sourceRunId: "source-1", index: 1_000_001, request: {} } }),
			resumeRequest({ input: { sourceRunId: "source/unsafe", index: 0, request: {} } }),
			resumeRequest({ input: { sourceRunId: "source-1", index: 0 } }),
			controlRequest("steer", { input: { target: { consumerId: "pi-signal", operationId: operationId() }, message: "" } }),
			controlRequest("interrupt", { input: { target: { consumerId: "other-consumer", operationId: operationId() } } }),
			controlRequest("interrupt", { input: { target: { consumerId: "pi-signal", operationId: operationId(), runId: "run-1" } } }),
			controlRequest("stop", { input: { target: { consumerId: "pi-signal" } } }),
			controlRequest("retire", { input: { target: { consumerId: "pi-signal", runId: "run-1" }, acknowledgeUncertain: undefined } }),
			new Proxy(spawnRequest(), {}),
		];
		for (const value of malformed) assert.throws(() => computeManagedRequestDigest(value));
	});

	it("is importable through the public package subpath", async () => {
		const publicApi = await import("pi-subagents/managed-dispatch");
		assert.equal(publicApi.SUBAGENT_MANAGED_DISPATCH_VERSION, 1);
		assert.equal(
			(publicApi as unknown as Record<string, unknown>).SUBAGENT_MANAGED_DISPATCH_CAPABILITIES_V1,
			undefined,
		);
		assert.deepEqual(publicApi.SUBAGENT_MANAGED_DISPATCH_REQUIREMENTS_V1, SUBAGENT_MANAGED_DISPATCH_REQUIREMENTS_V1);
		assert.equal(publicApi.computeManagedRequestDigest(spawnRequest()), computeManagedRequestDigest(spawnRequest()));
	});
});

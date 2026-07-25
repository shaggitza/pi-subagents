import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { types as utilTypes } from "node:util";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Compile } from "typebox/compile";
import {
	SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT,
	SUBAGENT_MANAGED_DISPATCH_VERSION,
	computeManagedProfileContentDigest,
	computeManagedProfileIdentityDigest,
	managedDispatchReplyEvent,
	projectManagedChildCapabilityV1,
	parseManagedPreflightRequestV1,
	type JsonObject,
	type ManagedDispatchErrorCodeV1,
	type ManagedPreflightReplyV1,
	type ManagedPreflightResultV1,
	type ManagedProfileIdentityV1,
} from "../api/managed-dispatch.ts";
import {
	resolveSubagentLaunchContract,
	type SubagentLaunchContract,
	type SubagentLaunchContractInput,
	type SubagentLaunchContractResult,
} from "../api/preflight.ts";
import { resolveCurrentSubagentCapabilityCeiling } from "../runs/shared/capability-ceiling.ts";
import type { SubagentParamsLike } from "../runs/foreground/subagent-executor.ts";
import { SubagentParams } from "./schemas.ts";

interface EventBus {
	on(event: string, handler: (data: unknown) => void): (() => void) | void;
	emit(event: string, data: unknown): void;
}

export interface ManagedDispatchPreflightBridgeOptions {
	events: EventBus;
	getContext: () => ExtensionContext | null;
	/** Monotonic active-session epoch; increment on every start/reset/shutdown. */
	getSessionGeneration?: () => number;
	artifactDir?: "project" | "session" | "temp";
	hostIdPath?: string;
	createRunId?: () => string;
	resolveContract?: (input: SubagentLaunchContractInput) => Promise<SubagentLaunchContractResult>;
	resolveCapabilityCeiling?: typeof resolveCurrentSubagentCapabilityCeiling;
}

export type ManagedSpawnLaunchResolverOptions = Pick<
	ManagedDispatchPreflightBridgeOptions,
	"artifactDir" | "resolveContract" | "resolveCapabilityCeiling"
>;

const paramsValidator = Compile(SubagentParams);
const SAFE_HOST_ID = /^[A-Za-z0-9][A-Za-z0-9._~:-]{0,255}$/;

function defaultHostIdPath(): string {
	const agentDir = process.env.PI_CODING_AGENT_DIR || path.join(os.homedir(), ".pi", "agent");
	return path.join(agentDir, "subagents", "managed-dispatch", "host-id");
}

function readHostId(filePath: string): string | undefined {
	try {
		const value = fs.readFileSync(filePath, "utf8").trim();
		return SAFE_HOST_ID.test(value) ? value : undefined;
	} catch {
		return undefined;
	}
}

/** Lazily creates a stable, non-secret host identity without replacing a concurrent winner. */
export function loadOrCreateManagedDispatchHostId(filePath = defaultHostIdPath()): string {
	const existing = readHostId(filePath);
	if (existing) return existing;
	fs.mkdirSync(path.dirname(filePath), { recursive: true, mode: 0o700 });
	const candidate = randomUUID();
	const temporary = `${filePath}.${process.pid}.${randomUUID()}.tmp`;
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(temporary, "wx", 0o600);
		fs.writeFileSync(descriptor, `${candidate}\n`, "utf8");
		fs.fsyncSync(descriptor);
		fs.closeSync(descriptor);
		descriptor = undefined;
		try {
			fs.linkSync(temporary, filePath);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
		}
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
		fs.rmSync(temporary, { force: true });
	}
	const resolved = readHostId(filePath);
	if (!resolved) throw new Error("Managed dispatch host identity is unavailable or corrupt.");
	return resolved;
}

function ownDataValue(value: unknown, key: string): unknown {
	if (!value || typeof value !== "object" || utilTypes.isProxy(value)) return undefined;
	const prototype = Object.getPrototypeOf(value);
	if (prototype !== Object.prototype && prototype !== null) return undefined;
	const descriptor = Object.getOwnPropertyDescriptor(value, key);
	return descriptor && "value" in descriptor ? descriptor.value : undefined;
}

function safeReplyEvent(payload: unknown): string | undefined {
	const requestId = ownDataValue(payload, "requestId");
	if (typeof requestId !== "string") return undefined;
	try {
		return managedDispatchReplyEvent(requestId);
	} catch {
		return undefined;
	}
}

function failure(code: ManagedDispatchErrorCodeV1, message: string): ManagedPreflightResultV1 {
	return { version: SUBAGENT_MANAGED_DISPATCH_VERSION, ok: false, code, message };
}

export function assertManagedSpawnParams(request: JsonObject): SubagentParamsLike {
	if (!paramsValidator.Check(request)) throw new TypeError("Executor request does not match the current subagent schema.");
	const params = request as unknown as SubagentParamsLike;
	if (params.action !== undefined || params.tasks !== undefined || params.chain !== undefined) {
		throw new TypeError("Managed spawn requires ordinary single-agent execution mode.");
	}
	if (typeof params.agent !== "string" || !params.agent || typeof params.task !== "string" || !params.task) {
		throw new TypeError("Managed spawn requires a non-empty agent and task.");
	}
	if (params.async !== true || params.clarify !== false || params.context !== "fresh") {
		throw new TypeError("Managed spawn requires async=true, clarify=false, and context='fresh'.");
	}
	if (typeof params.cwd !== "string" || !path.isAbsolute(params.cwd)) {
		throw new TypeError("Managed spawn requires an absolute cwd.");
	}
	if (typeof params.sessionDir !== "string" || !path.isAbsolute(params.sessionDir)) {
		throw new TypeError("Managed spawn requires an explicit absolute sessionDir.");
	}
	if (params.worktree === true) throw new TypeError("Managed spawn v1 does not yet support worktrees.");
	return params;
}

function rootIdentity(root: string): ManagedProfileIdentityV1["root"] {
	const realPath = fs.realpathSync(root);
	const stats = fs.statSync(realPath, { bigint: true });
	if (!stats.isDirectory()) throw new TypeError("Managed profile root is not a directory.");
	return {
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		realPath,
		device: String(stats.dev),
		inode: String(stats.ino),
	};
}

function profileContent(contract: SubagentLaunchContract): JsonObject {
	return {
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		agent: contract.agent as unknown as JsonObject,
		context: contract.context,
		...(contract.model ? { model: contract.model } : {}),
		modelCandidates: contract.modelCandidates,
		...(contract.thinking ? { thinking: contract.thinking } : {}),
		systemPromptMode: contract.systemPromptMode,
		inheritProjectContext: contract.inheritProjectContext,
		inheritSkills: contract.inheritSkills,
		skills: contract.skills as unknown as JsonObject,
		tools: contract.tools as unknown as JsonObject,
		protocol: contract.protocol as unknown as JsonObject,
	};
}

export function hasCompleteManagedContractIdentity(contract: SubagentLaunchContract): boolean {
	const attestations = contract.roots.attestations;
	if (!contract.parentSessionIdentityDigest || !contract.agent.definitionDigest || !attestations) return false;
	if (contract.skills.resolved.some((skill) => !skill.contentDigest || !/^[a-f0-9]{64}$/.test(skill.contentDigest))) return false;
	for (const [name, root] of Object.entries(contract.roots)) {
		if (name === "attestations" || name === "artifactPaths") continue;
		if (typeof root === "string" && !attestations[name]) return false;
	}
	if (contract.roots.artifactPaths) {
		for (const [name, artifactPath] of Object.entries(contract.roots.artifactPaths)) {
			if (typeof artifactPath === "string" && !attestations[`artifactPaths.${name}`]) return false;
		}
	}
	return typeof contract.roots.sessionRoot === "string"
		&& typeof contract.roots.sessionDir === "string"
		&& typeof contract.roots.sessionFile === "string"
		&& typeof contract.roots.asyncDir === "string"
		&& typeof contract.roots.resultPath === "string"
		&& typeof contract.roots.resultReservationPath === "string"
		&& typeof contract.roots.runnerConfigPath === "string"
		&& typeof contract.roots.runnerAdmissionPath === "string"
		&& typeof contract.roots.runnerAdmissionProceedPath === "string"
		&& typeof contract.roots.runnerAdmissionCommitPath === "string";
}

export function deriveManagedSpawnProfile(contract: SubagentLaunchContract): {
	profile: ManagedProfileIdentityV1;
	profileIdentityDigest: string;
} {
	const contentDigest = computeManagedProfileContentDigest(profileContent(contract));
	const profile: ManagedProfileIdentityV1 = {
		version: SUBAGENT_MANAGED_DISPATCH_VERSION,
		contentDigest,
		root: rootIdentity(contract.roots.cwd),
	};
	return { profile, profileIdentityDigest: computeManagedProfileIdentityDigest(profile) };
}

function launchContractInput(
	params: SubagentParamsLike,
	candidateRunId: string,
	ctx: ExtensionContext,
	parentSessionId: string,
	parentSessionFile: string,
	options: ManagedSpawnLaunchResolverOptions,
): SubagentLaunchContractInput {
	return {
		agent: params.agent!,
		cwd: params.cwd!,
		task: params.task,
		agentScope: params.agentScope as SubagentLaunchContractInput["agentScope"],
		context: params.context,
		model: params.model,
		thinking: params.thinking,
		parentModel: ctx.model ? { provider: ctx.model.provider, id: ctx.model.id } : undefined,
		availableModels: ctx.modelRegistry.getAvailable(),
		preferredProvider: ctx.model?.provider,
		skill: params.skill,
		output: params.output,
		outputMode: params.outputMode,
		outputSchema: params.outputSchema,
		artifacts: params.artifacts,
		artifactDir: options.artifactDir,
		parentSessionFile,
		identityMode: "managed-v1",
		parentSessionId,
		sessionDir: params.sessionDir,
		runId: candidateRunId,
		capabilityCeiling: (options.resolveCapabilityCeiling ?? resolveCurrentSubagentCapabilityCeiling)(
			ctx.sessionManager.getSessionFile() ?? ctx.sessionManager.getSessionId() ?? "",
		),
	};
}

export interface ResolvedManagedSpawnLaunchV1 {
	readonly params: SubagentParamsLike;
	readonly contract: SubagentLaunchContract;
	readonly profile: ManagedProfileIdentityV1;
	readonly profileIdentityDigest: string;
}

/** Recomputes host-owned managed spawn identity without claiming or launching an operation. */
export async function resolveManagedSpawnLaunchV1(
	request: JsonObject,
	candidateRunId: string,
	ctx: ExtensionContext,
	parentSessionId: string,
	parentSessionFile: string,
	options: ManagedSpawnLaunchResolverOptions = {},
): Promise<ResolvedManagedSpawnLaunchV1> {
	const params = assertManagedSpawnParams(request);
	if (!SAFE_HOST_ID.test(candidateRunId)) throw new TypeError("Managed candidate run identity is invalid.");
	const contractResult = await (options.resolveContract ?? resolveSubagentLaunchContract)(
		launchContractInput(params, candidateRunId, ctx, parentSessionId, parentSessionFile, options),
	);
	if (contractResult.ok === false) throw new TypeError("Managed launch contract preflight failed.");
	if (contractResult.contract.diagnostics.some((diagnostic) => diagnostic.severity !== "warning")) {
		throw new TypeError("Managed launch contract requires unavailable host state.");
	}
	if (!hasCompleteManagedContractIdentity(contractResult.contract)) {
		throw new TypeError("Managed launch contract lacks complete parent, agent, or root identity.");
	}
	const { profile, profileIdentityDigest } = deriveManagedSpawnProfile(contractResult.contract);
	return Object.freeze({ params, contract: contractResult.contract, profile, profileIdentityDigest });
}

export async function performManagedSpawnPreflightV1(
	payload: unknown,
	options: ManagedDispatchPreflightBridgeOptions,
): Promise<ManagedPreflightResultV1> {
	let request;
	try {
		request = parseManagedPreflightRequestV1(payload);
	} catch {
		return failure("invalid_request", "Managed preflight request is invalid.");
	}
	const ctx = options.getContext();
	let parentSessionId: string | null | undefined;
	let parentSessionFile: string | null | undefined;
	let sessionGeneration: number;
	try {
		parentSessionId = ctx?.sessionManager.getSessionId();
		parentSessionFile = ctx?.sessionManager.getSessionFile();
		sessionGeneration = options.getSessionGeneration?.() ?? 0;
	} catch {
		return failure("no_active_session", "Managed preflight requires an active persisted parent session.");
	}
	if (!ctx || !parentSessionId || !parentSessionFile) {
		return failure("no_active_session", "Managed preflight requires an active persisted parent session.");
	}
	if (request.input.kind !== "spawn") {
		return failure("unsupported_method", "Managed resume preflight is not available in this protocol increment.");
	}
	try {
		const candidateRunId = (options.createRunId ?? randomUUID)();
		const resolved = await resolveManagedSpawnLaunchV1(
			request.input.request,
			candidateRunId,
			ctx,
			parentSessionId,
			parentSessionFile,
			options,
		);
		let currentSessionMatches = false;
		try {
			const currentContext = options.getContext();
			currentSessionMatches = currentContext?.sessionManager.getSessionId() === parentSessionId
				&& currentContext?.sessionManager.getSessionFile() === parentSessionFile
				&& (options.getSessionGeneration?.() ?? 0) === sessionGeneration;
		} catch {
			currentSessionMatches = false;
		}
		if (!currentSessionMatches) {
			return failure("no_active_session", "Managed preflight parent session changed before completion.");
		}
		return {
			version: SUBAGENT_MANAGED_DISPATCH_VERSION,
			ok: true,
			host: {
				version: SUBAGENT_MANAGED_DISPATCH_VERSION,
				hostId: loadOrCreateManagedDispatchHostId(options.hostIdPath),
			},
			profile: resolved.profile,
			profileIdentityDigest: resolved.profileIdentityDigest,
			childCapability: projectManagedChildCapabilityV1(resolved.contract.tools),
			parentSessionIdentityDigest: resolved.contract.parentSessionIdentityDigest,
			candidateRunId,
			contractDigest: resolved.contract.digest,
		};
	} catch {
		return failure("invalid_request", "Managed executor request or resolved launch profile is invalid.");
	}
}

/** Registers non-launching managed preflight. Mutation methods remain unhandled. */
export function registerManagedDispatchPreflightBridge(options: ManagedDispatchPreflightBridgeOptions): () => void {
	let disposed = false;
	const emitIfCurrent = (replyEvent: string, reply: ManagedPreflightReplyV1): void => {
		if (!disposed) options.events.emit(replyEvent, reply);
	};
	const handler = (payload: unknown): void => {
		if (disposed || ownDataValue(payload, "method") !== "preflight") return;
		const replyEvent = safeReplyEvent(payload);
		const requestId = ownDataValue(payload, "requestId");
		if (!replyEvent || typeof requestId !== "string") return;
		void performManagedSpawnPreflightV1(payload, options)
			.then((data) => emitIfCurrent(replyEvent, {
				version: SUBAGENT_MANAGED_DISPATCH_VERSION,
				requestId,
				method: "preflight",
				success: true,
				data,
			}))
			.catch(() => emitIfCurrent(replyEvent, {
				version: SUBAGENT_MANAGED_DISPATCH_VERSION,
				requestId,
				method: "preflight",
				success: false,
				error: { code: "execution_failed", message: "Managed preflight failed closed." },
			}));
	};
	const unsubscribe = options.events.on(SUBAGENT_MANAGED_DISPATCH_REQUEST_EVENT, handler);
	return () => {
		disposed = true;
		if (typeof unsubscribe === "function") unsubscribe();
	};
}

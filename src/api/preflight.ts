import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { discoverAgents, discoverAgentsAll, type AgentConfig, type AgentScope, type AgentSource } from "../agents/agents.ts";
import { resolveExecutionAgentScope } from "../agents/agent-scope.ts";
import { normalizeSkillInput, resolveSkillsWithFallback } from "../agents/skills.ts";
import { buildModelCandidates, resolveEffectiveSubagentModel, type AvailableModelInfo, type ParentModel } from "../runs/shared/model-fallback.ts";
import { applyThinkingSuffix, resolvePiLaunchToolPlan, type PiLaunchToolPlan } from "../runs/shared/pi-args.ts";
import { normalizeSingleOutputOverride, resolveSingleOutputPath } from "../runs/shared/single-output.ts";
import { getArtifactPaths, getArtifactsDir } from "../shared/artifacts.ts";
import { resolveEffectiveThinking } from "../shared/model-info.ts";
import {
	ASYNC_DIR,
	RESULTS_DIR,
	SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
	getAsyncConfigPath,
	type ArtifactDirPreference,
	type ArtifactPaths,
	type JsonSchemaObject,
	type OutputMode,
} from "../shared/types.ts";
import type { ResolvedSubagentCapabilityCeiling, SubagentCapabilityAudit } from "../runs/shared/capability-ceiling.ts";
import type { ResolvedMcpDirectToolSelection } from "../runs/shared/mcp-direct-tool-allowlist.ts";
import { preparedResultReservationPath } from "../runs/background/prepared-result-reservation.ts";
import { preparedRunnerAdmissionPaths } from "../runs/background/prepared-runner-admission.ts";
import { resolveStepBehavior } from "../shared/settings.ts";

export const SUBAGENT_LAUNCH_CONTRACT_VERSION = 1 as const;

export type SubagentLaunchContractReasonCode =
	| "missing_agent"
	| "ambiguous_agent"
	| "missing_skill"
	| "denied_required_tool"
	| "invalid_artifact_dir"
	| "invalid_cwd"
	| "invalid_root"
	| "unsupported_mode";

export interface SubagentLaunchContractDiagnostic {
	code: SubagentLaunchContractReasonCode | "host_required" | "snapshot_warning";
	severity: "error" | "warning" | "host-required";
	message: string;
}

export interface SubagentLaunchContractInput {
	agent: string;
	cwd: string;
	task?: string;
	agentScope?: AgentScope;
	context?: "fresh" | "fork";
	model?: string;
	thinking?: string | false;
	parentModel?: ParentModel;
	availableModels?: ReadonlyArray<AvailableModelInfo | { provider: string; id: string; fullId?: string; reasoning?: boolean }>;
	preferredProvider?: string;
	skill?: string | string[] | boolean;
	output?: string | boolean;
	outputMode?: OutputMode;
	outputSchema?: JsonSchemaObject;
	artifacts?: boolean;
	artifactDir?: ArtifactDirPreference;
	parentSessionFile?: string | null;
	/** Opt in to additive host-bound identity fields used by managed dispatch. */
	identityMode?: "managed-v1";
	/** Host-supplied active parent session ID; never accepted from a managed request body. */
	parentSessionId?: string;
	sessionRoot?: string;
	sessionDir?: string;
	runId?: string;
	capabilityCeiling?: ResolvedSubagentCapabilityCeiling;
	inheritedCapabilityCeiling?: ResolvedSubagentCapabilityCeiling;
	/** Host-only exact recovered agent definition for managed resume. */
	managedAgentConfig?: AgentConfig;
	/** Host-only exact recovered artifact root for managed resume. */
	managedArtifactsDir?: string;
}

export interface SubagentLaunchContractAgentCandidate {
	name: string;
	localName?: string;
	packageName?: string;
	source: AgentSource;
	filePath: string;
	disabled?: boolean;
	selected: boolean;
}

export interface SubagentLaunchContractAgent {
	name: string;
	localName?: string;
	packageName?: string;
	source: AgentSource;
	filePath: string;
	/** Digest of every resolved agent-definition field, including the complete system prompt. */
	definitionDigest?: string;
	shadowedCandidates: SubagentLaunchContractAgentCandidate[];
}

export interface SubagentLaunchContractSkills {
	requested: string[];
	resolved: Array<{ name: string; path: string; source: string; contentDigest?: string }>;
	missing: string[];
}

export interface SubagentLaunchContractTools {
	requestedBuiltin: string[];
	declaredBuiltin: string[];
	effectiveAllowlist: string[];
	explicitAllowlist: boolean;
	requiredChildTools: string[];
	internalTools: string[];
	mcp: ResolvedMcpDirectToolSelection[];
	effectiveMcpTools: string[];
	toolExtensionPaths: string[];
	runtimeExtensions: string[];
	configuredExtensions: string[];
	extensionArgs: string[];
	disableAmbientExtensions: boolean;
	fanoutAuthorized: boolean;
	capabilityCeiling?: ResolvedSubagentCapabilityCeiling;
	capabilityAudit?: SubagentCapabilityAudit;
}

export interface SubagentLaunchRootAttestation {
	path: string;
	existingAncestor: string;
	existingAncestorRealPath: string;
	/** Stable projected real path: existing real ancestor plus the unresolved suffix. */
	projectedRealPath: string;
	existingAncestorDevice: string;
	existingAncestorInode: string;
	relativeSuffix: string;
}

export interface SubagentLaunchContractRoots {
	cwd: string;
	sessionRoot?: string;
	sessionDir?: string;
	sessionFile?: string;
	artifactsDir?: string;
	artifactPaths?: ArtifactPaths;
	outputPath?: string;
	/** Managed async-run root containing status, recovery, logs, and process proof. */
	asyncDir?: string;
	/** Managed terminal result sidecar outside asyncDir. */
	resultPath?: string;
	/** Exclusive ownership record retained until the managed result is published. */
	resultReservationPath?: string;
	/** Managed transient runner config path outside asyncDir. */
	runnerConfigPath?: string;
	/** Durable runner-ready/accepted evidence inside asyncDir. */
	runnerAdmissionPath?: string;
	/** Token-bound parent proceed control inside asyncDir. */
	runnerAdmissionProceedPath?: string;
	/** Token-bound parent commit control inside asyncDir. */
	runnerAdmissionCommitPath?: string;
	/** Identity evidence for every declared write/evidence path before creation. */
	attestations?: Record<string, SubagentLaunchRootAttestation>;
}

export interface SubagentLaunchContract {
	version: typeof SUBAGENT_LAUNCH_CONTRACT_VERSION;
	runId: string;
	parentSessionIdentityDigest?: string;
	agent: SubagentLaunchContractAgent;
	context: "fresh" | "fork";
	model?: string;
	modelCandidates: string[];
	thinking?: string;
	systemPromptMode: AgentConfig["systemPromptMode"];
	inheritProjectContext: boolean;
	inheritSkills: boolean;
	skills: SubagentLaunchContractSkills;
	tools: SubagentLaunchContractTools;
	roots: SubagentLaunchContractRoots;
	protocol: {
		lifecycleArtifactVersion: number;
		packageVersion: string;
	};
	diagnostics: SubagentLaunchContractDiagnostic[];
	digest: string;
}

export type SubagentLaunchContractResult =
	| { ok: true; contract: SubagentLaunchContract }
	| { ok: false; code: SubagentLaunchContractReasonCode; message: string; diagnostics: SubagentLaunchContractDiagnostic[] };

function packageVersion(): string {
	const packagePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "package.json");
	const parsed = JSON.parse(fs.readFileSync(packagePath, "utf-8")) as { version?: unknown };
	return typeof parsed.version === "string" ? parsed.version : "0.0.0";
}

function stableJson(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
	if (value && typeof value === "object") {
		return `{${Object.entries(value as Record<string, unknown>)
			.filter(([, entry]) => entry !== undefined)
			.sort(([a], [b]) => a.localeCompare(b))
			.map(([key, entry]) => `${JSON.stringify(key)}:${stableJson(entry)}`)
			.join(",")}}`;
	}
	return JSON.stringify(value);
}

function sha256StableJson(value: unknown): string {
	return createHash("sha256").update(stableJson(value)).digest("hex");
}

function digestContract(contract: Omit<SubagentLaunchContract, "digest">): string {
	if (!contract.parentSessionIdentityDigest || !contract.roots.attestations) return sha256StableJson(contract);
	const stableAttestations = Object.fromEntries(
		Object.entries(contract.roots.attestations).map(([name, attestation]) => [name, {
			path: attestation.path,
			projectedRealPath: attestation.projectedRealPath,
		}]),
	);
	return sha256StableJson({
		...contract,
		roots: { ...contract.roots, attestations: stableAttestations },
	});
}

function digestAgentDefinition(agent: AgentConfig): string {
	return sha256StableJson(agent);
}

function digestManagedSkillContent(content: string): string {
	return sha256StableJson({ domain: "pi-subagents/managed-dispatch/v1/skill-content", content });
}

export function computeParentSessionIdentityDigest(sessionId: string, sessionFile: string | null | undefined): string {
	return sha256StableJson({ sessionId, sessionFile: sessionFile ? path.resolve(sessionFile) : null });
}

function normalizeAvailableModels(models: SubagentLaunchContractInput["availableModels"]): AvailableModelInfo[] {
	return (models ?? []).map((model) => ({ ...model, fullId: model.fullId ?? `${model.provider}/${model.id}` }));
}

function attestLaunchPath(inputPath: string, expectedKind: "directory" | "file"): SubagentLaunchRootAttestation {
	const absolutePath = path.resolve(inputPath);
	let existingAncestor = absolutePath;
	for (;;) {
		try {
			fs.lstatSync(existingAncestor);
			break;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			const parent = path.dirname(existingAncestor);
			if (parent === existingAncestor) throw error;
			existingAncestor = parent;
		}
	}
	const existingAncestorRealPath = fs.realpathSync(existingAncestor);
	const stats = fs.statSync(existingAncestorRealPath, { bigint: true });
	const relativeSuffix = path.relative(existingAncestor, absolutePath);
	const projectedRealPath = path.resolve(existingAncestorRealPath, relativeSuffix);
	if (relativeSuffix && !stats.isDirectory()) throw new Error(`Existing launch-root ancestor is not a directory: ${existingAncestor}`);
	if (!relativeSuffix && expectedKind === "directory" && !stats.isDirectory()) {
		throw new Error(`Existing launch root is not a directory: ${absolutePath}`);
	}
	if (!relativeSuffix && expectedKind === "file" && !stats.isFile()) {
		throw new Error(`Existing launch file is not a regular file: ${absolutePath}`);
	}
	return {
		path: absolutePath,
		existingAncestor,
		existingAncestorRealPath,
		projectedRealPath,
		existingAncestorDevice: String(stats.dev),
		existingAncestorInode: String(stats.ino),
		relativeSuffix,
	};
}

function attestLaunchRoots(
	paths: Record<string, { path: string | undefined; kind: "directory" | "file" }>,
): Record<string, SubagentLaunchRootAttestation> {
	return Object.fromEntries(
		Object.entries(paths)
			.filter((entry): entry is [string, { path: string; kind: "directory" | "file" }] => typeof entry[1].path === "string")
			.map(([name, root]) => [name, attestLaunchPath(root.path, root.kind)]),
	);
}

const MANAGED_DIRECTORY_ATTESTATIONS = new Set(["cwd", "sessionRoot", "sessionDir", "artifactsDir", "asyncDir"]);

/** Synchronously re-attests stable real-path projections immediately before managed launch side effects. */
export function managedLaunchRootProjectionsAreCurrent(contract: SubagentLaunchContract): boolean {
	const attestations = contract.roots.attestations;
	if (!attestations) return false;
	try {
		return Object.entries(attestations).every(([name, expected]) => {
			let descriptor: number | undefined;
			try {
				const noFollow = "O_NOFOLLOW" in fs.constants
					? (fs.constants as typeof fs.constants & { O_NOFOLLOW: number }).O_NOFOLLOW
					: 0;
				const ancestorMustBeDirectory = expected.relativeSuffix.length > 0 || MANAGED_DIRECTORY_ATTESTATIONS.has(name);
				const directoryOnly = ancestorMustBeDirectory && "O_DIRECTORY" in fs.constants
					? (fs.constants as typeof fs.constants & { O_DIRECTORY: number }).O_DIRECTORY
					: 0;
				if (noFollow === 0 && fs.lstatSync(expected.existingAncestor).isSymbolicLink()) return false;
				descriptor = fs.openSync(expected.existingAncestor, fs.constants.O_RDONLY | noFollow | directoryOnly);
				const expectedAncestorStats = fs.fstatSync(descriptor, { bigint: true });
				if ((ancestorMustBeDirectory ? !expectedAncestorStats.isDirectory() : !expectedAncestorStats.isFile())
					|| fs.realpathSync(expected.existingAncestor) !== expected.existingAncestorRealPath
					|| String(expectedAncestorStats.dev) !== expected.existingAncestorDevice
					|| String(expectedAncestorStats.ino) !== expected.existingAncestorInode) return false;
			} finally {
				if (descriptor !== undefined) fs.closeSync(descriptor);
			}
			const current = attestLaunchPath(
				expected.path,
				MANAGED_DIRECTORY_ATTESTATIONS.has(name) ? "directory" : "file",
			);
			return current.path === expected.path && current.projectedRealPath === expected.projectedRealPath;
		});
	} catch {
		return false;
	}
}

function candidateList(inputAgent: string, selected: AgentConfig | undefined, cwd: string): SubagentLaunchContractAgentCandidate[] {
	const all = discoverAgentsAll(cwd);
	return [...all.builtin, ...all.package, ...all.user, ...all.project]
		.filter((agent) => agent.name === inputAgent || agent.localName === inputAgent)
		.map((agent) => ({
			name: agent.name,
			...(agent.localName ? { localName: agent.localName } : {}),
			...(agent.packageName ? { packageName: agent.packageName } : {}),
			source: agent.source,
			filePath: agent.filePath,
			...(agent.disabled === true ? { disabled: true } : {}),
			selected: Boolean(selected && agent.filePath === selected.filePath && agent.name === selected.name),
		}));
}

export async function resolveSubagentLaunchContract(input: SubagentLaunchContractInput): Promise<SubagentLaunchContractResult> {
	const diagnostics: SubagentLaunchContractDiagnostic[] = [];
	const effectiveCwd = path.resolve(input.cwd);
	try {
		if (!fs.statSync(effectiveCwd).isDirectory()) {
			return { ok: false, code: "invalid_cwd", message: `cwd '${effectiveCwd}' is not a directory.`, diagnostics };
		}
	} catch (error) {
		const detail = error instanceof Error ? ` ${error.message}` : "";
		return { ok: false, code: "invalid_cwd", message: `cwd '${effectiveCwd}' is not a directory.${detail}`, diagnostics };
	}
	if (input.context !== undefined && input.context !== "fresh" && input.context !== "fork") {
		return { ok: false, code: "unsupported_mode", message: `Unsupported context '${String(input.context)}'; expected 'fresh' or 'fork'.`, diagnostics };
	}
	if (input.artifactDir !== undefined && input.artifactDir !== "project" && input.artifactDir !== "session" && input.artifactDir !== "temp") {
		return { ok: false, code: "invalid_artifact_dir", message: `Unsupported artifactDir '${String(input.artifactDir)}'; expected 'project', 'session', or 'temp'.`, diagnostics };
	}
	if (input.context === "fork") {
		diagnostics.push({ code: "host_required", severity: "host-required", message: "Exact fork session branching and fork-thinking downgrade checks require Pi host session and model-registry snapshots." });
	}
	const scope = resolveExecutionAgentScope(input.agentScope);
	const discovered = discoverAgents(effectiveCwd, scope);
	const managedAgent = input.identityMode === "managed-v1" ? input.managedAgentConfig : undefined;
	const matches = discovered.agents.filter((agent) => agent.name === input.agent || agent.localName === input.agent);
	if (!managedAgent && matches.length === 0) {
		return { ok: false, code: "missing_agent", message: `Unknown agent: ${input.agent}`, diagnostics };
	}
	if (!managedAgent && matches.length > 1) {
		return { ok: false, code: "ambiguous_agent", message: `Ambiguous agent: ${input.agent}`, diagnostics };
	}
	if (managedAgent && managedAgent.name !== input.agent) {
		return { ok: false, code: "missing_agent", message: "Recovered managed agent identity differs from the requested agent.", diagnostics };
	}
	const agent = managedAgent ?? matches[0]!;
	const runId = input.runId ?? "preflight";
	const skillInput = normalizeSkillInput(input.skill);
	const outputOverride = normalizeSingleOutputOverride(input.output, agent.output);
	const behavior = resolveStepBehavior(agent, {
		...(outputOverride !== undefined ? { output: outputOverride } : {}),
		...(input.outputMode !== undefined ? { outputMode: input.outputMode } : {}),
		...(skillInput !== undefined ? { skills: skillInput } : {}),
		...(input.model !== undefined ? { model: input.model } : {}),
	});
	const requestedSkills = behavior.skills === false ? [] : behavior.skills;
	const resolvedSkills = resolveSkillsWithFallback(
		requestedSkills,
		effectiveCwd,
		effectiveCwd,
		agent.skillPath,
		agent.filePath ? path.dirname(agent.filePath) : effectiveCwd,
	);
	if (resolvedSkills.missing.includes("pi-subagents")) {
		return { ok: false, code: "missing_skill", message: "The pi-subagents orchestration skill is not child-injectable.", diagnostics };
	}
	if (resolvedSkills.missing.length > 0) diagnostics.push({ code: "missing_skill", severity: "error", message: `Missing skills: ${resolvedSkills.missing.join(", ")}` });

	const availableModels = normalizeAvailableModels(input.availableModels);
	const preferredProvider = input.preferredProvider ?? input.parentModel?.provider;
	const primaryModel = resolveEffectiveSubagentModel(input.model, agent.model, input.parentModel, availableModels, preferredProvider, { scope: discovered.modelScope });
	const effectiveThinkingConfig = input.thinking !== undefined ? input.thinking : agent.thinking;
	const model = applyThinkingSuffix(primaryModel, effectiveThinkingConfig, input.thinking !== undefined);
	const modelCandidates = buildModelCandidates(primaryModel, agent.fallbackModels, availableModels, preferredProvider, { scope: discovered.modelScope })
		.map((candidate) => applyThinkingSuffix(candidate, effectiveThinkingConfig, input.thinking !== undefined) ?? candidate);
	let toolPlan: PiLaunchToolPlan;
	try {
		toolPlan = resolvePiLaunchToolPlan({
			tools: agent.tools,
			extensions: agent.extensions,
			subagentOnlyExtensions: agent.subagentOnlyExtensions,
			mcpDirectTools: agent.mcpDirectTools,
			cwd: effectiveCwd,
			requireReadTool: resolvedSkills.resolved.length > 0,
			structuredOutput: Boolean(input.outputSchema),
			capabilityCeiling: input.capabilityCeiling,
			inheritedCapabilityCeiling: input.inheritedCapabilityCeiling,
		});
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		diagnostics.push({ code: "denied_required_tool", severity: "error", message });
		return { ok: false, code: "denied_required_tool", message, diagnostics };
	}
	const artifactsEnabled = input.artifacts !== false;
	const artifactsDir = artifactsEnabled
		? input.identityMode === "managed-v1" && input.managedArtifactsDir
			? path.resolve(input.managedArtifactsDir)
			: getArtifactsDir(input.parentSessionFile ?? null, effectiveCwd, input.artifactDir ?? "project")
		: undefined;
	const artifactPaths = artifactsDir ? getArtifactPaths(artifactsDir, runId, agent.name, 0) : undefined;
	const outputPath = resolveSingleOutputPath(behavior.output, effectiveCwd, effectiveCwd, artifactsDir ? path.join(artifactsDir, "outputs", runId) : undefined);
	const sessionRoot = input.sessionDir ? path.resolve(input.sessionDir) : input.sessionRoot ? path.join(path.resolve(input.sessionRoot), runId) : undefined;
	const sessionDir = sessionRoot ? path.join(sessionRoot, "run-0") : undefined;
	if (!sessionDir) diagnostics.push({ code: "host_required", severity: "host-required", message: "No sessionRoot/sessionDir was supplied; exact child session paths require the Pi host session-root policy." });
	if (input.identityMode === "managed-v1" && !input.parentSessionId) {
		diagnostics.push({ code: "host_required", severity: "host-required", message: "No active parent session identity was supplied; managed execution requires host-bound session identity." });
	}
	if (input.availableModels === undefined && (input.model || agent.model || input.parentModel)) {
		diagnostics.push({ code: "host_required", severity: "host-required", message: "No availableModels snapshot was supplied; model resolution may differ from the active Pi host registry." });
	}
	if (resolvedSkills.missing.length > 0) {
		return { ok: false, code: "missing_skill", message: `Missing skills: ${resolvedSkills.missing.join(", ")}`, diagnostics };
	}
	const sessionFile = sessionDir ? path.join(sessionDir, "session.jsonl") : undefined;
	const managedAsyncDir = path.join(ASYNC_DIR, runId);
	const managedResultPath = path.join(RESULTS_DIR, `${runId}.json`);
	const managedAdmissionPaths = preparedRunnerAdmissionPaths(managedAsyncDir);
	const managedRuntimePaths: Partial<Pick<
		SubagentLaunchContractRoots,
		| "asyncDir"
		| "resultPath"
		| "resultReservationPath"
		| "runnerConfigPath"
		| "runnerAdmissionPath"
		| "runnerAdmissionProceedPath"
		| "runnerAdmissionCommitPath"
	>> = input.identityMode === "managed-v1"
		? {
				asyncDir: managedAsyncDir,
				resultPath: managedResultPath,
				resultReservationPath: preparedResultReservationPath(managedResultPath),
				runnerConfigPath: getAsyncConfigPath(runId),
				runnerAdmissionPath: managedAdmissionPaths.evidencePath,
				runnerAdmissionProceedPath: managedAdmissionPaths.proceedPath,
				runnerAdmissionCommitPath: managedAdmissionPaths.commitPath,
			}
		: {};
	const attestationPaths: Record<string, { path: string | undefined; kind: "directory" | "file" }> = {
		cwd: { path: effectiveCwd, kind: "directory" },
		sessionRoot: { path: sessionRoot, kind: "directory" },
		sessionDir: { path: sessionDir, kind: "directory" },
		sessionFile: { path: sessionFile, kind: "file" },
		artifactsDir: { path: artifactsDir, kind: "directory" },
		outputPath: { path: outputPath, kind: "file" },
		asyncDir: { path: managedRuntimePaths.asyncDir, kind: "directory" },
		resultPath: { path: managedRuntimePaths.resultPath, kind: "file" },
		resultReservationPath: { path: managedRuntimePaths.resultReservationPath, kind: "file" },
		runnerConfigPath: { path: managedRuntimePaths.runnerConfigPath, kind: "file" },
		runnerAdmissionPath: { path: managedRuntimePaths.runnerAdmissionPath, kind: "file" },
		runnerAdmissionProceedPath: { path: managedRuntimePaths.runnerAdmissionProceedPath, kind: "file" },
		runnerAdmissionCommitPath: { path: managedRuntimePaths.runnerAdmissionCommitPath, kind: "file" },
	};
	if (artifactPaths) {
		for (const [name, artifactPath] of Object.entries(artifactPaths)) {
			if (typeof artifactPath === "string") {
				attestationPaths[`artifactPaths.${name}`] = { path: artifactPath, kind: "file" };
			}
		}
	}
	let rootAttestations: Record<string, SubagentLaunchRootAttestation> | undefined;
	if (input.identityMode === "managed-v1") {
		try {
			rootAttestations = attestLaunchRoots(attestationPaths);
		} catch {
			return {
				ok: false,
				code: "invalid_root",
				message: "A launch write/evidence root could not be attested.",
				diagnostics: [...diagnostics, {
					code: "invalid_root",
					severity: "error",
					message: "A launch write/evidence root could not be attested.",
				}],
			};
		}
	}
	const candidates = candidateList(input.agent, agent, effectiveCwd);
	const shadowedCandidates = candidates.filter((candidate) => !candidate.selected);
	const contractBase: Omit<SubagentLaunchContract, "digest"> = {
		version: SUBAGENT_LAUNCH_CONTRACT_VERSION,
		runId,
		...(input.identityMode === "managed-v1" && input.parentSessionId ? {
			parentSessionIdentityDigest: computeParentSessionIdentityDigest(input.parentSessionId, input.parentSessionFile),
		} : {}),
		agent: {
			name: agent.name,
			...(agent.localName ? { localName: agent.localName } : {}),
			...(agent.packageName ? { packageName: agent.packageName } : {}),
			source: agent.source,
			filePath: agent.filePath,
			...(input.identityMode === "managed-v1" ? { definitionDigest: digestAgentDefinition(agent) } : {}),
			shadowedCandidates,
		},
		context: input.context ?? agent.defaultContext ?? "fresh",
		...(model ? { model } : {}),
		modelCandidates,
		...(resolveEffectiveThinking(model, effectiveThinkingConfig) ? { thinking: resolveEffectiveThinking(model, effectiveThinkingConfig) } : {}),
		systemPromptMode: agent.systemPromptMode,
		inheritProjectContext: agent.inheritProjectContext,
		inheritSkills: agent.inheritSkills,
		skills: {
			requested: requestedSkills,
			resolved: resolvedSkills.resolved.map((skill) => ({
			name: skill.name,
			path: skill.path,
			source: skill.source,
			...(input.identityMode === "managed-v1" ? { contentDigest: digestManagedSkillContent(skill.content) } : {}),
		})),
			missing: resolvedSkills.missing,
		},
		tools: {
			requestedBuiltin: toolPlan.requestedBuiltinTools,
			declaredBuiltin: toolPlan.declaredBuiltinTools,
			effectiveAllowlist: toolPlan.effectiveToolAllowlist,
			explicitAllowlist: toolPlan.explicitToolAllowlist,
			requiredChildTools: toolPlan.requiredChildTools,
			internalTools: toolPlan.internalTools,
			mcp: toolPlan.effectiveMcpSelections,
			effectiveMcpTools: toolPlan.effectiveMcpTools,
			toolExtensionPaths: toolPlan.toolExtensionPaths,
			runtimeExtensions: toolPlan.runtimeExtensions,
			configuredExtensions: toolPlan.configuredExtensions,
			extensionArgs: toolPlan.extensionArgs,
			disableAmbientExtensions: toolPlan.disableAmbientExtensions,
			fanoutAuthorized: toolPlan.fanoutAuthorized,
			...(toolPlan.capabilityCeiling ? { capabilityCeiling: toolPlan.capabilityCeiling } : {}),
			...(toolPlan.capabilityAudit ? { capabilityAudit: toolPlan.capabilityAudit } : {}),
		},
		roots: {
			cwd: effectiveCwd,
			...(sessionRoot ? { sessionRoot } : {}),
			...(sessionDir ? { sessionDir, sessionFile } : {}),
			...(artifactsDir ? { artifactsDir } : {}),
			...(artifactPaths ? { artifactPaths } : {}),
			...(outputPath ? { outputPath } : {}),
			...managedRuntimePaths,
			...(rootAttestations ? { attestations: rootAttestations } : {}),
		},
		protocol: {
			lifecycleArtifactVersion: SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
			packageVersion: packageVersion(),
		},
		diagnostics,
	};
	return { ok: true, contract: { ...contractBase, digest: digestContract(contractBase) } };
}

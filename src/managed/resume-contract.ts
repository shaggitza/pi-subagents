import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	SUBAGENT_MANAGED_DISPATCH_VERSION,
	assertManagedResumeExecutorRequestV1,
	canonicalizeManagedJson,
	computeManagedProfileContentDigest,
	computeManagedProfileIdentityDigest,
	type JsonObject,
	type ManagedProfileIdentityV1,
	type ManagedResumeExecutorRequestV1,
} from "../api/managed-dispatch.ts";
import {
	resolveSubagentLaunchContract,
	type SubagentLaunchContract,
	type SubagentLaunchContractInput,
	type SubagentLaunchContractResult,
} from "../api/preflight.ts";
import type { ManagedResumeSourceV1 } from "./resume-source.ts";

export interface ManagedResumeLaunchContractV1 {
	readonly version: typeof SUBAGENT_MANAGED_DISPATCH_VERSION;
	readonly runId: string;
	readonly parentSessionIdentityDigest: string;
	readonly source: {
		readonly operationId: string;
		readonly requestDigest: string;
		readonly runId: string;
		readonly index: 0;
		readonly terminalProofDigest: string;
		readonly canonicalSessionId: string;
		readonly canonicalSessionFile: string;
		readonly sessionDevice: string;
		readonly sessionInode: string;
		readonly recoveryDescriptorDigest: string;
	};
	readonly launchContract: SubagentLaunchContract;
	readonly digest: string;
}

export interface ResolvedManagedResumeLaunchV1 {
	readonly request: Readonly<ManagedResumeExecutorRequestV1>;
	readonly source: Readonly<ManagedResumeSourceV1>;
	readonly contract: Readonly<ManagedResumeLaunchContractV1>;
	readonly profile: ManagedProfileIdentityV1;
	readonly profileIdentityDigest: string;
}

export interface ManagedResumeLaunchResolverOptions {
	artifactDir?: "project" | "session" | "temp";
	resolveContract?: (input: SubagentLaunchContractInput) => Promise<SubagentLaunchContractResult>;
}

function hash(domain: string, value: unknown): string {
	return createHash("sha256")
		.update(domain, "utf8")
		.update("\0", "utf8")
		.update(canonicalizeManagedJson(value).serialization, "utf8")
		.digest("hex");
}

function sourceSessionRoot(sessionFile: string): string {
	return path.dirname(path.dirname(sessionFile));
}

/** Non-launching host resolver for an already-authorized exact source snapshot. */
export async function resolveManagedResumeLaunchV1(
	requestInput: JsonObject,
	candidateRunId: string,
	source: Readonly<ManagedResumeSourceV1>,
	ctx: ExtensionContext,
	parentSessionId: string,
	parentSessionFile: string,
	options: ManagedResumeLaunchResolverOptions = {},
): Promise<ResolvedManagedResumeLaunchV1> {
	const request = assertManagedResumeExecutorRequestV1(requestInput, source.sourceRunId, source.sourceIndex);
	const descriptor = source.recoveryDescriptor;
	const contractResult = await (options.resolveContract ?? resolveSubagentLaunchContract)({
		agent: source.agent,
		cwd: source.cwd,
		task: request.message,
		context: "fresh",
		model: source.model,
		thinking: source.thinking,
		parentModel: ctx.model ? { provider: ctx.model.provider, id: ctx.model.id } : undefined,
		availableModels: ctx.modelRegistry.getAvailable(),
		preferredProvider: ctx.model?.provider,
		skill: descriptor.skills,
		output: descriptor.outputPath,
		outputMode: descriptor.outputMode,
		outputSchema: descriptor.structuredOutputSchema,
		artifacts: descriptor.artifactConfig?.enabled ?? false,
		artifactDir: options.artifactDir,
		parentSessionFile,
		identityMode: "managed-v1",
		parentSessionId,
		sessionDir: sourceSessionRoot(source.canonicalSessionFile),
		runId: candidateRunId,
		capabilityCeiling: descriptor.capabilityCeiling,
	});
	if (contractResult.ok === false || contractResult.contract.diagnostics.some((diagnostic) => diagnostic.severity !== "warning")) {
		throw new TypeError("Managed resume launch contract preflight failed.");
	}
	const launchContract = contractResult.contract;
	if (launchContract.roots.sessionFile !== source.canonicalSessionFile || launchContract.parentSessionIdentityDigest === undefined
		|| !launchContract.agent.definitionDigest || !launchContract.roots.attestations) {
		throw new TypeError("Managed resume launch contract lacks exact source or host identity.");
	}
	const base = {
		version: 1 as const,
		runId: candidateRunId,
		parentSessionIdentityDigest: launchContract.parentSessionIdentityDigest,
		source: {
			operationId: source.sourceOperationId,
			requestDigest: source.sourceRequestDigest,
			runId: source.sourceRunId,
			index: 0 as const,
			terminalProofDigest: source.sourceTerminalProofDigest,
			canonicalSessionId: source.canonicalSessionId,
			canonicalSessionFile: source.canonicalSessionFile,
			sessionDevice: source.sessionDevice,
			sessionInode: source.sessionInode,
			recoveryDescriptorDigest: source.recoveryDescriptorDigest,
		},
		launchContract,
	};
	const contract: ManagedResumeLaunchContractV1 = Object.freeze({
		...base,
		digest: hash("pi-subagents/managed-dispatch/v1/resume-contract", base),
	});
	const rootRealPath = fs.realpathSync(source.cwd);
	const rootStats = fs.statSync(rootRealPath, { bigint: true });
	if (!rootStats.isDirectory()) throw new TypeError("Managed resume profile root is not a directory.");
	const profile: ManagedProfileIdentityV1 = {
		version: 1,
		contentDigest: computeManagedProfileContentDigest({
			version: 1,
			launchProfileDigest: hash("pi-subagents/managed-dispatch/v1/resume-profile", {
				launchContractDigest: launchContract.digest,
				source: base.source,
			}),
		}),
		root: { version: 1, realPath: rootRealPath, device: String(rootStats.dev), inode: String(rootStats.ino) },
	};
	return Object.freeze({ request, source, contract, profile, profileIdentityDigest: computeManagedProfileIdentityDigest(profile) });
}

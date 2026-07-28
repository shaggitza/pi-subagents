import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { SubagentLaunchContractInput } from "../../src/api/preflight.ts";
import { resolveManagedResumeLaunchV1 } from "../../src/managed/resume-contract.ts";
import type { ManagedResumeSourceV1 } from "../../src/managed/resume-source.ts";

let temporary = "";
beforeEach(() => { temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-resume-contract-")); });
afterEach(() => { fs.rmSync(temporary, { recursive: true, force: true }); });

function source(overrides: Partial<ManagedResumeSourceV1> = {}): ManagedResumeSourceV1 {
	const sessionFile = path.join(temporary, "sessions", "source", "run-0", "session.jsonl");
	fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
	fs.writeFileSync(sessionFile, "{}\n", "utf8");
	return {
		version: 1,
		consumerId: "pi-signal" as ManagedResumeSourceV1["consumerId"],
		sourceOperationId: Buffer.alloc(32, 7).toString("base64url"),
		sourceRequestDigest: "1".repeat(64),
		sourceRunId: "source-run",
		sourceIndex: 0,
		sourceTerminalProofDigest: "2".repeat(64),
		canonicalSessionFile: sessionFile,
		canonicalSessionId: "3".repeat(64),
		sessionDevice: "4",
		sessionInode: "5",
		recoveryDescriptor: {
			version: 1,
			sourceRunId: "source-run",
			agent: "worker",
			sessionFile,
			cwd: temporary,
			systemPromptMode: "replace",
			inheritProjectContext: false,
			inheritSkills: false,
			outputMode: "inline",
			maxSubagentDepth: 1,
			share: false,
		},
		recoveryDescriptorDigest: "6".repeat(64),
		agent: "worker",
		cwd: temporary,
		...overrides,
	};
}

function context(): ExtensionContext {
	return {
		model: { provider: "test", id: "parent" },
		modelRegistry: { getAvailable: () => [] },
	} as unknown as ExtensionContext;
}

function resolver(input: SubagentLaunchContractInput) {
	const sessionRoot = input.sessionDir!;
	const sessionDir = path.join(sessionRoot, "run-0");
	return Promise.resolve({
		ok: true as const,
		contract: {
			version: 1 as const,
			runId: input.runId!,
			parentSessionIdentityDigest: "a".repeat(64),
			agent: { name: input.agent, source: "project" as const, filePath: path.join(temporary, "worker.md"), definitionDigest: "b".repeat(64), shadowedCandidates: [] },
			context: "fresh" as const,
			modelCandidates: [],
			systemPromptMode: "replace" as const,
			inheritProjectContext: false,
			inheritSkills: false,
			skills: { requested: [], resolved: [], missing: [] },
			tools: { requestedBuiltin: [], declaredBuiltin: [], effectiveAllowlist: [], explicitAllowlist: true, requiredChildTools: [], internalTools: [], mcp: [], effectiveMcpTools: [], toolExtensionPaths: [], runtimeExtensions: [], configuredExtensions: [], extensionArgs: [], disableAmbientExtensions: true, fanoutAuthorized: false },
			roots: { cwd: input.cwd, sessionRoot, sessionDir, sessionFile: path.join(sessionDir, "session.jsonl"), attestations: { cwd: { path: input.cwd, existingAncestor: input.cwd, existingAncestorRealPath: input.cwd, projectedRealPath: input.cwd, existingAncestorDevice: "1", existingAncestorInode: "2", relativeSuffix: "" } } },
			protocol: { lifecycleArtifactVersion: 3, packageVersion: "test" },
			diagnostics: [],
			digest: "c".repeat(64),
		},
	});
}

const request = { action: "resume", runId: "source-run", index: 0, message: "continue", async: true, clarify: false, context: "fresh" } as const;

describe("managed resume launch contract", () => {
	it("binds candidate, parent, exact source proof/session, recovery descriptor, and host profile", async () => {
		const resolved = await resolveManagedResumeLaunchV1(request, "candidate-resume", source(), context(), "parent", path.join(temporary, "parent.jsonl"), { resolveContract: resolver });
		assert.equal(resolved.contract.runId, "candidate-resume");
		assert.equal(resolved.contract.source.runId, "source-run");
		assert.equal(resolved.contract.source.index, 0);
		assert.equal(resolved.contract.launchContract.roots.sessionFile, source().canonicalSessionFile);
		assert.match(resolved.contract.digest, /^[a-f0-9]{64}$/);
		assert.match(resolved.profileIdentityDigest, /^[a-f0-9]{64}$/);
		const replay = await resolveManagedResumeLaunchV1(request, "candidate-resume", source(), context(), "parent", path.join(temporary, "parent.jsonl"), { resolveContract: resolver });
		assert.equal(replay.contract.digest, resolved.contract.digest);
		assert.equal(replay.profileIdentityDigest, resolved.profileIdentityDigest);
	});

	it("binds the exact recovered execution spec and tightened active capability ceiling", async () => {
		const artifactsDir = path.join(temporary, "retained-artifacts");
		fs.mkdirSync(artifactsDir, { recursive: true });
		const skillDir = path.join(temporary, "retained-skills");
		fs.mkdirSync(skillDir, { recursive: true });
		const retained = source({
			recoveryDescriptor: {
				version: 1,
				sourceRunId: "source-run",
				agent: "worker",
				sessionFile: path.join(temporary, "sessions", "source", "run-0", "session.jsonl"),
				cwd: temporary,
				model: "test/model",
				fallbackModels: ["test/fallback"],
				tools: ["read", "write"],
				extensions: ["retained-extension.ts"],
				subagentOnlyExtensions: ["retained-child.ts"],
				mcpDirectTools: ["server/tool"],
				systemPrompt: "retained prompt",
				systemPromptMode: "replace",
				inheritProjectContext: false,
				inheritSkills: false,
				skills: ["retained-skill"],
				skillPath: [skillDir],
				memory: { scope: "project", path: "MEMORY.md" },
				outputPath: path.join(temporary, "retained-output.md"),
				outputMode: "inline",
				artifactConfig: { enabled: true, includeInput: false, includeOutput: true, includeJsonl: false, includeMetadata: true, cleanupDays: 1 },
				artifactsDir,
				maxSubagentDepth: 1,
				share: false,
				capabilityCeiling: { version: 1, allowedTools: ["read", "write"], denyExtensions: false, sources: ["source"] },
			},
		});
		let captured: SubagentLaunchContractInput | undefined;
		const result = await resolveManagedResumeLaunchV1(request, "candidate-resume", retained, context(), "parent", path.join(temporary, "parent.jsonl"), {
			resolveContract: (input) => { captured = input; return resolver(input); },
			resolveCapabilityCeiling: () => ({ version: 1, allowedTools: ["read"], denyExtensions: true, sources: ["current"] }),
		});
		assert.deepEqual(result.execution.capabilityCeiling?.allowedTools, ["read"]);
		assert.equal(result.execution.capabilityCeiling?.denyExtensions, true);
		assert.deepEqual(captured?.capabilityCeiling, result.execution.capabilityCeiling);
		assert.equal(captured?.managedArtifactsDir, artifactsDir);
		assert.deepEqual(captured?.managedAgentConfig?.fallbackModels, ["test/fallback"]);
		assert.deepEqual(captured?.managedAgentConfig?.tools, ["read", "write"]);
		assert.deepEqual(captured?.managedAgentConfig?.extensions, ["retained-extension.ts"]);
		assert.equal(captured?.managedAgentConfig?.systemPrompt, "retained prompt");
		assert.deepEqual(captured?.managedAgentConfig?.skillPath, [skillDir]);
		assert.deepEqual(captured?.managedAgentConfig?.memory, { scope: "project", path: "MEMORY.md" });
	});

	it("changes the operation contract but retains role profile identity when source proof changes", async () => {
		const first = await resolveManagedResumeLaunchV1(request, "candidate-resume", source(), context(), "parent", path.join(temporary, "parent.jsonl"), { resolveContract: resolver });
		const changed = await resolveManagedResumeLaunchV1(request, "candidate-resume", source({ sourceTerminalProofDigest: "9".repeat(64) }), context(), "parent", path.join(temporary, "parent.jsonl"), { resolveContract: resolver });
		assert.notEqual(changed.contract.digest, first.contract.digest);
		assert.equal(changed.profileIdentityDigest, first.profileIdentityDigest);
	});

	it("rejects alternate source selectors before contract resolution", async () => {
		await assert.rejects(resolveManagedResumeLaunchV1({ ...request, runId: "prefix" }, "candidate-resume", source(), context(), "parent", path.join(temporary, "parent.jsonl"), { resolveContract: resolver }));
	});
});

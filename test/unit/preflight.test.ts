import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { registerSubagentCapabilityCeiling, resolveSubagentCapabilityCeiling } from "../../src/api/capability-ceiling.ts";
import { resolveSubagentLaunchContract, SUBAGENT_LAUNCH_CONTRACT_VERSION } from "../../src/api/preflight.ts";
import { clearSkillCache } from "../../src/agents/skills.ts";
import { computeMcpServerHash } from "../../src/runs/shared/mcp-direct-tool-allowlist.ts";
import { ASYNC_DIR, RESULTS_DIR, getAsyncConfigPath } from "../../src/shared/types.ts";
import { preparedResultReservationPath } from "../../src/runs/background/prepared-result-reservation.ts";
import { preparedRunnerAdmissionPaths } from "../../src/runs/background/prepared-runner-admission.ts";

let tempDir = "";
let previousHome: string | undefined;
let previousUserProfile: string | undefined;
let previousAgentDir: string | undefined;

function writeAgent(filePath: string, body: string): void {
	fs.mkdirSync(path.dirname(filePath), { recursive: true });
	fs.writeFileSync(filePath, body, "utf-8");
}

function writeSkill(cwd: string, name: string): void {
	const skillDir = path.join(cwd, ".pi", "skills", name);
	fs.mkdirSync(skillDir, { recursive: true });
	fs.writeFileSync(path.join(skillDir, "SKILL.md"), `---\ndescription: ${name}\n---\n\nUse ${name}.\n`, "utf-8");
}

function writeJson(filePath: string, value: unknown): void {
	fs.mkdirSync(path.dirname(filePath), { recursive: true });
	fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf-8");
}

function writeMcpFixture(): void {
	const agentDir = process.env.PI_CODING_AGENT_DIR;
	assert.equal(typeof agentDir, "string");
	const definition = { command: "github-mcp" };
	writeJson(path.join(agentDir, "mcp.json"), { mcpServers: { github: definition } });
	writeJson(path.join(agentDir, "mcp-cache.json"), {
		version: 1,
		servers: {
			github: {
				configHash: computeMcpServerHash(definition),
				cachedAt: Date.now(),
				tools: [{ name: "search_repositories" }, { name: "create_issue" }],
				resources: [],
			},
		},
	});
}

describe("public launch contract preflight", () => {
	beforeEach(() => {
		tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-preflight-"));
		previousHome = process.env.HOME;
		previousUserProfile = process.env.USERPROFILE;
		previousAgentDir = process.env.PI_CODING_AGENT_DIR;
		const home = path.join(tempDir, "home");
		process.env.HOME = home;
		process.env.USERPROFILE = home;
		process.env.PI_CODING_AGENT_DIR = path.join(home, ".pi", "agent");
		clearSkillCache();
	});

	afterEach(() => {
		clearSkillCache();
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
		if (previousUserProfile === undefined) delete process.env.USERPROFILE;
		else process.env.USERPROFILE = previousUserProfile;
		if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
		else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
		fs.rmSync(tempDir, { recursive: true, force: true });
	});

	it("resolves an ordinary single-agent contract without creating launch directories", async () => {
		const cwd = path.join(tempDir, "repo");
		fs.mkdirSync(cwd, { recursive: true });
		writeSkill(cwd, "project-skill");
		writeAgent(path.join(cwd, ".pi", "agents", "worker.md"), `---
name: worker
description: Project worker
tools:
  - read
  - write
  - /tmp/private-tool.ts
model: test/primary
fallbackModels:
  - test/fallback
thinking: high
skills:
  - project-skill
output: report.md
---
Project prompt.
`);
		const sessionRoot = path.join(tempDir, "sessions");
		const handle = registerSubagentCapabilityCeiling({ sessionId: "preflight-session", ceiling: { allowedTools: ["read"], denyExtensions: true }, source: "test" });
		try {
			const ceiling = resolveSubagentCapabilityCeiling("preflight-session");
			const result = await resolveSubagentLaunchContract({
				agent: "worker",
				cwd,
				task: "Inspect the repo",
				runId: "run-123",
				identityMode: "managed-v1",
				parentSessionId: "parent-session",
				sessionRoot,
				availableModels: [
					{ provider: "test", id: "primary", fullId: "test/primary" },
					{ provider: "test", id: "fallback", fullId: "test/fallback" },
				],
				capabilityCeiling: ceiling,
			});

			assert.equal(result.ok, true);
			assert.equal(result.contract.version, SUBAGENT_LAUNCH_CONTRACT_VERSION);
			assert.equal(result.contract.agent.source, "project");
			assert.ok(result.contract.agent.shadowedCandidates.some((candidate) => candidate.name === "worker" && candidate.source === "builtin"));
			assert.equal(result.contract.model, "test/primary:high");
			assert.deepEqual(result.contract.modelCandidates, ["test/primary:high", "test/fallback:high"]);
			assert.equal(result.contract.thinking, "high");
			assert.deepEqual(result.contract.skills.requested, ["project-skill"]);
			assert.equal(result.contract.skills.resolved[0]?.name, "project-skill");
			assert.deepEqual(result.contract.tools.effectiveAllowlist, ["read"]);
			assert.deepEqual(result.contract.tools.capabilityAudit?.removedTools, ["write"]);
			assert.equal(result.contract.tools.capabilityAudit?.removedExtensionCount, 1);
			assert.equal(result.contract.tools.disableAmbientExtensions, true);
			assert.equal(result.contract.roots.sessionFile, path.join(sessionRoot, "run-123", "run-0", "session.jsonl"));
			assert.equal(result.contract.roots.outputPath, path.join(cwd, ".pi-subagents", "artifacts", "outputs", "run-123", "report.md"));
			assert.equal(result.contract.roots.asyncDir, path.join(ASYNC_DIR, "run-123"));
			assert.equal(result.contract.roots.resultPath, path.join(RESULTS_DIR, "run-123.json"));
			assert.equal(result.contract.roots.resultReservationPath, preparedResultReservationPath(path.join(RESULTS_DIR, "run-123.json")));
			assert.equal(result.contract.roots.runnerConfigPath, getAsyncConfigPath("run-123"));
			const admissionPaths = preparedRunnerAdmissionPaths(path.join(ASYNC_DIR, "run-123"));
			assert.equal(result.contract.roots.runnerAdmissionPath, admissionPaths.evidencePath);
			assert.equal(result.contract.roots.runnerAdmissionProceedPath, admissionPaths.proceedPath);
			assert.equal(result.contract.roots.runnerAdmissionCommitPath, admissionPaths.commitPath);
			const attestations = result.contract.roots.attestations;
			assert.ok(attestations);
			assert.deepEqual(
				Object.keys(attestations).sort(),
				[
					"artifactPaths.inputPath",
					"artifactPaths.jsonlPath",
					"artifactPaths.metadataPath",
					"artifactPaths.outputPath",
					"artifactPaths.transcriptPath",
					"artifactsDir",
					"asyncDir",
					"cwd",
					"outputPath",
					"resultPath",
					"resultReservationPath",
					"runnerConfigPath",
					"runnerAdmissionPath",
					"runnerAdmissionProceedPath",
					"runnerAdmissionCommitPath",
					"sessionDir",
					"sessionFile",
					"sessionRoot",
				].sort(),
			);
			assert.equal(attestations.cwd.existingAncestorRealPath, fs.realpathSync(cwd));
			assert.match(result.contract.digest, /^[a-f0-9]{64}$/);
			const repeated = await resolveSubagentLaunchContract({
				agent: "worker",
				cwd,
				task: "Inspect the repo",
				runId: "run-123",
				identityMode: "managed-v1",
				parentSessionId: "parent-session",
				sessionRoot,
				availableModels: [
					{ provider: "test", id: "primary", fullId: "test/primary" },
					{ provider: "test", id: "fallback", fullId: "test/fallback" },
				],
				capabilityCeiling: ceiling,
			});
			assert.equal(repeated.ok, true);
			assert.equal(repeated.contract.digest, result.contract.digest);
			assert.match(result.contract.parentSessionIdentityDigest, /^[a-f0-9]{64}$/);
			const otherParent = await resolveSubagentLaunchContract({
				agent: "worker",
				cwd,
				task: "Inspect the repo",
				runId: "run-123",
				identityMode: "managed-v1",
				parentSessionId: "other-parent-session",
				sessionRoot,
				availableModels: [
					{ provider: "test", id: "primary", fullId: "test/primary" },
					{ provider: "test", id: "fallback", fullId: "test/fallback" },
				],
				capabilityCeiling: ceiling,
			});
			assert.equal(otherParent.ok, true);
			assert.notEqual(otherParent.contract.parentSessionIdentityDigest, result.contract.parentSessionIdentityDigest);
			assert.notEqual(otherParent.contract.digest, result.contract.digest);
			assert.match(result.contract.agent.definitionDigest, /^[a-f0-9]{64}$/);
			fs.appendFileSync(path.join(cwd, ".pi", "agents", "worker.md"), "\nChanged prompt content.\n", "utf-8");
			const changedDefinition = await resolveSubagentLaunchContract({
				agent: "worker",
				cwd,
				task: "Inspect the repo",
				runId: "run-123",
				identityMode: "managed-v1",
				parentSessionId: "parent-session",
				sessionRoot,
				availableModels: [
					{ provider: "test", id: "primary", fullId: "test/primary" },
					{ provider: "test", id: "fallback", fullId: "test/fallback" },
				],
				capabilityCeiling: ceiling,
			});
			assert.equal(changedDefinition.ok, true);
			assert.notEqual(changedDefinition.contract.agent.definitionDigest, result.contract.agent.definitionDigest);
			assert.notEqual(changedDefinition.contract.digest, result.contract.digest);
			assert.equal(fs.existsSync(sessionRoot), false);
			assert.equal(fs.existsSync(path.join(cwd, ".pi-subagents")), false);
		} finally {
			handle.dispose();
		}
	});

	it("treats an explicit sessionDir as the ordinary executor session root", async () => {
		const cwd = path.join(tempDir, "repo");
		fs.mkdirSync(cwd, { recursive: true });
		writeAgent(path.join(cwd, ".pi", "agents", "worker.md"), `---
name: worker
description: Project worker
---
Exact session path prompt.
`);
		const sessionRoot = path.join(tempDir, "managed", "operation-1");
		const result = await resolveSubagentLaunchContract({
			agent: "worker",
			cwd,
			runId: "candidate-1",
			identityMode: "managed-v1",
			parentSessionId: "parent-session",
			sessionDir: sessionRoot,
		});
		assert.equal(result.ok, true);
		assert.equal(result.contract.roots.sessionRoot, sessionRoot);
		assert.equal(result.contract.roots.sessionDir, path.join(sessionRoot, "run-0"));
		assert.equal(result.contract.roots.sessionFile, path.join(sessionRoot, "run-0", "session.jsonl"));
		assert.equal(result.contract.roots.asyncDir, path.join(ASYNC_DIR, "candidate-1"));
		assert.equal(result.contract.roots.resultPath, path.join(RESULTS_DIR, "candidate-1.json"));
		assert.equal(result.contract.roots.resultReservationPath, preparedResultReservationPath(path.join(RESULTS_DIR, "candidate-1.json")));
		assert.equal(result.contract.roots.runnerConfigPath, getAsyncConfigPath("candidate-1"));
		const candidateAdmissionPaths = preparedRunnerAdmissionPaths(path.join(ASYNC_DIR, "candidate-1"));
		assert.equal(result.contract.roots.runnerAdmissionPath, candidateAdmissionPaths.evidencePath);
		assert.equal(result.contract.roots.runnerAdmissionProceedPath, candidateAdmissionPaths.proceedPath);
		assert.equal(result.contract.roots.runnerAdmissionCommitPath, candidateAdmissionPaths.commitPath);
		assert.equal(result.contract.roots.attestations?.sessionDir.path, path.join(sessionRoot, "run-0"));
		assert.equal(result.contract.roots.attestations?.sessionDir.existingAncestorRealPath, fs.realpathSync(tempDir));
		assert.equal(fs.existsSync(sessionRoot), false);

		const ordinary = await resolveSubagentLaunchContract({
			agent: "worker",
			cwd,
			runId: "ordinary-1",
			sessionDir: sessionRoot,
		});
		assert.equal(ordinary.ok, true);
		assert.equal(ordinary.contract.parentSessionIdentityDigest, undefined);
		assert.equal(ordinary.contract.agent.definitionDigest, undefined);
		assert.equal(ordinary.contract.roots.attestations, undefined);
		assert.equal(ordinary.contract.diagnostics.some((diagnostic) => diagnostic.message.includes("parent session identity")), false);
	});

	it("returns closed failures for missing agents and missing skills", async () => {
		const cwd = path.join(tempDir, "repo");
		fs.mkdirSync(cwd, { recursive: true });
		writeAgent(path.join(cwd, ".pi", "agents", "worker.md"), `---
name: worker
description: Project worker
skills:
  - missing-skill
---
Project prompt.
`);

		const missingAgent = await resolveSubagentLaunchContract({ agent: "missing", cwd });
		assert.deepEqual(missingAgent, { ok: false, code: "missing_agent", message: "Unknown agent: missing", diagnostics: [] });

		const missingSkill = await resolveSubagentLaunchContract({ agent: "worker", cwd });
		assert.equal(missingSkill.ok, false);
		assert.equal(missingSkill.code, "missing_skill");
		assert.match(missingSkill.message, /missing-skill/);
	});

	it("fails closed for invalid runtime inputs", async () => {
		const cwd = path.join(tempDir, "repo");
		fs.mkdirSync(cwd, { recursive: true });
		writeAgent(path.join(cwd, ".pi", "agents", "worker.md"), `---
name: worker
description: Project worker
---
Project prompt.
`);

		const invalidCwd = await resolveSubagentLaunchContract({ agent: "worker", cwd: path.join(tempDir, "missing") });
		assert.equal(invalidCwd.ok, false);
		assert.equal(invalidCwd.code, "invalid_cwd");

		const unsupportedMode = await resolveSubagentLaunchContract({ agent: "worker", cwd, context: "bogus" as never });
		assert.equal(unsupportedMode.ok, false);
		assert.equal(unsupportedMode.code, "unsupported_mode");

		const invalidArtifactDir = await resolveSubagentLaunchContract({ agent: "worker", cwd, artifactDir: "bogus" as never });
		assert.equal(invalidArtifactDir.ok, false);
		assert.equal(invalidArtifactDir.code, "invalid_artifact_dir");

		const fileAsSessionRoot = path.join(tempDir, "not-a-directory");
		fs.writeFileSync(fileAsSessionRoot, "file", "utf8");
		const invalidRoot = await resolveSubagentLaunchContract({
			agent: "worker",
			cwd,
			identityMode: "managed-v1",
			parentSessionId: "parent-session",
			sessionDir: fileAsSessionRoot,
		});
		assert.equal(invalidRoot.ok, false);
		assert.equal(invalidRoot.code, "invalid_root");

		if (fs.existsSync("/dev/null")) {
			const specialOutput = path.join(tempDir, "special-output");
			fs.symlinkSync("/dev/null", specialOutput);
			const invalidSpecialFile = await resolveSubagentLaunchContract({
				agent: "worker",
				cwd,
				identityMode: "managed-v1",
				parentSessionId: "parent-session",
				sessionDir: path.join(tempDir, "sessions"),
				artifacts: false,
				output: specialOutput,
			});
			assert.equal(invalidSpecialFile.ok, false);
			assert.equal(invalidSpecialFile.code, "invalid_root");
		}
	});

	it("projects MCP, extension, fanout, structured-output, and fork diagnostics", async () => {
		const cwd = path.join(tempDir, "repo");
		fs.mkdirSync(cwd, { recursive: true });
		writeMcpFixture();
		writeAgent(path.join(cwd, ".pi", "agents", "fanout.md"), `---
name: fanout
description: Project fanout
tools:
  - read
  - subagent
  - /tmp/tool-ext.ts
  - mcp:github/search_repositories
extensions:
  - /tmp/config-ext.ts
subagentOnlyExtensions:
  - /tmp/subagent-only.ts
defaultContext: fork
---
Project prompt.
`);

		const result = await resolveSubagentLaunchContract({
			agent: "fanout",
			cwd,
			outputSchema: { type: "object", additionalProperties: false },
		});
		assert.equal(result.ok, true);
		assert.equal(result.contract.context, "fork");
		assert.ok(result.contract.diagnostics.some((diagnostic) => diagnostic.code === "host_required"));
		assert.deepEqual(result.contract.tools.declaredBuiltin, ["read", "subagent"]);
		assert.equal(result.contract.tools.explicitAllowlist, true);
		assert.equal(result.contract.tools.fanoutAuthorized, true);
		assert.deepEqual(result.contract.tools.internalTools, ["structured_output"]);
		assert.deepEqual(result.contract.tools.effectiveMcpTools, ["github_search_repositories"]);
		assert.deepEqual(result.contract.tools.requiredChildTools, ["read", "subagent", "github_search_repositories", "structured_output"]);
		assert.deepEqual(result.contract.tools.toolExtensionPaths, ["/tmp/tool-ext.ts"]);
		assert.equal(result.contract.tools.disableAmbientExtensions, true);
		assert.ok(result.contract.tools.runtimeExtensions.some((extensionPath) => extensionPath.endsWith("subagent-prompt-runtime.ts")));
		assert.ok(result.contract.tools.runtimeExtensions.some((extensionPath) => extensionPath.endsWith("fanout-child.ts")));
		assert.ok(result.contract.tools.extensionArgs.includes("/tmp/config-ext.ts"));
		assert.ok(result.contract.tools.extensionArgs.includes("/tmp/subagent-only.ts"));
	});

	it("fails closed when a capability ceiling denies read required for child skills", async () => {
		const cwd = path.join(tempDir, "repo");
		fs.mkdirSync(cwd, { recursive: true });
		writeSkill(cwd, "project-skill");
		writeAgent(path.join(cwd, ".pi", "agents", "worker.md"), `---
name: worker
description: Project worker
tools:
  - read
skills:
  - project-skill
---
Project prompt.
`);

		const result = await resolveSubagentLaunchContract({
			agent: "worker",
			cwd,
			capabilityCeiling: { version: 1, allowedTools: [], denyExtensions: false, sources: ["test"] },
		});
		assert.equal(result.ok, false);
		assert.equal(result.code, "denied_required_tool");
		assert.match(result.message, /excludes required tool 'read'/);
	});
});

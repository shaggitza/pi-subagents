import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { computeManagedRequestDigest, type JsonObject } from "../../src/api/managed-dispatch.ts";
import type { SubagentLaunchContract } from "../../src/api/preflight.ts";
import { ManagedOperationJournal } from "../../src/managed/operation-journal.ts";
import {
	ManagedSpawnCoordinator,
	type ManagedSpawnExecutor,
} from "../../src/managed/spawn-coordinator.ts";
import {
	computePreparedRunnerAdmissionTokenDigest,
	createPreparedRunnerAdmission,
	preparedRunnerAdmissionPaths,
	writePreparedRunnerAdmissionEvidence,
} from "../../src/runs/background/prepared-runner-admission.ts";
import { preparedResultReservationPath } from "../../src/runs/background/prepared-result-reservation.ts";
import { finalizeProcessTerminal, writeProcessTerminalCandidate } from "../../src/runs/background/process-terminal.ts";
import type { PreparedSubagentSpawnPlan, SubagentParamsLike } from "../../src/runs/foreground/subagent-executor.ts";
import { ASYNC_DIR, RESULTS_DIR, getAsyncConfigPath } from "../../src/shared/types.ts";
import { canonicalSessionId } from "../../src/runs/shared/session-lease.ts";

let temporary = "";
const parentDigest = "c".repeat(64);
const profileDigest = "a".repeat(64);
const contractDigest = "b".repeat(64);
const hostId = "host-1";

beforeEach(() => {
	temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-spawn-coordinator-"));
});

afterEach(() => {
	fs.rmSync(temporary, { recursive: true, force: true });
	for (const entry of fs.readdirSync(ASYNC_DIR, { withFileTypes: true })) {
		if (entry.name.startsWith("managed-coordinator-test-")) {
			fs.rmSync(path.join(ASYNC_DIR, entry.name), { recursive: true, force: true });
		}
	}
});

function operationId(byte = 11): string {
	return Buffer.alloc(32, byte).toString("base64url");
}

function candidate(suffix: string): string {
	return `managed-coordinator-test-${suffix}-${Date.now()}`;
}

function executorRequest(runId: string): JsonObject {
	return {
		agent: "worker",
		task: "host-owned opaque task",
		context: "fresh",
		async: true,
		clarify: false,
		cwd: temporary,
		sessionDir: path.join(temporary, `session-${runId}`),
		artifacts: false,
		output: false,
	};
}

function spawnRequest(runId: string, requestId = "transport-1", managedOperationId = operationId()): Record<string, unknown> {
	return {
		version: 1,
		requestId,
		method: "spawn",
		managed: { version: 1, consumerId: "pi-signal", operationId: managedOperationId },
		expectedLaunch: {
			version: 1,
			hostId,
			candidateRunId: runId,
			profileIdentityDigest: profileDigest,
			parentSessionIdentityDigest: parentDigest,
			contractDigest,
		},
		input: { request: executorRequest(runId) },
	};
}

function activeContext(): ExtensionContext {
	return {
		cwd: temporary,
		model: { provider: "test", id: "parent" },
		modelRegistry: { getAvailable: () => [] },
		sessionManager: {
			getSessionId: () => "parent-session",
			getSessionFile: () => path.join(temporary, "parent.jsonl"),
		},
	} as unknown as ExtensionContext;
}

function contract(runId: string, digest = contractDigest): SubagentLaunchContract {
	const sessionRoot = path.join(temporary, `session-${runId}`);
	const sessionDir = path.join(sessionRoot, "run-0");
	const asyncDir = path.join(ASYNC_DIR, runId);
	const resultPath = path.join(RESULTS_DIR, `${runId}.json`);
	const admission = preparedRunnerAdmissionPaths(asyncDir);
	return {
		version: 1,
		runId,
		parentSessionIdentityDigest: parentDigest,
		agent: {
			name: "worker",
			source: "project",
			filePath: path.join(temporary, ".pi", "agents", "worker.md"),
			definitionDigest: "d".repeat(64),
			shadowedCandidates: [],
		},
		context: "fresh",
		modelCandidates: [],
		systemPromptMode: "replace",
		inheritProjectContext: false,
		inheritSkills: false,
		skills: { requested: [], resolved: [], missing: [] },
		tools: {
			requestedBuiltin: [], declaredBuiltin: [], effectiveAllowlist: [], explicitAllowlist: true,
			requiredChildTools: [], internalTools: [], mcp: [], effectiveMcpTools: [], toolExtensionPaths: [],
			runtimeExtensions: [], configuredExtensions: [], extensionArgs: [], disableAmbientExtensions: true,
			fanoutAuthorized: false,
		},
		roots: {
			cwd: temporary,
			sessionRoot,
			sessionDir,
			sessionFile: path.join(sessionDir, "session.jsonl"),
			asyncDir,
			resultPath,
			resultReservationPath: preparedResultReservationPath(resultPath),
			runnerConfigPath: getAsyncConfigPath(runId),
			runnerAdmissionPath: admission.evidencePath,
			runnerAdmissionProceedPath: admission.proceedPath,
			runnerAdmissionCommitPath: admission.commitPath,
			attestations: {},
		},
		protocol: { lifecycleArtifactVersion: 3, packageVersion: "test" },
		diagnostics: [],
		digest,
	};
}

function contractWithAttestedSessionRoot(runId: string): SubagentLaunchContract {
	const launchRoot = path.join(temporary, `attested-root-${runId}`);
	fs.mkdirSync(launchRoot, { mode: 0o700 });
	const sessionRoot = path.join(launchRoot, "session");
	const base = contract(runId);
	const stats = fs.statSync(launchRoot, { bigint: true });
	const sessionDir = path.join(sessionRoot, "run-0");
	return {
		...base,
		roots: {
			...base.roots,
			sessionRoot,
			sessionDir,
			sessionFile: path.join(sessionDir, "session.jsonl"),
			attestations: {
				sessionRoot: {
					path: sessionRoot,
					existingAncestor: launchRoot,
					existingAncestorRealPath: fs.realpathSync(launchRoot),
					projectedRealPath: sessionRoot,
					existingAncestorDevice: String(stats.dev),
					existingAncestorInode: String(stats.ino),
					relativeSuffix: "session",
				},
			},
		},
	};
}

function plan(resolved: SubagentLaunchContract): PreparedSubagentSpawnPlan {
	return {
		runId: resolved.runId,
		parentSessionId: "parent-session",
		parentSessionFile: path.join(temporary, "parent.jsonl"),
		cwd: resolved.roots.cwd,
		sessionRoot: resolved.roots.sessionRoot!,
		sessionDir: resolved.roots.sessionDir!,
		sessionFile: resolved.roots.sessionFile!,
		asyncDir: resolved.roots.asyncDir!,
		resultPath: resolved.roots.resultPath!,
		resultReservationPath: resolved.roots.resultReservationPath!,
		runnerConfigPath: resolved.roots.runnerConfigPath!,
		runnerAdmissionPath: resolved.roots.runnerAdmissionPath!,
		runnerAdmissionProceedPath: resolved.roots.runnerAdmissionProceedPath!,
		runnerAdmissionCommitPath: resolved.roots.runnerAdmissionCommitPath!,
		artifactsDir: resolved.roots.artifactsDir,
	};
}

function resolved(runId: string, digest = contractDigest) {
	return {
		params: executorRequest(runId) as unknown as SubagentParamsLike,
		contract: contract(runId, digest),
		profile: {
			version: 1 as const,
			contentDigest: "e".repeat(64),
			root: { version: 1 as const, realPath: temporary },
		},
		profileIdentityDigest: profileDigest,
	};
}

function makeCoordinator(
	journal: ManagedOperationJournal,
	executor: ManagedSpawnExecutor,
	resolveLaunch: NonNullable<ConstructorParameters<typeof ManagedSpawnCoordinator>[0]["resolveLaunch"]>,
): ManagedSpawnCoordinator {
	const ctx = activeContext();
	return new ManagedSpawnCoordinator({
		journal,
		executor,
		getContext: () => ctx,
		getSessionGeneration: () => 1,
		loadHostId: () => hostId,
		resolveLaunch,
	});
}

function successfulExecutor(runId: string, callCount: { value: number }, terminalize = false): ManagedSpawnExecutor {
	return {
		executePreparedSpawn: (async (_id, _params, _signal, _update, _ctx, options) => {
			callCount.value++;
			const preparedPlan = plan(contract(runId));
			await options.beforeLaunch(preparedPlan);
			options.afterAuthorization(preparedPlan);
			const admission = createPreparedRunnerAdmission(runId, options.dispatchIdentityDigest);
			const ready = { ...admission, state: "ready" as const, pid: 123, runnerProcessInstanceId: "runner-instance-1", observedAt: 100 };
			options.onRunnerReady(ready);
			const accepted = { ...ready, state: "accepted" as const, observedAt: 200 };
			options.onRunnerAccepted(accepted);
			fs.mkdirSync(path.dirname(contract(runId).roots.runnerAdmissionPath!), { recursive: true });
			writePreparedRunnerAdmissionEvidence(
				contract(runId).roots.runnerAdmissionPath!,
				admission,
				"committed",
				123,
				"runner-instance-1",
				300,
			);
			if (terminalize) {
				const sessionFile = contract(runId).roots.sessionFile!;
				fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
				fs.writeFileSync(sessionFile, "{}\n", "utf8");
				const managed = {
					...options.processTerminalBinding,
					runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token),
				};
				writeProcessTerminalCandidate(contract(runId).roots.asyncDir!, {
					version: 1,
					runId,
					runnerProcessInstanceId: "runner-instance-1",
					writers: { "0": [{ processInstanceId: "writer-instance-1", kind: "pi-writer", attempt: 0, closeObservedAt: 350, exitCode: 0, signal: null }] },
					expectedWriters: { "0": 1 },
					sessionFile,
					managed,
				});
				fs.writeFileSync(path.join(contract(runId).roots.asyncDir!, "status.json"), JSON.stringify({ runId, state: "complete", steps: [{ agent: "worker", status: "complete", sessionFile }] }));
				fs.writeFileSync(path.join(contract(runId).roots.asyncDir!, "events.jsonl"), "");
				const proof = finalizeProcessTerminal(contract(runId).roots.asyncDir!, runId, {
					processInstanceId: "runner-instance-1",
					closeObservedAt: 400,
					exitCode: 0,
					signal: null,
				}, managed);
				options.onProcessTerminal(proof);
			}
			return { content: [{ type: "text", text: "started" }], details: { mode: "single", results: [] } };
		}) as ManagedSpawnExecutor["executePreparedSpawn"],
	};
}

describe("unregistered managed spawn coordinator", () => {
	it("binds journal transitions to prepared admission and replays without another launch", async () => {
		const runId = candidate("success");
		const store = new ManagedOperationJournal({ root: path.join(temporary, "journal") });
		const calls = { value: 0 };
		const coordinator = makeCoordinator(store, successfulExecutor(runId, calls), async () => resolved(runId));
		const first = await coordinator.dispatchSpawn(spawnRequest(runId));
		assert.equal(first.state, "accepted");
		assert.equal(first.replayed, false);
		assert.equal(calls.value, 1);
		const durable = store.read(parentDigest, "pi-signal", operationId());
		assert.equal(durable?.state, "accepted");
		assert.equal(durable?.runnerProcessInstanceId, "runner-instance-1");
		assert.match(durable?.runnerAdmissionTokenDigest ?? "", /^[a-f0-9]{64}$/);
		const replay = await coordinator.dispatchSpawn(spawnRequest(runId, "transport-retry"));
		assert.equal(replay.state, "accepted");
		assert.equal(replay.replayed, true);
		assert.equal(calls.value, 1);
		store.close();
	});

	it("terminalizes only from exact durable managed process proof and replays idempotently", async () => {
		const runId = candidate("terminal");
		const root = path.join(temporary, "journal-terminal");
		let store = new ManagedOperationJournal({ root });
		const calls = { value: 0 };
		let coordinator = makeCoordinator(store, successfulExecutor(runId, calls, true), async () => resolved(runId));
		const first = await coordinator.dispatchSpawn(spawnRequest(runId));
		assert.equal(first.state, "terminal");
		const durable = store.read(parentDigest, "pi-signal", operationId());
		assert.equal(durable?.state, "terminal");
		assert.match(durable?.terminalEvidence?.proofDigest ?? "", /^[a-f0-9]{64}$/);
		assert.match(durable?.terminalEvidence?.canonicalSessionId ?? "", /^[a-f0-9]{64}$/);
		assert.equal(durable?.terminalAsyncDir, contract(runId).roots.asyncDir);
		assert.equal(durable?.canonicalSessionFile, contract(runId).roots.sessionFile);
		store.close();

		store = new ManagedOperationJournal({ root });
		coordinator = makeCoordinator(store, {
			executePreparedSpawn: (async () => {
				calls.value++;
				return { content: [], details: { mode: "single", results: [] } };
			}) as ManagedSpawnExecutor["executePreparedSpawn"],
		}, async () => resolved(runId));
		const replay = await coordinator.dispatchSpawn(spawnRequest(runId, "terminal-replay"));
		assert.equal(replay.state, "terminal");
		assert.equal(replay.replayed, true);
		assert.equal(calls.value, 1);
		store.close();
	});

	it("reconciles accepted durable proof to terminal after journal reopen without relaunch", async () => {
		const runId = candidate("restart-terminal");
		const root = path.join(temporary, "journal-restart-terminal");
		let store = new ManagedOperationJournal({ root });
		const request = spawnRequest(runId);
		const digest = computeManagedRequestDigest(request);
		store.claim(parentDigest, request);
		store.transition(parentDigest, "pi-signal", operationId(), digest, "prepared");
		store.transition(parentDigest, "pi-signal", operationId(), digest, "dispatching", {
			runId,
			terminalAsyncDir: contract(runId).roots.asyncDir!,
			canonicalSessionFile: contract(runId).roots.sessionFile!,
		});
		const admission = createPreparedRunnerAdmission(runId, digest);
		store.transition(parentDigest, "pi-signal", operationId(), digest, "runner-ready", {
			runId,
			runnerProcessInstanceId: "runner-restart-1",
			runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token),
		});
		store.transition(parentDigest, "pi-signal", operationId(), digest, "accepted");
		const asyncDir = contract(runId).roots.asyncDir!;
		fs.mkdirSync(asyncDir, { recursive: true });
		writePreparedRunnerAdmissionEvidence(
			contract(runId).roots.runnerAdmissionPath!,
			admission,
			"committed",
			123,
			"runner-restart-1",
		);
		const sessionFile = contract(runId).roots.sessionFile!;
		fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
		fs.writeFileSync(sessionFile, "{}\n", "utf8");
		const managed = {
			version: 1 as const,
			parentSessionIdentityDigest: parentDigest,
			consumerId: "pi-signal",
			operationId: operationId(),
			requestDigest: digest,
			candidateRunId: runId,
			runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token),
		};
		writeProcessTerminalCandidate(asyncDir, {
			version: 1,
			runId,
			runnerProcessInstanceId: "runner-restart-1",
			writers: { "0": [{ processInstanceId: "writer-restart-1", kind: "pi-writer", attempt: 0, closeObservedAt: 350, exitCode: 0, signal: null }] },
			expectedWriters: { "0": 1 },
			sessionFile,
			managed,
		});
		fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId, state: "complete", steps: [{ agent: "worker", status: "complete", sessionFile }] }));
		fs.writeFileSync(path.join(asyncDir, "events.jsonl"), "");
		assert.equal(finalizeProcessTerminal(asyncDir, runId, {
			processInstanceId: "runner-restart-1",
			closeObservedAt: 400,
			exitCode: 0,
			signal: null,
		}, managed).state, "observed");
		store.close();

		store = new ManagedOperationJournal({ root });
		let calls = 0;
		const coordinator = makeCoordinator(store, {
			executePreparedSpawn: (async () => {
				calls++;
				return { content: [], details: { mode: "single", results: [] } };
			}) as ManagedSpawnExecutor["executePreparedSpawn"],
		}, async () => resolved(runId));
		const replay = await coordinator.dispatchSpawn({ ...request, requestId: "restart-terminal-replay" });
		assert.equal(replay.state, "terminal");
		assert.equal(replay.replayed, true);
		assert.equal(calls, 0);
		store.close();
	});

	it("joins concurrent transport retries to one in-process launch", async () => {
		const runId = candidate("concurrent");
		const store = new ManagedOperationJournal({ root: path.join(temporary, "journal") });
		let release!: () => void;
		const gate = new Promise<void>((resolve) => { release = resolve; });
		const calls = { value: 0 };
		const base = successfulExecutor(runId, calls);
		const executor: ManagedSpawnExecutor = {
			executePreparedSpawn: (async (...args: Parameters<ManagedSpawnExecutor["executePreparedSpawn"]>) => {
				await gate;
				return base.executePreparedSpawn(...args);
			}) as ManagedSpawnExecutor["executePreparedSpawn"],
		};
		const coordinator = makeCoordinator(store, executor, async () => resolved(runId));
		const first = coordinator.dispatchSpawn(spawnRequest(runId, "transport-concurrent-1"));
		const second = coordinator.dispatchSpawn(spawnRequest(runId, "transport-concurrent-2"));
		await new Promise((resolve) => setImmediate(resolve));
		const conflicting = spawnRequest(runId, "transport-concurrent-conflict");
		conflicting.input = {
			request: { ...executorRequest(runId), task: "different semantics" },
		};
		await assert.rejects(
			coordinator.dispatchSpawn(conflicting),
			(error: unknown) => error instanceof Error
				&& "code" in error
				&& (error as { code: string }).code === "operation_conflict",
		);
		release();
		const [firstReceipt, secondReceipt] = await Promise.all([first, second]);
		assert.equal(calls.value, 1);
		assert.equal(firstReceipt.state, "accepted");
		assert.equal(secondReceipt.state, "accepted");
		assert.equal([firstReceipt.replayed, secondReceipt.replayed].filter(Boolean).length, 1);
		store.close();
	});

	it("fails a duplicate candidate operation before binding the dispatch boundary", async () => {
		const runId = candidate("duplicate-binding");
		const store = new ManagedOperationJournal({ root: path.join(temporary, "journal-duplicate-binding") });
		const calls = { value: 0 };
		const coordinator = makeCoordinator(store, successfulExecutor(runId, calls), async () => resolved(runId));
		const first = await coordinator.dispatchSpawn(spawnRequest(runId, "duplicate-first", operationId()));
		assert.equal(first.state, "accepted");
		const secondOperationId = operationId(12);
		const second = await coordinator.dispatchSpawn(spawnRequest(runId, "duplicate-second", secondOperationId));
		assert.equal(second.state, "failed-before-launch");
		assert.equal(store.read(parentDigest, "pi-signal", secondOperationId)?.runId, undefined);
		assert.equal(store.readByRun(parentDigest, "pi-signal", runId)?.operationId, operationId());
		assert.equal(calls.value, 2, "the loser may enter authorization but must not create a second runner");
		store.close();
	});

	it("rejects final-fence root substitutions before candidate side effects", async () => {
		for (const substitution of ["directory", "file"] as const) {
			const runId = candidate(`fence-${substitution}`);
			const store = new ManagedOperationJournal({ root: path.join(temporary, `journal-fence-${substitution}`) });
			const authorized = contractWithAttestedSessionRoot(runId);
			let calls = 0;
			const executor: ManagedSpawnExecutor = {
				executePreparedSpawn: (async (_id, _params, _signal, _update, _ctx, options) => {
					calls++;
					const preparedPlan = plan(authorized);
					await options.beforeLaunch(preparedPlan);
					const launchRoot = authorized.roots.attestations!.sessionRoot!.existingAncestor;
					fs.renameSync(launchRoot, `${launchRoot}.replaced`);
					if (substitution === "directory") fs.mkdirSync(launchRoot, { mode: 0o700 });
					else fs.writeFileSync(launchRoot, "replacement", "utf8");
					try {
						options.afterAuthorization(preparedPlan);
					} catch {
						return { content: [], isError: true, details: { mode: "single", results: [] } };
					}
					assert.fail("substituted launch root must fail the final coordinator fence");
				}) as ManagedSpawnExecutor["executePreparedSpawn"],
			};
			const coordinator = makeCoordinator(store, executor, async () => ({ ...resolved(runId), contract: authorized }));
			const result = await coordinator.dispatchSpawn(spawnRequest(runId));
			assert.equal(result.state, "failed-before-launch");
			assert.equal(calls, 1);
			assert.equal(store.read(parentDigest, "pi-signal", operationId())?.runId, undefined);
			assert.equal(fs.existsSync(authorized.roots.sessionRoot!), false);
			assert.equal(fs.existsSync(authorized.roots.asyncDir!), false);
			assert.equal(fs.existsSync(authorized.roots.resultPath!), false);
			assert.equal(fs.existsSync(authorized.roots.resultReservationPath!), false);
			assert.equal(fs.existsSync(authorized.roots.runnerConfigPath!), false);
			store.close();
		}
	});

	it("fails before launch when execution-time contract identity changes", async () => {
		const runId = candidate("changed");
		const store = new ManagedOperationJournal({ root: path.join(temporary, "journal") });
		let resolutions = 0;
		let callbacks = 0;
		const executor: ManagedSpawnExecutor = {
			executePreparedSpawn: (async (_id, _params, _signal, _update, _ctx, options) => {
				try {
					await options.beforeLaunch(plan(contract(runId)));
					callbacks++;
					return { content: [], details: { mode: "single", results: [] } };
				} catch {
					return { content: [], isError: true, details: { mode: "single", results: [] } };
				}
			}) as ManagedSpawnExecutor["executePreparedSpawn"],
		};
		const coordinator = makeCoordinator(store, executor, async () => resolved(runId, ++resolutions === 1 ? contractDigest : "f".repeat(64)));
		const result = await coordinator.dispatchSpawn(spawnRequest(runId));
		assert.equal(result.state, "failed-before-launch");
		assert.equal(callbacks, 0);
		assert.equal(store.read(parentDigest, "pi-signal", operationId())?.runId, undefined);
		store.close();
	});

	it("marks post-dispatch startup failures uncertain and never relaunches on retry", async () => {
		const runId = candidate("uncertain");
		const store = new ManagedOperationJournal({ root: path.join(temporary, "journal") });
		let calls = 0;
		const executor: ManagedSpawnExecutor = {
			executePreparedSpawn: (async (_id, _params, _signal, _update, _ctx, options) => {
				calls++;
				const preparedPlan = plan(contract(runId));
				await options.beforeLaunch(preparedPlan);
				options.afterAuthorization(preparedPlan);
				return { content: [], isError: true, details: { mode: "single", results: [] } };
			}) as ManagedSpawnExecutor["executePreparedSpawn"],
		};
		const coordinator = makeCoordinator(store, executor, async () => resolved(runId));
		assert.equal((await coordinator.dispatchSpawn(spawnRequest(runId))).state, "uncertain");
		assert.equal((await coordinator.dispatchSpawn(spawnRequest(runId, "transport-retry"))).state, "uncertain");
		assert.equal(calls, 1);
		store.close();
	});

	it("accepts only correlated committed evidence when recovering an accepted operation", async () => {
		for (const evidenceState of ["accepted", "committed"] as const) {
			const runId = candidate(evidenceState);
			const store = new ManagedOperationJournal({ root: path.join(temporary, `journal-${evidenceState}`) });
			const request = spawnRequest(runId);
			const digest = computeManagedRequestDigest(request);
			store.claim(parentDigest, request);
			store.transition(parentDigest, "pi-signal", operationId(), digest, "prepared");
			store.transition(parentDigest, "pi-signal", operationId(), digest, "dispatching", {
				runId,
				terminalAsyncDir: contract(runId).roots.asyncDir!,
				canonicalSessionFile: contract(runId).roots.sessionFile!,
			});
			const admission = createPreparedRunnerAdmission(runId, digest);
			store.transition(parentDigest, "pi-signal", operationId(), digest, "runner-ready", {
				runId,
				runnerProcessInstanceId: "runner-recovery-1",
				runnerAdmissionTokenDigest: computePreparedRunnerAdmissionTokenDigest(admission.token),
			});
			store.transition(parentDigest, "pi-signal", operationId(), digest, "accepted", { runId });
			const paths = preparedRunnerAdmissionPaths(path.join(ASYNC_DIR, runId));
			fs.mkdirSync(path.dirname(paths.evidencePath), { recursive: true });
			writePreparedRunnerAdmissionEvidence(paths.evidencePath, admission, evidenceState, 123, "runner-recovery-1");
			let calls = 0;
			const coordinator = makeCoordinator(store, {
				executePreparedSpawn: (async () => {
					calls++;
					return { content: [], details: { mode: "single", results: [] } };
				}) as ManagedSpawnExecutor["executePreparedSpawn"],
			}, async () => resolved(runId));
			const replay = await coordinator.dispatchSpawn({ ...request, requestId: `retry-${evidenceState}` });
			assert.equal(replay.state, evidenceState === "committed" ? "accepted" : "uncertain");
			assert.equal(replay.replayed, true);
			assert.equal(calls, 0);
			if (evidenceState === "accepted") {
				writePreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "committed", 123, "runner-recovery-1");
				const recovered = await coordinator.dispatchSpawn({ ...request, requestId: "retry-late-committed" });
				assert.equal(recovered.state, "accepted");
				assert.equal(recovered.replayed, true);
				assert.equal(calls, 0);
			} else {
				const sessionFile = contract(runId).roots.sessionFile!;
				fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
				fs.writeFileSync(sessionFile, "{}\n", "utf8");
				fs.writeFileSync(path.join(contract(runId).roots.asyncDir!, "process-terminal.json"), JSON.stringify({
					version: 1,
					state: "observed",
					runId,
					runnerProcessInstanceId: "runner-recovery-1",
					observedAt: 400,
					instances: [{ processInstanceId: "runner-recovery-1", kind: "runner", closeObservedAt: 400, exitCode: 0, signal: null }],
					canonicalSession: { canonicalSessionId: canonicalSessionId(sessionFile), leaseDisposition: "not-held", freeAtObservation: true },
					resumeDisposition: "resumable",
				}));
				const forged = await coordinator.dispatchSpawn({ ...request, requestId: "retry-forged-ordinary-proof" });
				assert.equal(forged.state, "uncertain");
				const forgedAgain = await coordinator.dispatchSpawn({ ...request, requestId: "retry-forged-ordinary-proof-again" });
				assert.equal(forgedAgain.state, "uncertain", "durable non-observed proof must remain sticky across retries");
				assert.equal(calls, 0);
			}
			store.close();
		}
	});
});

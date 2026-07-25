import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { computeManagedRequestDigest } from "../../src/api/managed-dispatch.ts";
import {
	ManagedOperationJournal,
	ManagedOperationJournalError,
} from "../../src/managed/operation-journal.ts";

let temporary = "";
const parentDigest = "e".repeat(64);

beforeEach(() => {
	temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-journal-"));
});

afterEach(() => {
	fs.rmSync(temporary, { recursive: true, force: true });
});

function operationId(byte = 7): string {
	return Buffer.alloc(32, byte).toString("base64url");
}

function spawnRequest(overrides: Record<string, unknown> = {}): Record<string, unknown> {
	return {
		version: 1,
		requestId: "transport-1",
		method: "spawn",
		managed: {
			version: 1,
			consumerId: "pi-signal",
			operationId: operationId(),
		},
		expectedLaunch: {
			version: 1,
			hostId: "host-1",
			candidateRunId: "candidate-1",
			profileIdentityDigest: "a".repeat(64),
			parentSessionIdentityDigest: parentDigest,
			contractDigest: "b".repeat(64),
		},
		input: {
			request: {
				agent: "worker",
				task: "inspect",
				context: "fresh",
				async: true,
				clarify: false,
				cwd: "/repo",
				sessionDir: "/sessions/operation-1",
			},
		},
		...overrides,
	};
}

function journal(root = path.join(temporary, "journal"), now = () => 100): ManagedOperationJournal {
	return new ManagedOperationJournal({ root, now });
}

function expectCode(action: () => unknown, code: string): void {
	assert.throws(action, (error: unknown) => error instanceof ManagedOperationJournalError && error.code === code);
}

describe("managed durable operation journal", () => {
	it("claims once and replays exact semantic identity across transport retries and reopen", () => {
		const root = path.join(temporary, "journal");
		const first = journal(root, () => 100);
		const created = first.claim(parentDigest, spawnRequest());
		assert.equal(created.created, true);
		assert.equal(created.replayed, false);
		assert.equal(created.record.state, "claimed");
		assert.equal(created.record.expectedLaunch?.candidateRunId, "candidate-1");
		const retry = spawnRequest({ requestId: "transport-retry" });
		const replay = first.claim(parentDigest, retry);
		assert.equal(replay.created, false);
		assert.equal(replay.replayed, true);
		assert.equal(replay.record.requestDigest, created.record.requestDigest);
		first.close();

		const reopened = journal(root, () => 200);
		const durableReplay = reopened.claim(parentDigest, retry);
		assert.equal(durableReplay.replayed, true);
		assert.equal(durableReplay.record.createdAt, 100);
		assert.equal(durableReplay.record.updatedAt, 100);
		reopened.close();
	});

	it("namespaces identities by parent session and consumer and rejects semantic conflicts", () => {
		const store = journal();
		const baseline = spawnRequest();
		store.claim(parentDigest, baseline);
		const changed = spawnRequest({
			input: { request: { ...(baseline.input as { request: Record<string, unknown> }).request, task: "different" } },
		});
		expectCode(() => store.claim(parentDigest, changed), "operation_conflict");
		expectCode(() => store.claim("f".repeat(64), baseline), "host_mismatch");

		const otherConsumer = spawnRequest({
			managed: { version: 1, consumerId: "other", operationId: operationId() },
		});
		const otherExpected = {
			...(otherConsumer.expectedLaunch as Record<string, unknown>),
			parentSessionIdentityDigest: "f".repeat(64),
		};
		const namespaced = { ...otherConsumer, expectedLaunch: otherExpected };
		assert.equal(store.claim("f".repeat(64), namespaced).created, true);
		store.close();
	});

	it("requires parent-session binding before creating a launch operation directory", () => {
		const store = journal();
		const missing = spawnRequest();
		delete (missing.expectedLaunch as Record<string, unknown>).parentSessionIdentityDigest;
		expectCode(() => store.claim(parentDigest, missing), "invalid_request");
		assert.equal(fs.existsSync(path.join(store.root, "operations")), false);
		store.close();
	});

	it("persists fenced state transitions and immutable candidate run identity", () => {
		let now = 10;
		const store = journal(path.join(temporary, "journal"), () => now++);
		const request = spawnRequest();
		const claimed = store.claim(parentDigest, request).record;
		const digest = computeManagedRequestDigest(request);
		const prepared = store.transition(parentDigest, "pi-signal", operationId(), digest, "prepared");
		assert.equal(prepared.state, "prepared");
		expectCode(
			() => store.transition(parentDigest, "pi-signal", operationId(), digest, "prepared", { sourceRunId: "source-1" }),
			"invalid_state",
		);
		expectCode(
			() => store.transition(parentDigest, "pi-signal", operationId(), digest, "failed-before-launch", { runId: "candidate-1" }),
			"invalid_state",
		);
		assert.equal(store.read(parentDigest, "pi-signal", operationId())?.state, "prepared");
		expectCode(
			() => store.transition(parentDigest, "pi-signal", operationId(), digest, "dispatching"),
			"invalid_state",
		);
		const dispatching = store.transition(parentDigest, "pi-signal", operationId(), digest, "dispatching", { runId: "candidate-1" });
		assert.equal(dispatching.runId, "candidate-1");
		assert.equal(store.transition(parentDigest, "pi-signal", operationId(), digest, "dispatching", { runId: "candidate-1" }).updatedAt, dispatching.updatedAt);
		expectCode(
			() => store.transition(parentDigest, "pi-signal", operationId(), digest, "uncertain", {
				runnerProcessInstanceId: "premature-runner",
				runnerAdmissionTokenDigest: "d".repeat(64),
			}),
			"invalid_state",
		);
		expectCode(
			() => store.transition(parentDigest, "pi-signal", operationId(), digest, "runner-ready", { runId: "other-run" }),
			"operation_conflict",
		);
		expectCode(
			() => store.transition(parentDigest, "pi-signal", operationId(), digest, "runner-ready", { runId: "candidate-1" }),
			"invalid_state",
		);
		const runnerReady = store.transition(parentDigest, "pi-signal", operationId(), digest, "runner-ready", {
			runId: "candidate-1",
			runnerProcessInstanceId: "runner-instance-1",
			runnerAdmissionTokenDigest: "d".repeat(64),
		});
		assert.equal(runnerReady.runnerProcessInstanceId, "runner-instance-1");
		assert.equal(runnerReady.runnerAdmissionTokenDigest, "d".repeat(64));
		expectCode(
			() => store.transition(parentDigest, "pi-signal", operationId(), digest, "accepted", {
				runnerProcessInstanceId: "runner-instance-2",
				runnerAdmissionTokenDigest: "d".repeat(64),
			}),
			"operation_conflict",
		);
		store.transition(parentDigest, "pi-signal", operationId(), digest, "accepted", { runId: "candidate-1" });
		store.transition(parentDigest, "pi-signal", operationId(), digest, "uncertain");
		store.transition(parentDigest, "pi-signal", operationId(), digest, "reconciling");
		const terminal = store.transition(parentDigest, "pi-signal", operationId(), digest, "terminal", { runId: "candidate-1" });
		assert.equal(terminal.state, "terminal");
		assert.equal(store.read(parentDigest, "pi-signal", operationId())?.createdAt, claimed.createdAt);
		expectCode(() => store.transition(parentDigest, "pi-signal", operationId(), digest, "prepared"), "invalid_state");
		store.close();
	});

	it("enforces one live owner and safely reclaims a provably dead owner", () => {
		const root = path.join(temporary, "journal");
		const first = journal(root);
		expectCode(() => journal(root), "busy");
		first.close();

		const ownerDir = path.join(root, ".owner");
		fs.mkdirSync(ownerDir, { recursive: true });
		fs.writeFileSync(path.join(ownerDir, "owner.json"), JSON.stringify({
			version: 1,
			token: "stale-owner",
			pid: 2_000_000_000,
			processStartFingerprint: "1",
			createdAt: 1,
		}), "utf8");
		const recovered = journal(root);
		const retainedTombstone = path.join(root, ".owner.stale.stale-owner");
		assert.equal(fs.existsSync(retainedTombstone), true);
		assert.throws(() => fs.renameSync(ownerDir, retainedTombstone), (error: unknown) =>
			["EEXIST", "ENOTEMPTY"].includes((error as NodeJS.ErrnoException).code ?? ""));
		recovered.close();

		fs.mkdirSync(ownerDir, { recursive: true });
		fs.writeFileSync(path.join(ownerDir, "owner.json"), "{}\n", "utf8");
		expectCode(() => journal(root), "busy");
	});

	it("fails visibly on incomplete and corrupt durable identity instead of inventing a replay", () => {
		const store = journal();
		const operationDirectory = path.join(store.root, "operations", parentDigest, "pi-signal", operationId());
		fs.mkdirSync(operationDirectory, { recursive: true, mode: 0o700 });
		expectCode(() => store.claim(parentDigest, spawnRequest()), "operation_uncertain");
		fs.writeFileSync(path.join(operationDirectory, "record.json"), "{}\n", { encoding: "utf8", mode: 0o600 });
		expectCode(() => store.claim(parentDigest, spawnRequest()), "corrupt");
		store.close();
	});

	it("rejects semantic record corruption and namespace substitution", () => {
		const store = journal();
		const request = spawnRequest();
		store.claim(parentDigest, request);
		const recordPath = path.join(store.root, "operations", parentDigest, "pi-signal", operationId(), "record.json");
		const record = JSON.parse(fs.readFileSync(recordPath, "utf8")) as Record<string, unknown>;
		fs.writeFileSync(recordPath, JSON.stringify({ ...record, state: "dispatching" }), { encoding: "utf8", mode: 0o600 });
		expectCode(() => store.read(parentDigest, "pi-signal", operationId()), "corrupt");
		fs.writeFileSync(recordPath, JSON.stringify({ ...record, consumerId: "other" }), { encoding: "utf8", mode: 0o600 });
		expectCode(() => store.read(parentDigest, "pi-signal", operationId()), "corrupt");
		store.close();
	});

	it("rejects permissive roots and symlinked namespace ancestors", () => {
		const permissiveRoot = path.join(temporary, "permissive");
		fs.mkdirSync(permissiveRoot, { mode: 0o755 });
		expectCode(() => journal(permissiveRoot), "invalid_state");

		const realParent = path.join(temporary, "real-parent");
		fs.mkdirSync(realParent, { mode: 0o700 });
		const linkedParent = path.join(temporary, "linked-parent");
		fs.symlinkSync(realParent, linkedParent);
		expectCode(() => journal(path.join(linkedParent, "journal")), "invalid_state");

		const store = journal(path.join(temporary, "private"));
		const operations = path.join(store.root, "operations");
		const session = path.join(operations, parentDigest);
		fs.mkdirSync(session, { recursive: true, mode: 0o700 });
		const outside = path.join(temporary, "outside");
		fs.mkdirSync(outside, { mode: 0o700 });
		fs.symlinkSync(outside, path.join(session, "pi-signal"));
		expectCode(() => store.claim(parentDigest, spawnRequest()), "invalid_state");
		store.close();
	});
});

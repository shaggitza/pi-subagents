import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
	computePreparedRunnerAdmissionTokenDigest,
	computePreparedRunnerSessionLeaseTokenDigest,
	createPreparedRunnerAdmission,
	preparedRunnerAdmissionPaths,
	readPreparedRunnerAdmissionControl,
	readPreparedRunnerAdmissionEvidence,
	readPreparedRunnerAdmissionEvidenceForDispatch,
	writePreparedRunnerAdmissionControl,
	writePreparedRunnerAdmissionEvidence,
} from "../../src/runs/background/prepared-runner-admission.ts";

let temporary = "";

beforeEach(() => {
	temporary = fs.mkdtempSync(path.join(os.tmpdir(), "prepared-runner-admission-"));
});

afterEach(() => {
	fs.rmSync(temporary, { recursive: true, force: true });
});

describe("prepared runner admission", () => {
	it("binds durable ready and accepted evidence to run and dispatch identity", () => {
		const admission = createPreparedRunnerAdmission("candidate-1", "a".repeat(64));
		const paths = preparedRunnerAdmissionPaths(temporary);
		writePreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "ready", 123, "runner-1", 100);
		const ready = readPreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "ready");
		assert.equal(ready?.state, "ready");
		assert.match(computePreparedRunnerAdmissionTokenDigest(admission.token), /^[a-f0-9]{64}$/);
		assert.equal(
			readPreparedRunnerAdmissionEvidenceForDispatch(paths.evidencePath, {
				runId: admission.runId,
				dispatchIdentityDigest: admission.dispatchIdentityDigest,
			})?.token,
			admission.token,
		);
		assert.equal(ready?.dispatchIdentityDigest, "a".repeat(64));
		assert.equal(fs.statSync(paths.evidencePath).mode & 0o077, 0);

		writePreparedRunnerAdmissionControl(paths.proceedPath, admission, "proceed");
		assert.equal(readPreparedRunnerAdmissionControl(paths.proceedPath, admission, "proceed")?.action, "proceed");
		writePreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "accepted", 123, "runner-1", 200);
		const accepted = readPreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "accepted");
		assert.equal(accepted?.state, "accepted");
		assert.equal(accepted?.runnerProcessInstanceId, ready?.runnerProcessInstanceId);
		writePreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "committed", 123, "runner-1", 300);
		const committed = readPreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "committed");
		assert.equal(committed?.state, "committed");
	});

	it("binds exact resume session lease identity through evidence and controls", () => {
		const resume = { version: 1 as const, sourceRunId: "source-1", sourceIndex: 0 as const, canonicalSessionId: "c".repeat(64) };
		const admission = createPreparedRunnerAdmission("candidate-resume", "d".repeat(64), resume);
		const leaseDigest = computePreparedRunnerSessionLeaseTokenDigest("private-lease-token");
		const paths = preparedRunnerAdmissionPaths(temporary);
		const ready = writePreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "ready", 123, "runner-resume", 100, leaseDigest);
		assert.deepEqual(ready.resume, resume);
		assert.equal(ready.sessionLeaseTokenDigest, leaseDigest);
		writePreparedRunnerAdmissionControl(paths.proceedPath, admission, "proceed", ready);
		assert.equal(readPreparedRunnerAdmissionControl(paths.proceedPath, admission, "proceed", leaseDigest)?.resume?.sessionLeaseTokenDigest, leaseDigest);
		assert.throws(() => readPreparedRunnerAdmissionControl(paths.proceedPath, admission, "proceed", "e".repeat(64)), /control is invalid/);
		assert.throws(() => writePreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "accepted", 123, "runner-resume"), /lease correlation/);
		const wrongSource = createPreparedRunnerAdmission("candidate-resume", "d".repeat(64), { ...resume, sourceRunId: "source-2" });
		assert.throws(() => readPreparedRunnerAdmissionEvidenceForDispatch(paths.evidencePath, wrongSource), /resume identity changed/);
	});

	it("fails closed for changed identity, state, controls, and unsafe descriptors", () => {
		assert.throws(() => createPreparedRunnerAdmission("bad/path", "a".repeat(64)), /safe run identity/);
		assert.throws(() => createPreparedRunnerAdmission("candidate-2", "bad"), /dispatch identity digest/);
		const admission = createPreparedRunnerAdmission("candidate-2", "b".repeat(64));
		const paths = preparedRunnerAdmissionPaths(temporary);
		writePreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "ready", 123, "runner-2");
		assert.equal(readPreparedRunnerAdmissionEvidence(paths.evidencePath, admission, "accepted"), undefined);
		const other = createPreparedRunnerAdmission("candidate-2", "b".repeat(64));
		assert.throws(() => readPreparedRunnerAdmissionEvidence(paths.evidencePath, other, "ready"), /identity changed/);

		writePreparedRunnerAdmissionControl(paths.commitPath, admission, "commit");
		assert.throws(() => readPreparedRunnerAdmissionControl(paths.commitPath, admission, "proceed"), /control is invalid/);
		fs.writeFileSync(paths.commitPath, "{}\n", "utf8");
		assert.throws(() => readPreparedRunnerAdmissionControl(paths.commitPath, admission, "commit"), /control is invalid/);
	});
});

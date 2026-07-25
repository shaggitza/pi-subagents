import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { writePrivateAtomicJson } from "../../shared/atomic-json.ts";

export const PREPARED_RUNNER_ADMISSION_VERSION = 1 as const;

export interface PreparedRunnerAdmissionV1 {
	version: typeof PREPARED_RUNNER_ADMISSION_VERSION;
	runId: string;
	dispatchIdentityDigest: string;
	token: string;
}

export interface PreparedRunnerAdmissionEvidenceV1 extends PreparedRunnerAdmissionV1 {
	state: "ready" | "accepted";
	pid: number;
	runnerProcessInstanceId: string;
	observedAt: number;
}

export interface PreparedRunnerAdmissionControlV1 {
	version: typeof PREPARED_RUNNER_ADMISSION_VERSION;
	action: "proceed" | "commit";
	token: string;
}

const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._~:-]{0,255}$/;
const DIGEST = /^[a-f0-9]{64}$/;
const TOKEN = /^[a-f0-9-]{36}$/;
const MAX_FILE_BYTES = 16_384;

function fsyncDirectory(directory: string): void {
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(directory, "r");
		fs.fsyncSync(descriptor);
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
	}
}

function writeDurablePrivateJson(filePath: string, value: object): void {
	const temporary = path.join(path.dirname(filePath), `.${path.basename(filePath)}.${process.pid}.${randomUUID()}.tmp`);
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(temporary, "wx", 0o600);
		fs.writeFileSync(descriptor, `${JSON.stringify(value)}\n`, "utf8");
		fs.fsyncSync(descriptor);
		fs.closeSync(descriptor);
		descriptor = undefined;
		fs.renameSync(temporary, filePath);
		fsyncDirectory(path.dirname(filePath));
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
		fs.rmSync(temporary, { force: true });
	}
}

export function createPreparedRunnerAdmission(runId: string, dispatchIdentityDigest: string): PreparedRunnerAdmissionV1 {
	if (!SAFE_ID.test(runId)) throw new Error("Prepared runner admission requires a safe run identity.");
	if (!DIGEST.test(dispatchIdentityDigest)) throw new Error("Prepared runner admission requires a dispatch identity digest.");
	return Object.freeze({
		version: PREPARED_RUNNER_ADMISSION_VERSION,
		runId,
		dispatchIdentityDigest,
		token: randomUUID(),
	});
}

export function preparedRunnerAdmissionPaths(asyncDir: string): {
	evidencePath: string;
	proceedPath: string;
	commitPath: string;
} {
	return {
		evidencePath: path.join(asyncDir, "runner-admission.json"),
		proceedPath: path.join(asyncDir, "runner-admission-proceed.json"),
		commitPath: path.join(asyncDir, "runner-admission-commit.json"),
	};
}

function boundedJson(filePath: string): unknown {
	const stats = fs.lstatSync(filePath);
	if (!stats.isFile() || stats.isSymbolicLink() || stats.size > MAX_FILE_BYTES) {
		throw new Error("Prepared runner admission file is not a bounded regular file.");
	}
	return JSON.parse(fs.readFileSync(filePath, "utf8")) as unknown;
}

function exactAdmissionBase(value: Record<string, unknown>, expected: PreparedRunnerAdmissionV1): boolean {
	return value.version === PREPARED_RUNNER_ADMISSION_VERSION
		&& value.runId === expected.runId
		&& value.dispatchIdentityDigest === expected.dispatchIdentityDigest
		&& value.token === expected.token;
}

export function readPreparedRunnerAdmissionEvidence(
	filePath: string,
	expected: PreparedRunnerAdmissionV1,
	expectedState: PreparedRunnerAdmissionEvidenceV1["state"],
): PreparedRunnerAdmissionEvidenceV1 | undefined {
	let value: unknown;
	try {
		value = boundedJson(filePath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Prepared runner admission evidence is invalid.");
	const record = value as Record<string, unknown>;
	const keys = ["dispatchIdentityDigest", "observedAt", "pid", "runId", "runnerProcessInstanceId", "state", "token", "version"];
	if (Object.keys(record).sort().join("\0") !== keys.sort().join("\0") || !exactAdmissionBase(record, expected)) {
		throw new Error("Prepared runner admission evidence identity changed.");
	}
	if (
		(record.state !== "ready" && record.state !== "accepted")
		|| typeof record.pid !== "number"
		|| !Number.isSafeInteger(record.pid)
		|| record.pid <= 0
		|| typeof record.runnerProcessInstanceId !== "string"
		|| !SAFE_ID.test(record.runnerProcessInstanceId)
		|| typeof record.observedAt !== "number"
		|| !Number.isSafeInteger(record.observedAt)
		|| record.observedAt < 0
	) {
		throw new Error("Prepared runner admission evidence is invalid.");
	}
	if (record.state !== expectedState) return undefined;
	return Object.freeze(record as unknown as PreparedRunnerAdmissionEvidenceV1);
}

export function writePreparedRunnerAdmissionEvidence(
	filePath: string,
	admission: PreparedRunnerAdmissionV1,
	state: PreparedRunnerAdmissionEvidenceV1["state"],
	pid: number,
	runnerProcessInstanceId: string,
	now = Date.now(),
): PreparedRunnerAdmissionEvidenceV1 {
	const evidence: PreparedRunnerAdmissionEvidenceV1 = {
		...admission,
		state,
		pid,
		runnerProcessInstanceId,
		observedAt: now,
	};
	writeDurablePrivateJson(filePath, evidence);
	return evidence;
}

export function readPreparedRunnerAdmissionControl(
	filePath: string,
	expected: PreparedRunnerAdmissionV1,
	action: PreparedRunnerAdmissionControlV1["action"],
): PreparedRunnerAdmissionControlV1 | undefined {
	let value: unknown;
	try {
		value = boundedJson(filePath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Prepared runner admission control is invalid.");
	const record = value as Record<string, unknown>;
	if (
		Object.keys(record).sort().join("\0") !== ["action", "token", "version"].sort().join("\0")
		|| record.version !== PREPARED_RUNNER_ADMISSION_VERSION
		|| record.action !== action
		|| record.token !== expected.token
	) {
		throw new Error("Prepared runner admission control is invalid.");
	}
	return Object.freeze({ version: PREPARED_RUNNER_ADMISSION_VERSION, action, token: expected.token });
}

export function writePreparedRunnerAdmissionControl(
	filePath: string,
	admission: PreparedRunnerAdmissionV1,
	action: PreparedRunnerAdmissionControlV1["action"],
): void {
	if (!TOKEN.test(admission.token)) throw new Error("Prepared runner admission token is invalid.");
	writePrivateAtomicJson(filePath, {
		version: PREPARED_RUNNER_ADMISSION_VERSION,
		action,
		token: admission.token,
	} satisfies PreparedRunnerAdmissionControlV1);
}

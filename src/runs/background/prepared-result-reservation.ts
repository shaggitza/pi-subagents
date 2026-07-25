import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

export const PREPARED_RESULT_RESERVATION_VERSION = 1 as const;

export interface PreparedResultReservationV1 {
	version: typeof PREPARED_RESULT_RESERVATION_VERSION;
	runId: string;
	resultPath: string;
	path: string;
	token: string;
}

const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._~:-]{0,255}$/;
const MAX_RESERVATION_BYTES = 16_384;

export function preparedResultReservationPath(resultPath: string): string {
	return `${path.resolve(resultPath)}.prepared-reservation`;
}

function fsyncDirectory(directory: string): void {
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(directory, "r");
		fs.fsyncSync(descriptor);
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
	}
}

function exactReservation(value: unknown, expected: PreparedResultReservationV1): boolean {
	if (!value || typeof value !== "object" || Array.isArray(value)) return false;
	const record = value as Record<string, unknown>;
	if (Object.keys(record).sort().join("\0") !== ["path", "resultPath", "runId", "token", "version"].sort().join("\0")) return false;
	return record.version === PREPARED_RESULT_RESERVATION_VERSION
		&& record.runId === expected.runId
		&& record.resultPath === expected.resultPath
		&& record.path === expected.path
		&& record.token === expected.token;
}

function assertReservationIdentity(reservation: PreparedResultReservationV1): void {
	const stats = fs.lstatSync(reservation.path);
	if (!stats.isFile() || stats.isSymbolicLink() || stats.size > MAX_RESERVATION_BYTES) {
		throw new Error("Prepared result reservation is not a bounded regular file.");
	}
	const parsed = JSON.parse(fs.readFileSync(reservation.path, "utf8")) as unknown;
	if (!exactReservation(parsed, reservation)) throw new Error("Prepared result reservation identity changed.");
}

export function assertPreparedResultReservation(reservation: PreparedResultReservationV1): void {
	assertReservationIdentity(reservation);
	try {
		fs.lstatSync(reservation.resultPath);
		throw new Error("Prepared result path already exists.");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}

export function createPreparedResultReservation(runId: string, resultPathInput: string): PreparedResultReservationV1 {
	if (!SAFE_ID.test(runId)) throw new Error("Prepared result reservation requires a safe run identity.");
	const resultPath = path.resolve(resultPathInput);
	const reservation: PreparedResultReservationV1 = {
		version: PREPARED_RESULT_RESERVATION_VERSION,
		runId,
		resultPath,
		path: preparedResultReservationPath(resultPath),
		token: randomUUID(),
	};
	let descriptor: number | undefined;
	try {
		try {
			fs.lstatSync(resultPath);
			throw new Error("Prepared result path already exists.");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		descriptor = fs.openSync(reservation.path, "wx", 0o600);
		fs.writeFileSync(descriptor, `${JSON.stringify(reservation)}\n`, "utf8");
		fs.fsyncSync(descriptor);
		fs.closeSync(descriptor);
		descriptor = undefined;
		fsyncDirectory(path.dirname(reservation.path));
		assertPreparedResultReservation(reservation);
		return Object.freeze(reservation);
	} catch (error) {
		if (descriptor !== undefined) fs.closeSync(descriptor);
		throw error;
	}
}

export function releasePreparedResultReservation(reservation: PreparedResultReservationV1): void {
	assertReservationIdentity(reservation);
	fs.unlinkSync(reservation.path);
	fsyncDirectory(path.dirname(reservation.path));
}

import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
	assertPreparedResultReservation,
	createPreparedResultReservation,
	preparedResultReservationPath,
	releasePreparedResultReservation,
} from "../../src/runs/background/prepared-result-reservation.ts";

let temporary = "";

beforeEach(() => {
	temporary = fs.mkdtempSync(path.join(os.tmpdir(), "prepared-result-reservation-"));
});

afterEach(() => {
	fs.rmSync(temporary, { recursive: true, force: true });
});

describe("prepared result reservation", () => {
	it("claims one candidate result atomically and releases only after publication", () => {
		const resultPath = path.join(temporary, "candidate.json");
		const reservation = createPreparedResultReservation("candidate-1", resultPath);
		assert.equal(reservation.path, preparedResultReservationPath(resultPath));
		assert.equal(fs.statSync(reservation.path).mode & 0o077, 0);
		assert.doesNotThrow(() => assertPreparedResultReservation(reservation));
		assert.throws(
			() => createPreparedResultReservation("candidate-1", resultPath),
			/EEXIST|exist/i,
		);

		fs.writeFileSync(resultPath, "published", "utf8");
		assert.throws(() => assertPreparedResultReservation(reservation), /result path already exists/);
		releasePreparedResultReservation(reservation);
		assert.equal(fs.existsSync(reservation.path), false);
		assert.equal(fs.readFileSync(resultPath, "utf8"), "published");
	});

	it("fails closed when reservation identity changes or becomes a symlink", () => {
		const resultPath = path.join(temporary, "candidate.json");
		const reservation = createPreparedResultReservation("candidate-2", resultPath);
		fs.writeFileSync(reservation.path, "{}\n", "utf8");
		assert.throws(() => assertPreparedResultReservation(reservation), /identity changed/);
		assert.throws(() => releasePreparedResultReservation(reservation), /identity changed/);
		assert.equal(fs.existsSync(reservation.path), true);

		fs.rmSync(reservation.path, { force: true });
		fs.symlinkSync(path.join(temporary, "missing"), reservation.path);
		assert.throws(() => assertPreparedResultReservation(reservation), /bounded regular file/);
	});
});

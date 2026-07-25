import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";

function run(command: string, args: string[], cwd: string) {
	const result = spawnSync(command, args, { cwd, encoding: "utf8" });
	assert.equal(result.status, 0, `${command} ${args.join(" ")} failed:\n${result.stdout}\n${result.stderr}`);
	return result;
}

test("plain Node imports managed-dispatch from a packed dependency install", () => {
	const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "managed-dispatch-packed-"));
	try {
		const pack = run(npmCommand, ["pack", "--json", "--pack-destination", temporary], projectRoot);
		const [{ filename }] = JSON.parse(pack.stdout) as Array<{ filename: string }>;
		const tarball = path.join(temporary, filename);
		const consumer = path.join(temporary, "consumer");
		fs.mkdirSync(consumer);
		fs.writeFileSync(path.join(consumer, "package.json"), '{"name":"managed-dispatch-probe","private":true,"type":"module"}\n');
		run(npmCommand, [
			"install",
			"--ignore-scripts",
			"--no-package-lock",
			"--no-save",
			"--omit=optional",
			"--omit=peer",
			"--audit=false",
			"--fund=false",
			tarball,
		], consumer);

		const probe = run(process.execPath, [
			"--input-type=module",
			"--eval",
			[
				'import * as api from "pi-subagents/managed-dispatch";',
				'if (api.SUBAGENT_MANAGED_DISPATCH_VERSION !== 1) throw new Error("wrong version");',
				'if (typeof api.computeManagedRequestDigest !== "function") throw new Error("missing runtime export");',
				'console.log(import.meta.resolve("pi-subagents/managed-dispatch"));',
			].join("\n"),
		], consumer);
		assert.match(probe.stdout.trim(), /node_modules[\\/]pi-subagents[\\/]src[\\/]api[\\/]managed-dispatch\.js$/);
	} finally {
		fs.rmSync(temporary, { recursive: true, force: true });
	}
});

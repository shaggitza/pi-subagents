import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import { computeManagedRequestDigest } from "../../src/api/managed-dispatch.ts";
import {
	closeSteerInbox,
	consumeManagedControlRequests,
	consumeInterruptRequest,
	consumeSteerAcks,
	consumeSteerCapabilities,
	consumeSteerRequests,
	consumeStopRequest,
	deliverInterruptRequest,
	enqueueManagedStepSteer,
	enqueueStepSteer,
	interruptRequestPath,
	inspectManagedControlTransport,
	managedControlAckPath,
	managedControlConsumedPath,
	managedControlRequestPath,
	publishManagedControlRequest,
	readManagedControlAck,
	requestAsyncInterrupt,
	requestAsyncSteer,
	requestAsyncStop,
	steerAckPathFromDir,
	steerAcksDir,
	steerInboxClosedPath,
	steerCapabilityPath,
	writeManagedControlAck,
	writeSteerAck,
	writeSteerCapability,
	stopRequestPath,
	steerRequestsDir,
	stepSteerInboxDir,
	watchAsyncControlInbox,
} from "../../src/runs/background/control-channel.ts";

function tmpAsyncDir(label: string): string {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), label));
	return path.join(root, "run");
}

function cleanup(asyncDir: string): void {
	fs.rmSync(path.dirname(asyncDir), { recursive: true, force: true });
}

function managedAuthority(commandId: string, method: "steer" | "interrupt" | "stop", message?: string, byte = 9) {
	const actorOperationId = Buffer.alloc(32, byte).toString("base64url");
	const semantic = {
		version: 1,
		requestId: "transport-proof",
		method,
		managed: { version: 1, consumerId: "consumer-a", operationId: commandId },
		input: { target: { consumerId: "consumer-a", operationId: actorOperationId }, ...(method === "steer" ? { message } : {}) },
	};
	return {
		commandRequestDigest: computeManagedRequestDigest(semantic),
		consumerId: "consumer-a",
		targetOperationId: actorOperationId,
		actorOperationId,
		actorRequestDigest: "b".repeat(64),
		runnerProcessInstanceId: "runner-instance-1",
	};
}

describe("control channel: request file", () => {
	it("writes a parseable interrupt request, creating the inbox dir", () => {
		const asyncDir = tmpAsyncDir("pi-control-write-");
		try {
			const requestPath = requestAsyncInterrupt(asyncDir, { source: "test" }, { now: () => 999 });
			assert.equal(requestPath, interruptRequestPath(asyncDir));
			const data = JSON.parse(fs.readFileSync(requestPath, "utf-8"));
			assert.equal(data.type, "interrupt");
			assert.equal(data.ts, 999);
			assert.equal(data.source, "test");
		} finally {
			cleanup(asyncDir);
		}
	});

	it("writes and consumes a stop request", () => {
		const asyncDir = tmpAsyncDir("pi-control-stop-");
		try {
			const requestPath = requestAsyncStop(asyncDir, { source: "test" }, { now: () => 1234 });
			assert.equal(requestPath, stopRequestPath(asyncDir));
			const data = JSON.parse(fs.readFileSync(requestPath, "utf-8"));
			assert.equal(data.type, "stop");
			assert.equal(data.ts, 1234);
			assert.equal(data.source, "test");
			assert.equal(consumeStopRequest(asyncDir), true);
			assert.equal(fs.existsSync(stopRequestPath(asyncDir)), false);
			assert.equal(consumeStopRequest(asyncDir), false);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("keeps the request type authoritative even for untyped callers", () => {
		const asyncDir = tmpAsyncDir("pi-control-write-type-");
		try {
			const requestPath = requestAsyncInterrupt(asyncDir, { type: "not-interrupt", source: "test" } as any, { now: () => 999 });
			const data = JSON.parse(fs.readFileSync(requestPath, "utf-8"));
			assert.equal(data.type, "interrupt");
			assert.equal(data.source, "test");
		} finally {
			cleanup(asyncDir);
		}
	});

	it("consumes a pending request exactly once and removes the file", () => {
		const asyncDir = tmpAsyncDir("pi-control-consume-");
		try {
			requestAsyncInterrupt(asyncDir);
			assert.equal(consumeInterruptRequest(asyncDir), true);
			assert.equal(fs.existsSync(interruptRequestPath(asyncDir)), false);
			assert.equal(consumeInterruptRequest(asyncDir), false);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("removes a malformed request directory instead of firing forever", () => {
		const asyncDir = tmpAsyncDir("pi-control-consume-dir-");
		try {
			fs.mkdirSync(interruptRequestPath(asyncDir), { recursive: true });
			assert.equal(consumeInterruptRequest(asyncDir), true);
			assert.equal(fs.existsSync(interruptRequestPath(asyncDir)), false);
			assert.equal(consumeInterruptRequest(asyncDir), false);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("writes and consumes ordered steer requests", () => {
		const asyncDir = tmpAsyncDir("pi-control-steer-");
		try {
			requestAsyncSteer(asyncDir, { message: "  later guidance  ", targetIndex: 1, id: "b", ts: 200, source: "test" });
			requestAsyncSteer(asyncDir, { message: "first guidance", targetIndexes: [0, 2], id: "a", ts: 100 });
			assert.equal(fs.readdirSync(steerRequestsDir(asyncDir)).length, 2);

			assert.deepEqual(consumeSteerRequests(asyncDir), [
				{ type: "steer", id: "a", ts: 100, message: "first guidance", targetIndexes: [0, 2] },
				{ type: "steer", id: "b", ts: 200, message: "later guidance", targetIndex: 1, source: "test" },
			]);
			assert.deepEqual(consumeSteerRequests(asyncDir), []);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("rejects requests after the runner closes the steering inbox", () => {
		const asyncDir = tmpAsyncDir("pi-control-steer-closed-");
		try {
			closeSteerInbox(asyncDir, "complete");
			assert.equal(fs.existsSync(steerInboxClosedPath(asyncDir)), true);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "too late", targetIndexes: [0] }), /no longer accepts steering/);
			assert.equal(fs.existsSync(steerRequestsDir(asyncDir)), false);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("keeps steer request ids out of filesystem paths", () => {
		const asyncDir = tmpAsyncDir("pi-control-steer-safe-name-");
		try {
			const requestPath = requestAsyncSteer(asyncDir, { message: "safe", id: "../outside\\bad:thing", ts: 1 });
			assert.equal(path.dirname(requestPath), steerRequestsDir(asyncDir));
			assert.equal(path.basename(requestPath), `0000000000001-${Buffer.from("../outside\\bad:thing").toString("base64url")}.json`);
			assert.deepEqual(consumeSteerRequests(asyncDir), [
				{ type: "steer", id: "../outside\\bad:thing", ts: 1, message: "safe" },
			]);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("does not deliver a steer request if another consumer removed it first", () => {
		const asyncDir = tmpAsyncDir("pi-control-steer-concurrent-");
		try {
			requestAsyncSteer(asyncDir, { message: "already taken", id: "s", ts: 1 });
			const fsImpl = {
				existsSync: fs.existsSync,
				readdirSync: fs.readdirSync,
				readFileSync: fs.readFileSync,
				rmSync: (target: fs.PathLike, options?: fs.RmOptions) => {
					fs.rmSync(target, options);
					const error = new Error("already removed") as NodeJS.ErrnoException;
					error.code = "ENOENT";
					throw error;
				},
			};
			assert.deepEqual(consumeSteerRequests(asyncDir, fsImpl), []);
			assert.deepEqual(consumeSteerRequests(asyncDir), []);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("enqueues a steer request for a specific child inbox", () => {
		const asyncDir = tmpAsyncDir("pi-control-step-steer-");
		try {
			enqueueStepSteer(asyncDir, 2, { type: "steer", id: "s1", ts: 300, message: "focus", targetIndexes: [0, 1] });
			const request = JSON.parse(fs.readFileSync(path.join(stepSteerInboxDir(asyncDir, 2), fs.readdirSync(stepSteerInboxDir(asyncDir, 2))[0]!), "utf-8"));
			assert.equal(request.targetIndex, 2);
			assert.equal(request.targetIndexes, undefined);
			assert.equal(request.message, "focus");
		} finally {
			cleanup(asyncDir);
		}
	});

	it("rejects empty steer messages and invalid target indexes", () => {
		const asyncDir = tmpAsyncDir("pi-control-steer-invalid-");
		try {
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "   " }), /steer message must not be empty/);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "ok", targetIndex: -1 }), /targetIndex/);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "ok", targetIndex: 1_000_001 }), /targetIndex/);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "ok", targetIndexes: [] }), /targetIndexes/);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "ok", targetIndexes: [0, 0] }), /targetIndexes/);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "ok", targetIndex: 0, targetIndexes: [0] }), /targetIndexes/);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "ok", targetIndexes: "bad" as unknown as number[] }), /targetIndexes/);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "x".repeat(128 * 1024 + 1) }), /exceeds/);
			assert.throws(() => requestAsyncSteer(asyncDir, { message: "ok", id: "contains whitespace" }), /malformed/);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("publishes exclusive managed sidecars and consumes a command once with a durable ack", () => {
		const asyncDir = tmpAsyncDir("pi-control-managed-");
		const commandId = Buffer.alloc(32, 4).toString("base64url");
		try {
			const requestPath = publishManagedControlRequest(asyncDir, {
				commandId,
				...managedAuthority(commandId, "steer", "private steering text"),
				method: "steer",
				runId: "managed-run",
				message: "private steering text",
				requestedAt: 100,
			});
			assert.equal(requestPath, managedControlRequestPath(asyncDir, commandId));
			assert.equal(inspectManagedControlTransport(asyncDir, commandId), "published");
			assert.throws(() => publishManagedControlRequest(asyncDir, {
				commandId, ...managedAuthority(commandId, "steer", "different"), method: "steer", runId: "managed-run", message: "different", requestedAt: 101,
			}), (error: unknown) => (error as NodeJS.ErrnoException).code === "EEXIST");
			const delivered: string[] = [];
			consumeManagedControlRequests(asyncDir, (request) => {
				delivered.push(request.message!);
				return { outcome: "acknowledged" };
			});
			consumeManagedControlRequests(asyncDir, () => {
				throw new Error("must not execute twice");
			});
			assert.deepEqual(delivered, ["private steering text"]);
			assert.equal(fs.existsSync(managedControlRequestPath(asyncDir, commandId)), false);
			assert.equal(fs.existsSync(managedControlConsumedPath(asyncDir, commandId)), true);
			assert.equal(inspectManagedControlTransport(asyncDir, commandId), "acknowledged");
			assert.equal(readManagedControlAck(asyncDir, commandId)?.runId, "managed-run");
			assert.throws(() => publishManagedControlRequest(asyncDir, {
				commandId, ...managedAuthority(commandId, "steer", "private steering text"), method: "steer", runId: "managed-run", message: "private steering text", requestedAt: 102,
			}), (error: unknown) => (error as NodeJS.ErrnoException).code === "EEXIST", "consumed/ack lifecycle must prohibit republish");
			consumeManagedControlRequests(asyncDir, () => {
				throw new Error("republished lifecycle must never execute");
			});
			assert.deepEqual(delivered, ["private steering text"]);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("uses a cross-platform no-clobber claim when a consumed tombstone already exists", () => {
		const asyncDir = tmpAsyncDir("pi-control-managed-claim-");
		const commandId = Buffer.alloc(32, 6).toString("base64url");
		try {
			publishManagedControlRequest(asyncDir, { commandId, ...managedAuthority(commandId, "stop"), method: "stop", runId: "managed-run", requestedAt: 100 });
			fs.mkdirSync(path.dirname(managedControlConsumedPath(asyncDir, commandId)), { recursive: true, mode: 0o700 });
			fs.copyFileSync(managedControlRequestPath(asyncDir, commandId), managedControlConsumedPath(asyncDir, commandId), fs.constants.COPYFILE_EXCL);
			let executions = 0;
			consumeManagedControlRequests(asyncDir, () => { executions++; return { outcome: "acknowledged" }; });
			assert.equal(executions, 0);
			assert.equal(fs.existsSync(managedControlRequestPath(asyncDir, commandId)), true);
			assert.equal(inspectManagedControlTransport(asyncDir, commandId), "corrupt");
		} finally {
			cleanup(asyncDir);
		}
	});

	it("keeps managed request and child steer handoff private end-to-end", () => {
		const asyncDir = tmpAsyncDir("pi-control-managed-private-");
		const commandId = Buffer.alloc(32, 7).toString("base64url");
		try {
			const requestPath = publishManagedControlRequest(asyncDir, { commandId, ...managedAuthority(commandId, "steer", "private handoff"), method: "steer", runId: "managed-run", message: "private handoff", requestedAt: 100 });
			const handoffPath = enqueueManagedStepSteer(asyncDir, 0, { type: "steer", id: commandId, ts: 100, message: "private handoff", targetIndex: 0, source: "managed-v1" });
			if (process.platform !== "win32") {
				assert.equal(fs.statSync(path.join(asyncDir, "control")).mode & 0o777, 0o700);
				assert.equal(fs.statSync(path.dirname(requestPath)).mode & 0o777, 0o700);
				assert.equal(fs.statSync(path.dirname(handoffPath)).mode & 0o777, 0o700);
				assert.equal(fs.statSync(requestPath).mode & 0o777, 0o600);
				assert.equal(fs.statSync(handoffPath).mode & 0o777, 0o600);
			}
			assert.equal(fs.readFileSync(handoffPath, "utf8").includes("private handoff"), true);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("requires consumed proof and rejects ack precedence while a request is pending", () => {
		const asyncDir = tmpAsyncDir("pi-control-managed-ack-precedence-");
		const commandId = Buffer.alloc(32, 8).toString("base64url");
		try {
			const binding = managedAuthority(commandId, "interrupt");
			publishManagedControlRequest(asyncDir, { commandId, ...binding, method: "interrupt", runId: "managed-run", requestedAt: 100 });
			writeManagedControlAck(asyncDir, { version: 1, commandId, ...binding, method: "interrupt", runId: "managed-run", acknowledgedAt: 101, outcome: "acknowledged" });
			assert.equal(inspectManagedControlTransport(asyncDir, commandId), "corrupt");
			assert.equal(fs.existsSync(managedControlRequestPath(asyncDir, commandId)), true);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("fails closed after consume-before-ack and keeps acknowledgments immutable", () => {
		const asyncDir = tmpAsyncDir("pi-control-managed-gap-");
		const commandId = Buffer.alloc(32, 5).toString("base64url");
		try {
			publishManagedControlRequest(asyncDir, { commandId, ...managedAuthority(commandId, "interrupt"), method: "interrupt", runId: "managed-run", requestedAt: 100 });
			fs.mkdirSync(path.dirname(managedControlConsumedPath(asyncDir, commandId)), { recursive: true });
			fs.renameSync(managedControlRequestPath(asyncDir, commandId), managedControlConsumedPath(asyncDir, commandId));
			assert.equal(inspectManagedControlTransport(asyncDir, commandId), "consumed");
			const ack = { version: 1 as const, commandId, ...managedAuthority(commandId, "interrupt"), method: "interrupt" as const, runId: "managed-run", acknowledgedAt: 200, outcome: "failed" as const, reason: "not active" };
			writeManagedControlAck(asyncDir, ack);
			assert.equal(managedControlAckPath(asyncDir, commandId), path.join(asyncDir, "control", "managed-acks", `${commandId}.json`));
			assert.equal(inspectManagedControlTransport(asyncDir, commandId), "failed");
			assert.throws(() => writeManagedControlAck(asyncDir, ack), (error: unknown) => (error as NodeJS.ErrnoException).code === "EEXIST");
		} finally {
			cleanup(asyncDir);
		}
	});

	it("writes strict capabilities and acknowledgments with safe paths", () => {
		const asyncDir = tmpAsyncDir("pi-control-steer-ack-");
		try {
			const capabilityPath = writeSteerCapability(asyncDir, { index: 0, pid: 42, readyAt: 100, supported: true });
			assert.equal(capabilityPath, steerCapabilityPath(asyncDir, 0));
			writeSteerAck(asyncDir, { requestId: "../request", index: 0, ts: 101, state: "delivered", message: "accepted" });
			assert.equal(path.dirname(steerAckPathFromDir(steerAcksDir(asyncDir, 0), "../request")), steerAcksDir(asyncDir, 0));
			assert.deepEqual(consumeSteerCapabilities(asyncDir), [{ type: "steer-capability", protocolVersion: 1, index: 0, pid: 42, readyAt: 100, supported: true }]);
			assert.deepEqual(consumeSteerAcks(asyncDir), [{ type: "steer-ack", protocolVersion: 1, requestId: "../request", index: 0, ts: 101, state: "delivered", message: "accepted" }]);
			assert.deepEqual(consumeSteerAcks(asyncDir), []);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("ignores malformed capabilities and acknowledgments", () => {
		const asyncDir = tmpAsyncDir("pi-control-steer-malformed-");
		try {
			fs.mkdirSync(path.join(asyncDir, "control", "steer-capabilities"), { recursive: true });
			fs.writeFileSync(steerCapabilityPath(asyncDir, 0), JSON.stringify({ type: "steer-capability", protocolVersion: 99 }), "utf-8");
			fs.mkdirSync(steerAcksDir(asyncDir, 0), { recursive: true });
			fs.writeFileSync(path.join(steerAcksDir(asyncDir, 0), "bad.json"), JSON.stringify({ type: "steer-ack", protocolVersion: 1, requestId: "", index: 0, ts: 1, state: "delivered", message: "bad" }), "utf-8");
			fs.writeFileSync(path.join(asyncDir, "control", "steer-acks", "1"), "not a directory", "utf-8");
			writeSteerAck(asyncDir, { requestId: "valid", index: 2, ts: 2, state: "delivered", message: "accepted" });
			assert.deepEqual(consumeSteerCapabilities(asyncDir), []);
			assert.deepEqual(consumeSteerAcks(asyncDir), [{ type: "steer-ack", protocolVersion: 1, requestId: "valid", index: 2, ts: 2, state: "delivered", message: "accepted" }]);
		} finally {
			cleanup(asyncDir);
		}
	});
});

describe("control channel: deliverInterruptRequest", () => {
	it("writes the portable request and signals best-effort when kill succeeds", () => {
		const asyncDir = tmpAsyncDir("pi-control-deliver-ok-");
		try {
			const kills: Array<{ pid: number; signal?: NodeJS.Signals | 0 }> = [];
			deliverInterruptRequest({
				asyncDir,
				pid: 4242,
				signal: "SIGUSR2",
				kill: (pid, signal) => {
					kills.push({ pid, signal });
					return true;
				},
			});
			assert.equal(fs.existsSync(interruptRequestPath(asyncDir)), true);
			assert.deepEqual(kills, [{ pid: 4242, signal: "SIGUSR2" }]);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("still writes the request when the OS signal throws ENOSYS (Windows)", () => {
		const asyncDir = tmpAsyncDir("pi-control-deliver-enosys-");
		try {
			assert.doesNotThrow(() =>
				deliverInterruptRequest({
					asyncDir,
					pid: 4242,
					kill: () => {
						const error = new Error("kill ENOSYS") as NodeJS.ErrnoException;
						error.code = "ENOSYS";
						throw error;
					},
				}),
			);
			assert.equal(fs.existsSync(interruptRequestPath(asyncDir)), true);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("surfaces non-portability signal failures and removes the stale request", () => {
		const asyncDir = tmpAsyncDir("pi-control-deliver-esrch-");
		try {
			assert.throws(
				() =>
					deliverInterruptRequest({
						asyncDir,
						pid: 4242,
						kill: () => {
							const error = new Error("missing process") as NodeJS.ErrnoException;
							error.code = "ESRCH";
							throw error;
						},
					}),
				/missing process/,
			);
			assert.equal(fs.existsSync(interruptRequestPath(asyncDir)), false);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("skips signalling when no live pid is provided", () => {
		const asyncDir = tmpAsyncDir("pi-control-deliver-nopid-");
		try {
			let killed = false;
			deliverInterruptRequest({ asyncDir, kill: () => { killed = true; return true; } });
			assert.equal(killed, false);
			assert.equal(fs.existsSync(interruptRequestPath(asyncDir)), true);
		} finally {
			cleanup(asyncDir);
		}
	});
});

describe("control channel: watchAsyncControlInbox", () => {
	type WatchHarness = {
		fsImpl: import("../../src/runs/background/control-channel.ts").ControlChannelFs;
		timers: import("../../src/runs/background/control-channel.ts").ControlChannelTimers;
		trigger: () => void;
		closed: () => boolean;
		watchedDir: () => string | undefined;
	};

	function harness(nativeDir?: string): WatchHarness {
		let listener: (() => void) | undefined;
		let closed = false;
		let watchedDir: string | undefined;
		const realpathSync = ((target: fs.PathLike, options?: unknown) => fs.realpathSync(target, options as BufferEncoding)) as typeof fs.realpathSync;
		realpathSync.native = ((target: fs.PathLike) => nativeDir ?? fs.realpathSync.native(target)) as typeof fs.realpathSync.native;
		const fsImpl = {
			mkdirSync: fs.mkdirSync,
			existsSync: fs.existsSync,
			rmSync: fs.rmSync,
			readdirSync: fs.readdirSync,
			readFileSync: fs.readFileSync,
			realpathSync,
			watch: ((dir: string, cb: () => void) => {
				watchedDir = dir;
				listener = cb;
				return { close: () => { closed = true; }, on: () => {} };
			}),
		} as unknown as WatchHarness["fsImpl"];
		const timers = {
			setInterval: (() => ({ unref() {} })) as unknown as typeof setInterval,
			clearInterval: (() => {}) as unknown as typeof clearInterval,
		};
		return { fsImpl, timers, trigger: () => listener?.(), closed: () => closed, watchedDir: () => watchedDir };
	}

	it("registers the native canonical control inbox path", () => {
		const asyncDir = tmpAsyncDir("pi-control-watch-native-");
		try {
			const nativeDir = path.join(path.dirname(asyncDir), "native-control-path");
			const h = harness(nativeDir);
			const dispose = watchAsyncControlInbox(asyncDir, { onInterrupt() {}, fs: h.fsImpl, timers: h.timers });
			assert.equal(h.watchedDir(), nativeDir);
			dispose();
		} finally {
			cleanup(asyncDir);
		}
	});

	it("fires on a request that arrived before the watcher started", () => {
		const asyncDir = tmpAsyncDir("pi-control-watch-early-");
		try {
			requestAsyncInterrupt(asyncDir);
			let fired = 0;
			const h = harness();
			const dispose = watchAsyncControlInbox(asyncDir, { onInterrupt: () => fired++, fs: h.fsImpl, timers: h.timers });
			assert.equal(fired, 1);
			assert.equal(fs.existsSync(interruptRequestPath(asyncDir)), false);
			dispose();
		} finally {
			cleanup(asyncDir);
		}
	});

	it("fires once per request via the watch event and stops after dispose", () => {
		const asyncDir = tmpAsyncDir("pi-control-watch-event-");
		try {
			let fired = 0;
			const h = harness();
			const dispose = watchAsyncControlInbox(asyncDir, { onInterrupt: () => fired++, fs: h.fsImpl, timers: h.timers });
			assert.equal(fired, 0);

			requestAsyncInterrupt(asyncDir);
			h.trigger();
			assert.equal(fired, 1);
			assert.equal(fs.existsSync(interruptRequestPath(asyncDir)), false);

			// No pending request → spurious event is a no-op.
			h.trigger();
			assert.equal(fired, 1);

			dispose();
			assert.equal(h.closed(), true);

			// After dispose, even a fresh request is ignored.
			requestAsyncInterrupt(asyncDir);
			h.trigger();
			assert.equal(fired, 1);
		} finally {
			cleanup(asyncDir);
		}
	});

	it("delivers stop requests before interrupt requests", () => {
		const asyncDir = tmpAsyncDir("pi-control-watch-stop-early-");
		try {
			requestAsyncStop(asyncDir);
			requestAsyncInterrupt(asyncDir);
			const events: string[] = [];
			const h = harness();
			const dispose = watchAsyncControlInbox(asyncDir, {
				onInterrupt: () => events.push("interrupt"),
				onStop: () => events.push("stop"),
				fs: h.fsImpl,
				timers: h.timers,
			});

			assert.deepEqual(events, ["stop", "interrupt"]);
			dispose();
		} finally {
			cleanup(asyncDir);
		}
	});

	it("delivers steer requests without firing interrupt", () => {
		const asyncDir = tmpAsyncDir("pi-control-watch-steer-");
		try {
			let interrupted = 0;
			const steers: Array<{ message: string; targetIndex?: number }> = [];
			const h = harness();
			const dispose = watchAsyncControlInbox(asyncDir, {
				onInterrupt: () => interrupted++,
				onSteer: (request) => steers.push({ message: request.message, targetIndex: request.targetIndex }),
				fs: h.fsImpl,
				timers: h.timers,
			});

			requestAsyncSteer(asyncDir, { message: "go narrower", targetIndex: 0, id: "s", ts: 1 });
			h.trigger();

			assert.equal(interrupted, 0);
			assert.deepEqual(steers, [{ message: "go narrower", targetIndex: 0 }]);
			dispose();
		} finally {
			cleanup(asyncDir);
		}
	});
});

// @effect-diagnostics nodeBuiltinImport:off - fixture exercises the native inbox boundary.
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import { sendClaudeMessage } from "./ClaudeSessionControl.ts";

it.effect("sends an authenticated prompt to the existing process and rejects a reused PID", () =>
  Effect.gen(function* () {
    const fixture = yield* Effect.acquireRelease(
      Effect.promise(async () => {
        const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-inbox-"));
        const socketPath = NodePath.join(home, "inbox.sock");
        const frames: Array<Record<string, unknown>> = [];
        let resolveReceived!: () => void;
        const received = new Promise<void>((resolve) => {
          resolveReceived = resolve;
        });
        const server = NodeNet.createServer((socket) => {
          let buffer = "";
          socket.on("data", (data) => {
            buffer += data.toString();
          });
          socket.on("end", () => {
            for (const line of buffer.trim().split("\n")) frames.push(JSON.parse(line));
            resolveReceived();
          });
        });
        await new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(socketPath, resolve);
        });
        const stat = await NodeFSP.readFile(`/proc/${process.pid}/stat`, "utf8");
        const procStart = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19]!;
        const session = {
          pid: process.pid,
          sessionId: "fixture",
          procStart,
          messagingSocketPath: socketPath,
        };
        await NodeFSP.mkdir(NodePath.join(home, "sessions"));
        const registry = NodePath.join(home, "sessions", `${process.pid}.json`);
        await NodeFSP.writeFile(registry, JSON.stringify(session));
        const hash = NodeCrypto.createHash("sha256").update(socketPath).digest("hex");
        await NodeFSP.writeFile(
          NodePath.join(home, "sessions", `${process.pid}.${hash}.key`),
          JSON.stringify({ peerToken: "a".repeat(32), procStart }),
          { mode: 0o600 },
        );
        return { home, server, registry, session, frames, received };
      }),
      (f) =>
        Effect.promise(async () => {
          await new Promise<void>((resolve) => f.server.close(() => resolve()));
          await NodeFSP.rm(f.home, { recursive: true, force: true });
        }),
    );
    const input = {
      configDirectory: fixture.home,
      sessionId: "fixture",
      uuid: "fixture-message",
      text: "User's exact prompt\nwith another line",
    };
    yield* sendClaudeMessage(input);
    yield* Effect.promise(() => fixture.received);
    expect(fixture.frames).toHaveLength(2);
    expect(fixture.frames[0]?.type).toBe("auth");
    expect(fixture.frames[1]).toMatchObject({
      type: "user",
      session_id: "fixture",
      uuid: input.uuid,
      priority: "next",
      message: { role: "user", content: input.text },
    });
    yield* Effect.promise(() =>
      NodeFSP.writeFile(fixture.registry, JSON.stringify({ ...fixture.session, procStart: "0" })),
    );
    expect((yield* sendClaudeMessage(input).pipe(Effect.flip))._tag).toBe(
      "ClaudeSessionControlError",
    );
    expect(fixture.frames).toHaveLength(2);
  }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
);

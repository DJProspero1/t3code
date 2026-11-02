import * as NodeNet from "node:net";
// @effect-diagnostics-next-line nodeBuiltinImport:off - inspect process identity and private inbox credentials without exposing them.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Claude names inbox credentials by the socket path hash.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off - native local paths belong to this transport boundary.
import * as NodePath from "node:path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const decodeSession = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      sessionId: Schema.String,
      pid: Schema.Int,
      procStart: Schema.String,
      messagingSocketPath: Schema.String,
    }),
  ),
);
const decodeKey = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      peerToken: Schema.String.check(Schema.isPattern(/^[0-9a-f]{32}$/)),
      procStart: Schema.String,
    }),
  ),
);
export class ClaudeSessionControlError extends Schema.TaggedError<ClaudeSessionControlError>()(
  "ClaudeSessionControlError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "Claude's live inbox could not accept this message. Check that the original session is running. Delivery may be uncertain; automatic resend is disabled.";
  }
}
export const sendClaudeMessage = Effect.fn("ClaudeSessionControl.send")(function* (input: {
  configDirectory: string;
  sessionId: string;
  uuid: string;
  text: string;
}) {
  const platform = yield* HostProcess.Platform;
  const uid = yield* HostProcess.UserId;
  return yield* Effect.tryPromise({
    try: async () => {
      if (platform !== "linux") throw new Error("Live inbox control currently requires Linux");
      const directory = NodePath.join(input.configDirectory, "sessions");
      const matches: Array<ReturnType<typeof decodeSession>> = [];
      for (const file of await NodeFSP.readdir(directory)) {
        if (!/^\d+\.json$/.test(file)) continue;
        try {
          const session = decodeSession(
            await NodeFSP.readFile(NodePath.join(directory, file), "utf8"),
          );
          if (session.sessionId !== input.sessionId || session.pid <= 0) continue;
          const stat = await NodeFSP.readFile(`/proc/${session.pid}/stat`, "utf8");
          if (stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] === session.procStart)
            matches.push(session);
        } catch {
          /* Stale registry entries cannot receive messages. */
        }
      }
      if (matches.length !== 1) throw new Error("Expected one live owner");
      const session = matches[0]!;
      const socketPath = session.messagingSocketPath;
      const socketStat = await NodeFSP.lstat(socketPath);
      if (!socketStat.isSocket() || socketStat.uid !== uid) throw new Error("Invalid socket owner");
      const hash = NodeCrypto.createHash("sha256")
        .update(NodePath.resolve(socketPath))
        .digest("hex");
      const keyPath = NodePath.join(directory, `${session.pid}.${hash}.key`);
      const keyStat = await NodeFSP.lstat(keyPath);
      if (!keyStat.isFile() || keyStat.uid !== uid || (keyStat.mode & 0o077) !== 0)
        throw new Error("Invalid inbox key permissions");
      const key = decodeKey(await NodeFSP.readFile(keyPath, "utf8"));
      if (key.procStart !== session.procStart) throw new Error("Inbox owner changed");
      await new Promise<void>((resolve, reject) => {
        const socket = NodeNet.createConnection(socketPath);
        socket.setTimeout(5000, () => socket.destroy(new Error("Inbox timed out")));
        socket.once("error", reject);
        socket.once("connect", () => {
          // The peer inbox queues a prompt between tool calls; it does not replace the process.
          socket.end(
            JSON.stringify({ type: "auth", token: key.peerToken }) +
              "\n" +
              JSON.stringify({
                type: "user",
                session_id: input.sessionId,
                uuid: input.uuid,
                priority: "next",
                message: { role: "user", content: input.text },
              }) +
              "\n",
            () => {
              socket.destroy();
              resolve();
            },
          );
        });
      });
    },
    catch: (cause) => new ClaudeSessionControlError({ cause }),
  });
});

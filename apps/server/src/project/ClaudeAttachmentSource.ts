// @effect-diagnostics-next-line nodeBuiltinImport:off - this adapter owns positional reads on one open descriptor.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off - native paths are used inside the filesystem adapter.
import * as NodePath from "node:path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

const Session = Schema.Struct({
  procStart: Schema.optional(Schema.String),
  messagingSocketPath: Schema.optional(Schema.String),
  pid: Schema.Int,
  sessionId: Schema.String,
  cwd: Schema.String,
  name: Schema.optional(Schema.String),
  status: Schema.optional(Schema.String),
});
const LiveAlias = Schema.Struct({
  version: Schema.Literal(1),
  epoch: Schema.String,
  uuid: Schema.String,
  role: Schema.Literals(["user", "assistant"]),
  agentId: Schema.NullOr(Schema.String),
  step: Schema.optional(
    Schema.Struct({ turnId: Schema.String, index: Schema.Int, blockIndex: Schema.Int }),
  ),
  commandId: Schema.optional(Schema.String),
});
const decodeLiveAlias = Schema.decodeUnknownSync(Schema.fromJsonString(LiveAlias));
const decodeSession = Schema.decodeUnknownSync(Schema.fromJsonString(Session));
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const agentFile = /^agent-([a-zA-Z0-9_-]+)\.jsonl$/;
const CHUNK_BYTES = 512 * 1024;
const MAX_LINE_BYTES = 8 * 1024 * 1024;

export class ClaudeAttachmentSourceError extends Schema.TaggedError<ClaudeAttachmentSourceError>()(
  "ClaudeAttachmentSourceError",
  {
    operation: Schema.Literals(["discover", "read", "subagents", "handoff"]),
    cause: Schema.Defect(),
  },
) {
  override get message() {
    if (this.operation === "handoff")
      return "Handoff requires the original Claude session to have exited after its agents finish. T3 will not stop a process based on transcript inactivity.";
    return `Could not ${this.operation} the Claude session. Check that it is running locally and its transcript is readable.`;
  }
}

export interface TranscriptCursor {
  offset: number;
  identity?: string | undefined;
}
export class ClaudeAttachmentSource extends Context.Service<
  ClaudeAttachmentSource,
  {
    readonly aliases: (
      home: string,
      sessionId: string,
      uuids: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<typeof LiveAlias.Type>, ClaudeAttachmentSourceError>;
    readonly assertStopped: (
      home: string,
      sessionId: string,
    ) => Effect.Effect<void, ClaudeAttachmentSourceError>;
    readonly tail: (
      home: string,
      path: string,
    ) => Effect.Effect<
      {
        lines: ReadonlyArray<string>;
        offset: number;
        size: number;
        modifiedAt: number;
        identity?: string;
      },
      ClaudeAttachmentSourceError
    >;
    readonly discover: (
      home: string,
      query: string,
    ) => Effect.Effect<
      {
        sessionId: string;
        title: string;
        transcriptPath: string;
      },
      ClaudeAttachmentSourceError
    >;
    readonly children: (
      home: string,
      transcript: string,
    ) => Effect.Effect<
      ReadonlyArray<{ agentId: string; path: string }>,
      ClaudeAttachmentSourceError
    >;
    readonly read: (
      home: string,
      path: string,
      cursor: TranscriptCursor,
    ) => Effect.Effect<
      {
        cursor: TranscriptCursor;
        lines: ReadonlyArray<string>;
        caughtUp: boolean;
      },
      ClaudeAttachmentSourceError
    >;
  }
>()("t3/project/ClaudeAttachmentSource") {}

// Resolve both sides on every read: a replaced symlink must not escape the transcript directory.
async function checkedPath(home: string, file: string) {
  const root = await NodeFSP.realpath(NodePath.join(home, "projects"));
  const actual = await NodeFSP.realpath(file);
  const relative = NodePath.relative(root, actual);
  if (
    !relative ||
    relative.startsWith(`..${NodePath.sep}`) ||
    relative === ".." ||
    NodePath.isAbsolute(relative)
  ) {
    throw new Error("Transcript is outside Claude projects");
  }
  return actual;
}

export const layer = Layer.sync(ClaudeAttachmentSource, () => {
  const tailCache = new Map<
    string,
    {
      stamp: string;
      result: {
        lines: string[];
        offset: number;
        size: number;
        modifiedAt: number;
        identity: string;
      };
    }
  >();

  return ClaudeAttachmentSource.of({
    aliases: Effect.fn("ClaudeAttachmentSource.aliases")((home, sessionId, uuids) =>
      Effect.tryPromise({
        try: async () => {
          if (!/^[a-zA-Z0-9_-]{1,128}$/.test(sessionId)) return [];
          const expected = NodePath.join(home, "t3-live-session", "aliases", sessionId);
          let root: string;
          try {
            root = await NodeFSP.realpath(expected);
          } catch (cause) {
            if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
            throw cause;
          }
          // Alias files contain identities only, never transcript content or credentials.
          const result: Array<typeof LiveAlias.Type> = [];
          for (let index = 0; index < uuids.length; index += 16) {
            const rows = await Promise.all(
              uuids.slice(index, index + 16).map(async (id) => {
                if (!/^[a-zA-Z0-9_-]{1,128}$/.test(id)) return undefined;
                let file: NodeFSP.FileHandle | undefined;
                try {
                  const actual = await NodeFSP.realpath(NodePath.join(root, `${id}.json`));
                  if (NodePath.dirname(actual) !== root) throw new Error("Invalid live alias path");
                  file = await NodeFSP.open(actual, "r");
                  if ((await file.stat()).size > 4096) throw new Error("Oversized live alias");
                  const alias = decodeLiveAlias(await file.readFile("utf8"));
                  return alias.uuid === id ? alias : undefined;
                } catch (cause) {
                  if ((cause as NodeJS.ErrnoException).code === "ENOENT") return undefined;
                  throw cause;
                } finally {
                  await file?.close();
                }
              }),
            );
            for (const row of rows) if (row) result.push(row);
          }
          return result;
        },
        catch: (cause) => new ClaudeAttachmentSourceError({ operation: "read", cause }),
      }),
    ),
    assertStopped: Effect.fn("ClaudeAttachmentSource.assertStopped")(function* (home, sessionId) {
      const platform = yield* HostProcess.Platform;
      return yield* Effect.tryPromise({
        try: async () => {
          // A quiet transcript cannot establish that background agents have stopped.
          for (const name of await NodeFSP.readdir(NodePath.join(home, "sessions"))) {
            if (!/^\d+\.json$/.test(name)) continue;
            let session;
            try {
              session = decodeSession(
                await NodeFSP.readFile(NodePath.join(home, "sessions", name), "utf8"),
              );
            } catch (cause) {
              if ((cause as NodeJS.ErrnoException).code === "ENOENT") continue;
              throw cause;
            }
            if (session.sessionId !== sessionId) continue;
            if (session.pid <= 0) throw new Error("Invalid session owner");
            try {
              process.kill(session.pid, 0);
            } catch (cause) {
              if ((cause as NodeJS.ErrnoException).code === "ESRCH") continue;
              throw cause;
            }
            if (platform === "linux" && session.procStart) {
              try {
                const stat = await NodeFSP.readFile(`/proc/${session.pid}/stat`, "utf8");
                if (stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] !== session.procStart)
                  continue;
              } catch (cause) {
                if ((cause as NodeJS.ErrnoException).code === "ENOENT") continue;
                throw cause;
              }
            }
            throw new Error("The original session still has a live owner");
          }
        },
        catch: (cause) => new ClaudeAttachmentSourceError({ operation: "handoff", cause }),
      });
    }),
    tail: Effect.fn("ClaudeAttachmentSource.tail")((home, path) =>
      Effect.tryPromise({
        try: async () => {
          const actual = await checkedPath(home, path);
          const file = await NodeFSP.open(actual, "r");
          try {
            const stat = await file.stat();
            const stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}`;
            const cached = tailCache.get(actual);
            if (cached?.stamp === stamp) return cached.result;
            const offset = Math.max(0, stat.size - 256 * 1024);
            const buffer = Buffer.alloc(stat.size - offset);
            const { bytesRead } = await file.read(buffer, 0, buffer.length, offset);
            const complete = buffer.subarray(0, bytesRead);
            const end = complete.lastIndexOf(10);
            const start = offset ? complete.indexOf(10) + 1 : 0;
            const completeLines =
              end >= start ? complete.subarray(start, end).toString("utf8").split("\n") : [];
            const omitted = completeLines.slice(0, -64);
            const result = {
              lines: completeLines.slice(-64),
              offset:
                offset +
                start +
                omitted.reduce((bytes, line) => bytes + Buffer.byteLength(line) + 1, 0),
              identity: `${stat.dev}:${stat.ino}`,
              size: stat.size,
              modifiedAt: stat.mtimeMs,
            };
            if (tailCache.size >= 2048) tailCache.clear();
            tailCache.set(actual, { stamp, result });
            return result;
          } finally {
            await file.close();
          }
        },
        catch: (cause) => new ClaudeAttachmentSourceError({ operation: "read", cause }),
      }),
    ),
    discover: Effect.fn("ClaudeAttachmentSource.discover")((home, query) =>
      Effect.tryPromise({
        try: async () => {
          const sessions: Array<typeof Session.Type> = [];
          for (const name of await NodeFSP.readdir(NodePath.join(home, "sessions"))) {
            if (!/^\d+\.json$/.test(name)) continue;
            try {
              const session = decodeSession(
                await NodeFSP.readFile(NodePath.join(home, "sessions", name), "utf8"),
              );
              if (session.pid <= 0 || !uuid.test(session.sessionId)) continue;
              process.kill(session.pid, 0);
              if (query === session.sessionId || query === session.name) sessions.push(session);
            } catch {
              /* Stale or partially written registry entries are not live sessions. */
            }
          }
          if (sessions.length !== 1)
            throw new Error("Expected exactly one live session with this name or ID");
          const session = sessions[0]!;
          const candidates: Array<string> = [];
          for (const dir of await NodeFSP.readdir(NodePath.join(home, "projects"), {
            withFileTypes: true,
          })) {
            if (!dir.isDirectory()) continue;
            const file = NodePath.join(home, "projects", dir.name, `${session.sessionId}.jsonl`);
            try {
              candidates.push(await checkedPath(home, file));
            } catch {
              /* This directory has no matching transcript. */
            }
          }
          if (candidates.length !== 1)
            throw new Error("Expected exactly one transcript for the live session");
          return {
            sessionId: session.sessionId,
            title: session.name ?? session.sessionId,
            transcriptPath: candidates[0]!,
          };
        },
        catch: (cause) => new ClaudeAttachmentSourceError({ operation: "discover", cause }),
      }),
    ),
    children: Effect.fn("ClaudeAttachmentSource.children")((home, transcript) =>
      Effect.tryPromise({
        try: async () => {
          const root = await checkedPath(home, transcript);
          const dir = NodePath.join(root.slice(0, -6), "subagents");
          let entries;
          try {
            entries = await NodeFSP.readdir(dir, { withFileTypes: true });
          } catch (cause) {
            if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
            throw cause;
          }
          return entries.flatMap((entry) => {
            const match = agentFile.exec(entry.name);
            return entry.isFile() && match
              ? [{ agentId: match[1]!, path: NodePath.join(dir, entry.name) }]
              : [];
          });
        },
        catch: (cause) => new ClaudeAttachmentSourceError({ operation: "subagents", cause }),
      }),
    ),
    read: Effect.fn("ClaudeAttachmentSource.read")((home, path, cursor) =>
      Effect.tryPromise({
        try: async () => {
          const actual = await checkedPath(home, path);
          const file = await NodeFSP.open(actual, "r");
          try {
            const stat = await file.stat();
            const identity = `${stat.dev}:${stat.ino}`;
            let offset =
              cursor.identity === identity && cursor.offset <= stat.size ? cursor.offset : 0;
            const buffer = Buffer.alloc(Math.min(CHUNK_BYTES, Math.max(0, stat.size - offset)));
            let data = Buffer.alloc(0);
            while (offset + data.length < stat.size) {
              const { bytesRead } = await file.read(buffer, 0, buffer.length, offset + data.length);
              if (bytesRead === 0) break;
              data = Buffer.concat([data, buffer.subarray(0, bytesRead)]);
              if (data.includes(10)) break;
              if (data.length >= MAX_LINE_BYTES)
                throw new Error("Transcript line exceeds the size limit");
            }
            const end = data.lastIndexOf(10);
            if (end < 0) return { cursor: { offset, identity }, lines: [], caughtUp: true };
            const lines = data.subarray(0, end).toString("utf8").split("\n");
            offset += end + 1;
            return { cursor: { offset, identity }, lines, caughtUp: offset >= stat.size };
          } finally {
            await file.close();
          }
        },
        catch: (cause) => new ClaudeAttachmentSourceError({ operation: "read", cause }),
      }),
    ),
  });
});

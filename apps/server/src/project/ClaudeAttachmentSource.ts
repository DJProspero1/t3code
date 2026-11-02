// @effect-diagnostics-next-line nodeBuiltinImport:off - this adapter owns positional reads on one open descriptor.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off - native paths are used inside the filesystem adapter.
import * as NodePath from "node:path";
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
const decodeSession = Schema.decodeUnknownSync(Schema.fromJsonString(Session));
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const agentFile = /^agent-([a-zA-Z0-9_-]+)\.jsonl$/;
const CHUNK_BYTES = 512 * 1024;
const MAX_LINE_BYTES = 8 * 1024 * 1024;

export class ClaudeAttachmentSourceError extends Schema.TaggedError<ClaudeAttachmentSourceError>()(
  "ClaudeAttachmentSourceError",
  {
    operation: Schema.Literals(["discover", "read", "subagents"]),
    cause: Schema.Defect(),
  },
) {
  override get message() {
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
    readonly tail: (
      home: string,
      path: string,
    ) => Effect.Effect<
      { lines: ReadonlyArray<string>; size: number; modifiedAt: number; identity?: string },
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
      result: { lines: string[]; size: number; modifiedAt: number; identity: string };
    }
  >();

  return ClaudeAttachmentSource.of({
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
            const result = {
              lines:
                end >= start
                  ? complete.subarray(start, end).toString("utf8").split("\n").slice(-64)
                  : [],
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

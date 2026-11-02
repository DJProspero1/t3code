// @effect-diagnostics-next-line nodeBuiltinImport:off - fixtures exercise the native filesystem adapter.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off - fixture paths match the native adapter.
import * as NodePath from "node:path";
import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Source from "./ClaudeAttachmentSource.ts";

const fixture = Effect.acquireRelease(
  Effect.promise(async () => {
    const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-claude-attach-"));
    const sessionId = "00000000-0000-4000-8000-000000000001";
    const root = NodePath.join(home, "projects", "deleted-workspace");
    const file = NodePath.join(root, `${sessionId}.jsonl`);
    await NodeFSP.mkdir(root, { recursive: true });
    await NodeFSP.mkdir(NodePath.join(home, "sessions"));
    await NodeFSP.writeFile(
      NodePath.join(home, "sessions", `${process.pid}.json`),
      JSON.stringify({
        pid: process.pid,
        sessionId,
        cwd: "/a/deleted/workspace",
        name: "repair-session",
      }),
    );
    return { home, file, sessionId };
  }),
  (f) => Effect.promise(() => NodeFSP.rm(f.home, { recursive: true, force: true })),
);

it.effect(
  "finds the exact live session even when its workspace was deleted and discovers new subagents",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const source = yield* Source.ClaudeAttachmentSource;
      yield* Effect.promise(() => NodeFSP.writeFile(f.file, "{}\n"));
      expect((yield* source.discover(f.home, "repair-session")).sessionId).toBe(f.sessionId);
      expect(yield* source.children(f.home, f.file)).toEqual([]);
      const dir = f.file.slice(0, -6) + "/subagents";
      yield* Effect.promise(async () => {
        await NodeFSP.mkdir(dir, { recursive: true });
        await NodeFSP.writeFile(NodePath.join(dir, "agent-child.jsonl"), "{}\n");
      });
      expect(yield* source.children(f.home, f.file)).toEqual([
        { agentId: "child", path: NodePath.join(dir, "agent-child.jsonl") },
      ]);
      expect((yield* source.discover(f.home, "repair").pipe(Effect.flip))._tag).toBe(
        "ClaudeAttachmentSourceError",
      );
    }).pipe(Effect.provide(Source.layer)),
);

it.effect("waits for complete UTF-8 records and restarts after file replacement", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const source = yield* Source.ClaudeAttachmentSource;
    yield* Effect.promise(() => NodeFSP.writeFile(f.file, '{"text":"olá"}\n{"text":'));
    const first = yield* source.read(f.home, f.file, { offset: 0 });
    expect(first.lines).toEqual(['{"text":"olá"}']);
    const partial = yield* source.read(f.home, f.file, first.cursor);
    expect(partial.lines).toEqual([]);
    expect(partial.cursor).toEqual(first.cursor);
    yield* Effect.promise(() => NodeFSP.appendFile(f.file, '"done"}\n'));
    const next = yield* source.read(f.home, f.file, partial.cursor);
    expect(next.lines).toEqual(['{"text":"done"}']);
    yield* Effect.promise(async () => {
      await NodeFSP.rm(f.file);
      await NodeFSP.writeFile(f.file, "{}\n");
    });
    expect((yield* source.read(f.home, f.file, next.cursor)).lines).toEqual(["{}"]);
  }).pipe(Effect.provide(Source.layer)),
);

it.effect("rejects transcripts symlinked outside Claude projects", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const source = yield* Source.ClaudeAttachmentSource;
    yield* Effect.promise(async () => {
      const outside = NodePath.join(f.home, "secret.txt");
      await NodeFSP.writeFile(outside, "private\n");
      await NodeFSP.symlink(outside, f.file);
    });
    expect((yield* source.read(f.home, f.file, { offset: 0 }).pipe(Effect.flip))._tag).toBe(
      "ClaudeAttachmentSourceError",
    );
  }).pipe(Effect.provide(Source.layer)),
);

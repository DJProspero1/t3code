import { expect, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ProjectId, ProviderDriverKind } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Source from "./ClaudeAttachmentSource.ts";
import * as Attachment from "./ClaudeSessionAttachment.ts";
import * as Project from "./ProjectService.ts";
import { attachmentThreadId } from "./claudeAttachmentProjection.ts";

it.effect(
  "keeps a live source untouched, then drains history and preserves the native conversation on handoff",
  () =>
    Effect.gen(function* () {
      let alive = true;
      let restartDuringImport = false;
      let incomplete = false;
      let accountHome = "/claude";
      let accountEnabled = true;
      const root = "/claude/projects/project/session.jsonl";
      const child = "/claude/projects/project/session/subagents/agent-child.jsonl";
      const record = (uuid: string) =>
        JSON.stringify({
          type: "assistant",
          uuid,
          sessionId: "session",
          cwd: "/work/original",
          timestamp: "2026-01-01T00:00:00Z",
          message: { content: uuid, stop_reason: "end_turn", model: "claude-sonnet-4-6" },
        });
      const files = new Map([
        [root, [record("first"), record("latest")]],
        [child, [record("child-final")]],
      ]);
      const source = Layer.mock(Source.ClaudeAttachmentSource)({
        aliases: () => Effect.succeed([]),
        discover: () =>
          Effect.succeed({ sessionId: "session", title: "Session", transcriptPath: root }),
        assertStopped: () =>
          alive
            ? Effect.fail(
                new Source.ClaudeAttachmentSourceError({ operation: "handoff", cause: "alive" }),
              )
            : Effect.void,
        children: () =>
          Effect.sync(() => (files.has(child) ? [{ agentId: "child", path: child }] : [])),
        tail: (_home, path) =>
          Effect.sync(() => {
            if (restartDuringImport && path === child) alive = true;
            return {
              lines: files.get(path)!,
              offset: 0,
              modifiedAt: 0,
              size: Buffer.byteLength(files.get(path)!.join("\n") + "\n") + (incomplete ? 1 : 0),
            };
          }),
        read: (_home, path, cursor) =>
          Effect.sync(() => {
            const bytes = Buffer.from(files.get(path)!.join("\n") + "\n");
            const remaining = bytes.subarray(cursor.offset);
            const end = remaining.indexOf(10);
            return end < 0
              ? { lines: [], cursor, caughtUp: true }
              : {
                  lines: [remaining.subarray(0, end).toString()],
                  cursor: { offset: cursor.offset + end + 1 },
                  caughtUp: cursor.offset + end + 1 === bytes.length,
                };
          }),
      });
      const projectId = ProjectId.make("project");
      const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
        Layer.provideMerge(Sqlite.layerMemory),
      );
      const deps = Layer.mergeAll(
        stores,
        EventSink.layer.pipe(Layer.provide(stores)),
        source,
        Layer.succeed(HostProcess.HomeDirectory, "/"),
        Layer.succeed(HostProcess.Environment, { CLAUDE_CONFIG_DIR: "/claude" }),
        Path.layer,
        Layer.succeed(ServerActivation, Effect.never),
        Layer.mock(ServerSettings.ServerSettingsService)({
          getSettings: Effect.sync(() => ({
            ...DEFAULT_SERVER_SETTINGS,
            providerInstances: {
              claudeAgent: {
                driver: ProviderDriverKind.make("claudeAgent"),
                enabled: accountEnabled,
                config: { homePath: accountHome },
              },
            },
          })),
        }),
        Layer.mock(Project.ProjectService)({
          getById: () =>
            Effect.succeedSome({
              id: projectId,
              title: "Project",
              workspaceRoot: "/work/project",
              defaultModelSelection: null,
              scripts: [],
              createdAt: "2026-01-01T00:00:00Z",
              updatedAt: "2026-01-01T00:00:00Z",
              deletedAt: null,
            }),
        }),
      );
      yield* Effect.gen(function* () {
        const service = yield* Attachment.ClaudeSessionAttachment;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const attached = yield* service.change({ action: "attach", projectId, session: "Session" });
        const before = yield* projections.getThread(attached.threadId);
        accountHome = "/another-account";
        expect(
          (yield* service
            .change({ action: "handoff", threadId: attached.threadId })
            .pipe(Effect.flip)).reason,
        ).toBe("handoff-blocked");
        accountHome = "/claude";
        accountEnabled = false;
        expect(
          (yield* service
            .change({ action: "handoff", threadId: attached.threadId })
            .pipe(Effect.flip)).reason,
        ).toBe("unavailable-instance");
        accountEnabled = true;
        yield* service.change({ action: "handoff", threadId: attached.threadId }).pipe(Effect.flip);
        expect(yield* projections.getThread(attached.threadId)).toEqual(before);
        alive = false;
        incomplete = true;
        yield* service.change({ action: "handoff", threadId: attached.threadId }).pipe(Effect.flip);
        expect((yield* projections.getThread(attached.threadId)).externalSession?.state).toBe(
          "detached",
        );
        incomplete = false;
        restartDuringImport = true;
        yield* service.change({ action: "handoff", threadId: attached.threadId }).pipe(Effect.flip);
        expect((yield* projections.getThread(attached.threadId)).externalSession).toBeDefined();
        restartDuringImport = false;
        alive = false;
        files.delete(child);
        expect(yield* service.change({ action: "handoff", threadId: attached.threadId })).toEqual({
          threadId: attached.threadId,
          attached: false,
          readOnly: false,
        });
        const after = yield* projections.getThread(attached.threadId);
        expect(after.externalSession).toBeUndefined();
        expect(after.worktreePath).toBe("/work/original");
        expect(after.modelSelection.model).toBe("claude-sonnet-4-6");
        expect(after.activeProviderThreadId).toBe(before.activeProviderThreadId);
        const records = yield* projections.getThreadRecords(attached.threadId, [
          "messages",
          "providerThreads",
        ]);
        expect(records.messages.map((message) => message.text).sort()).toEqual(["first", "latest"]);
        expect(records.providerThreads[0]?.nativeThreadRef?.nativeId).toBe("session");
        const childId = attachmentThreadId("/claude", "session", "child");
        expect((yield* projections.getThread(childId)).externalSession?.attached).toBe(false);
        expect((yield* projections.getThreadRecords(childId, ["messages"])).messages[0]?.text).toBe(
          "child-final",
        );
        yield* service.sync;
        expect(yield* projections.getThread(attached.threadId)).toEqual(after);
        expect(yield* service.change({ action: "handoff", threadId: attached.threadId })).toEqual({
          threadId: attached.threadId,
          attached: false,
          readOnly: false,
        });
        expect(yield* projections.getThread(attached.threadId)).toEqual(after);
      }).pipe(Effect.provide(Attachment.layer.pipe(Layer.provideMerge(deps))));
    }),
);

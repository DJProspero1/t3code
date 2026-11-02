import { expect, it } from "@effect/vitest";
import { ProjectId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as Source from "./ClaudeAttachmentSource.ts";
import * as Attachment from "./ClaudeSessionAttachment.ts";
import * as Project from "./ProjectService.ts";
import { attachmentNodeId, attachmentThreadId } from "./claudeAttachmentProjection.ts";

const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
);
const persistence = Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores)));

it.effect(
  "follows existing and future children, replays idempotently after restart, and detaches without executing anything",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("project");
      const files = new Map<string, string[]>();
      const rootPath = "/claude/projects/project/session.jsonl";
      const childPath = (id: string) => `${rootPath.slice(0, -6)}/subagents/agent-${id}.jsonl`;
      const rootLine = JSON.stringify({
        type: "user",
        uuid: "prompt",
        sessionId: "session",
        timestamp: "2026-01-01T00:00:00Z",
        message: { content: "Repair the purchases" },
      });
      const reply = (uuid: string) =>
        JSON.stringify({
          type: "assistant",
          uuid,
          sessionId: "session",
          timestamp: "2026-01-01T00:01:00Z",
          message: { content: "Done", stop_reason: "end_turn" },
        });
      files.set(rootPath, [rootLine]);
      files.set(childPath("child-one"), [reply("child-one")]);
      const source = Layer.mock(Source.ClaudeAttachmentSource)({
        discover: () =>
          Effect.succeed({
            sessionId: "session",
            title: "repair-session",
            transcriptPath: rootPath,
          }),
        children: () =>
          Effect.sync(() =>
            [...files.keys()]
              .filter((path) => path !== rootPath)
              .map((path) => ({ path, agentId: path.split("/").at(-1)!.slice(6, -6) })),
          ),
        read: (_home, path, cursor) =>
          Effect.sync(() => {
            const lines = files.get(path) ?? [];
            return {
              lines: lines.slice(cursor.offset),
              cursor: { offset: lines.length },
              caughtUp: true,
            };
          }),
      });
      const projection = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const count = () =>
        sql<{ count: number }>`SELECT count(*) AS count FROM orchestration_v2_projection_messages`;
      const deps = Layer.mergeAll(
        source,
        Layer.succeed(ProjectionStore.ProjectionStoreV2, projection),
        Layer.succeed(EventSink.EventSinkV2, yield* EventSink.EventSinkV2),
        Layer.succeed(SqlClient.SqlClient, sql),
        Layer.succeed(HostProcess.HomeDirectory, "/"),
        Layer.succeed(HostProcess.Environment, { CLAUDE_CONFIG_DIR: "/claude" }),
        Path.layer,
        Layer.succeed(ServerActivation, Effect.never),
        Layer.mock(Project.ProjectService)({
          getById: () =>
            Effect.succeedSome({
              id: projectId,
              title: "Project",
              workspaceRoot: "/deleted/project",
              defaultModelSelection: null,
              scripts: [],
              createdAt: "2026-01-01T00:00:00Z",
              updatedAt: "2026-01-01T00:00:00Z",
              deletedAt: null,
            }),
        }),
      );
      const firstContext = yield* Layer.build(Attachment.layer.pipe(Layer.provide(deps)));
      const first = Context.get(firstContext, Attachment.ClaudeSessionAttachment);
      const attached = yield* first.change({
        action: "attach",
        projectId,
        session: "repair-session",
      });
      yield* first.sync;
      expect(
        (yield* projection.getThreadRecords(attached.threadId, ["subagents"])).subagents,
      ).toHaveLength(1);
      files.set(childPath("child-two"), [reply("child-two")]);
      files.get(childPath("child-one"))!.push(
        JSON.stringify({
          type: "user",
          uuid: "spawn-two",
          sessionId: "session",
          timestamp: "2026-01-01T00:02:00Z",
          message: { content: [] },
          toolUseResult: {
            agentId: "child-two",
            description: "Nested repair",
            resolvedModel: "claude-opus-4-6",
          },
        }),
      );
      yield* first.sync;
      const state = yield* projection.getThreadRecords(attached.threadId, ["subagents", "runs"]);
      expect(state.subagents).toHaveLength(2);
      expect(state.runs).toHaveLength(0);
      const nested = yield* projection.getThread(
        attachmentThreadId("/claude", "session", "child-two"),
      );
      expect(nested.lineage.parentThreadId).toBe(
        attachmentThreadId("/claude", "session", "child-one"),
      );
      expect(nested.title).toBe("Nested repair");
      expect((yield* projection.getThreadRecords(nested.id, ["nodes"])).nodes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ kind: "root_turn", status: "completed", runId: null }),
        ]),
      );
      expect(state.subagents.find((child) => child.childThreadId === nested.id)?.parentNodeId).toBe(
        attachmentNodeId(nested.lineage.parentThreadId!),
      );
      expect(state.subagents.every((child) => child.origin === "provider_native")).toBe(true);
      expect(
        (yield* projection.getThreadRecords(attachmentThreadId("/claude", "session", "child-two"), [
          "messages",
        ])).messages,
      ).toHaveLength(1);
      const before = yield* count();
      const restartedContext = yield* Layer.build(
        Layer.fresh(Attachment.layer).pipe(Layer.provide(deps)),
      );
      const restarted = Context.get(restartedContext, Attachment.ClaudeSessionAttachment);
      yield* restarted.sync;
      expect(yield* count()).toEqual(before);
      yield* restarted.change({ action: "detach", threadId: attached.threadId });
      expect(
        (yield* projection.getThread(attachmentThreadId("/claude", "session", "child-two")))
          .externalSession?.state,
      ).toBe("detached");
      files.get(rootPath)!.push(reply("after-detach"));
      yield* restarted.sync;
      expect(yield* count()).toEqual(before);
      expect((yield* projection.getThread(attached.threadId)).externalSession?.state).toBe(
        "detached",
      );
    }).pipe(Effect.provide(persistence)),
);

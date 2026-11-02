import { expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderInstanceId,
  ProviderDriverKind,
  ProjectId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Source from "./ClaudeAttachmentSource.ts";
import * as Attachment from "./ClaudeSessionAttachment.ts";
import * as Project from "./ProjectService.ts";
import {
  attachmentEvent,
  attachmentNodeId,
  attachmentThreadId,
} from "./claudeAttachmentProjection.ts";

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
      let readCalls = 0;
      let chunkSize = Infinity;
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
        tail: (_home, path) =>
          Effect.sync(() => ({
            lines: (files.get(path) ?? []).slice(-2),
            offset: (files.get(path) ?? [])
              .slice(0, -2)
              .reduce((bytes, line) => bytes + Buffer.byteLength(line) + 1, 0),
            size: (files.get(path) ?? []).join("\n").length,
            modifiedAt: 1,
          })),
        read: (_home, path, cursor) =>
          Effect.sync(() => {
            readCalls++;
            const lines = files.get(path) ?? [];
            return {
              lines: lines.slice(cursor.offset, cursor.offset + chunkSize),
              cursor: { offset: Math.min(lines.length, cursor.offset + chunkSize) },
              caughtUp: cursor.offset + chunkSize >= lines.length,
            };
          }),
      });
      const projection = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const count = () =>
        sql<{ count: number }>`SELECT count(*) AS count FROM orchestration_v2_projection_messages`;
      const deps = Layer.mergeAll(
        source,
        Layer.mock(ServerSettings.ServerSettingsService)({
          getSettings: Effect.succeed({
            ...DEFAULT_SERVER_SETTINGS,
            providerInstances: {
              work: {
                driver: ProviderDriverKind.make("claudeAgent"),
                config: { homePath: "~/claude-work" },
              },
              disabled: { driver: ProviderDriverKind.make("claudeAgent"), enabled: false },
              codex: { driver: ProviderDriverKind.make("codex") },
            },
          }),
        }),
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
      const initialReads = readCalls;
      yield* first.sync;
      expect(readCalls).toBe(initialReads);
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
      expect((yield* projection.getThread(attached.threadId)).externalSession?.state).toBe(
        "following",
      );
      // An upgrade must correct already-imported bubbles, not just future records.
      const prior = yield* projection.getThread(attached.threadId);
      const at = yield* DateTime.now;
      yield* (yield* EventSink.EventSinkV2).write({
        events: [
          attachmentEvent(
            prior.id,
            "thread.metadata-updated",
            { ...prior, externalSession: { ...prior.externalSession!, projectionVersion: 1 } },
            at,
            "old-projection",
          ),
        ],
      });
      files.set(rootPath, [
        JSON.stringify({
          type: "user",
          uuid: "prompt",
          sessionId: "session",
          timestamp: "2026-01-01T00:00:00Z",
          isMeta: true,
          message: { content: "Internal context" },
        }),
      ]);
      yield* restarted.sync;
      expect((yield* projection.getThreadRecords(prior.id, ["messages"])).messages[0]?.role).toBe(
        "system",
      );
      expect((yield* projection.getThread(prior.id)).externalSession?.projectionVersion).toBe(4);
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
      const work = yield* restarted.change({
        action: "attach",
        projectId,
        session: "repair-session",
        providerInstanceId: ProviderInstanceId.make("work"),
      });
      expect(work.threadId).not.toBe(attached.threadId);
      expect(yield* projection.getThread(work.threadId)).toMatchObject({
        providerInstanceId: "work",
        externalSession: { configDirectory: "/claude-work" },
      });
      for (const providerInstanceId of ["disabled", "codex", "missing"]) {
        expect(
          (yield* restarted
            .change({
              action: "attach",
              projectId,
              session: "repair-session",
              providerInstanceId: ProviderInstanceId.make(providerInstanceId),
            })
            .pipe(Effect.flip))._tag,
        ).toBe("ClaudeSessionAttachmentError");
      }
      const sink = yield* EventSink.EventSinkV2;
      const beforeRename = yield* projection.getThread(work.threadId);
      yield* sink.write({
        events: [
          attachmentEvent(
            work.threadId,
            "thread.metadata-updated",
            {
              ...beforeRename,
              title: "User renamed this",
            },
            at,
            "rename",
          ),
        ],
      });
      expect(
        yield* sink.write({
          expectedThread: beforeRename,
          events: [
            attachmentEvent(
              work.threadId,
              "thread.metadata-updated",
              {
                ...beforeRename,
                externalSession: { ...beforeRename.externalSession!, state: "following" },
              },
              at,
              "stale-sync",
            ),
          ],
        }),
      ).toEqual([]);
      expect((yield* projection.getThread(work.threadId)).title).toBe("User renamed this");
      for (let i = 0; i < 80; i++)
        files.set(childPath(`large-tree-${i}`), [reply(`large-tree-${i}`)]);
      yield* restarted.sync;
      yield* restarted.sync;
      const lastChild = attachmentThreadId("/claude-work", "session", "large-tree-79");
      expect((yield* projection.getThreadRecords(lastChild, ["messages"])).messages).toHaveLength(
        1,
      );
      const caughtUpReads = readCalls;
      yield* restarted.sync;
      expect(readCalls).toBe(caughtUpReads);
      // Recent messages must not wait for either the root's history cursor or the child batch.
      chunkSize = 1;
      for (let i = 0; i < 10; i++) {
        const record = JSON.parse(reply(`new-root-${i}`));
        record.timestamp = `2026-11-02T12:00:${String(i).padStart(2, "0")}Z`;
        files.get(rootPath)!.push(JSON.stringify(record));
      }
      const revised = JSON.parse(reply("new-root-0"));
      revised.message.content = "Updated live text";
      revised.timestamp = "2026-11-02T12:01:00Z";
      files.get(rootPath)!.push(JSON.stringify(revised));
      for (let i = 0; i < 80; i++)
        for (let j = 0; j < 10; j++)
          files.get(childPath(`large-tree-${i}`))!.push(reply(`new-child-${i}-${j}`));
      yield* restarted.sync;
      expect((yield* projection.getThread(work.threadId)).externalSession?.state).toBe(
        "connecting",
      );
      expect(
        (yield* projection.getThreadRecords(work.threadId, ["messages"])).messages.some((m) =>
          m.id.endsWith(":new-root-9"),
        ),
      ).toBe(true);
      expect(
        (yield* projection.getThreadRecords(work.threadId, ["messages"])).messages.find((m) =>
          m.id.endsWith(":new-root-0"),
        )?.text,
      ).toBe("Updated live text");
      yield* restarted.sync;
      const items = (yield* projection.getThreadRecords(work.threadId, ["turnItems"])).turnItems;
      const oldItem = items.find((item) => item.id.endsWith(":new-root-1:item"))!;
      const recentItem = items.find((item) => item.id.endsWith(":new-root-9:item"))!;
      expect(oldItem).toBeDefined();
      expect(oldItem.ordinal).toBeLessThan(recentItem.ordinal);
      for (let i = 0; i < 80; i++) {
        const id = attachmentThreadId("/claude-work", "session", `large-tree-${i}`);
        expect(
          (yield* projection.getThreadRecords(id, ["messages"])).messages.some((m) =>
            m.id.endsWith(`:new-child-${i}-9`),
          ),
        ).toBe(true);
      }
    }).pipe(Effect.provide(persistence)),
);

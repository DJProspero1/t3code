import * as Crypto from "effect/Crypto";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import { OrchestratorCommandPreviouslyRejectedError } from "../orchestration-v2/Orchestrator.ts";
import { expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
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
import { attachmentEvent, attachmentThreadId } from "./claudeAttachmentProjection.ts";

it.effect.each(["wait", "interrupt", "retry", "rejected", "legacy"] as const)(
  "preserves history and resumes only an interrupted handoff (interrupt: %s)",
  (mode) =>
    Effect.gen(function* () {
      const interrupt = mode !== "wait";
      let continuationUnavailable = mode === "retry";
      let rejectContinuation = mode === "rejected";
      const rejectedCommands = new Set<string>();
      let randomByte = 0;
      let alive = true;
      let interruptions = 0;
      const dispatched: Array<string> = [];
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
          cwd: uuid === "latest" ? "/work/later" : "/work/original",
          timestamp: "2026-01-01T00:00:00Z",
          message: { content: uuid, stop_reason: "end_turn", model: "claude-sonnet-4-6" },
        });
      const files = new Map([
        [root, [record("first"), record("latest")]],
        [child, [record("child-final")]],
      ]);
      const source = Layer.mock(Source.ClaudeAttachmentSource)({
        aliases: () => Effect.succeed([]),
        interrupt: (_home, _sessionId, options) =>
          Effect.sync(() => {
            if (options?.verifyOnly) return;
            interruptions++;
            alive = false;
          }),
        discover: () =>
          Effect.succeed({ sessionId: "session", title: "Session", transcriptPath: root }),
        assertStopped: () =>
          alive
            ? Effect.fail(
                new Source.ClaudeAttachmentSourceError({
                  operation: "handoff",
                  reason: "running",
                  cause: "alive",
                }),
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
        Layer.succeed(
          Crypto.Crypto,
          Crypto.make({
            randomBytes: (size) => new Uint8Array(size).fill(randomByte++),
            digest: (_algorithm, data) => Effect.succeed(data),
          }),
        ),
        Layer.mock(ThreadManagement.ThreadManagementService)({
          dispatch: (command) =>
            Effect.suspend(
              (): ReturnType<ThreadManagement.ThreadManagementService["Service"]["dispatch"]> => {
                expect(command.type).toBe("message.dispatch");
                if (command.type === "message.dispatch") {
                  expect(command.dispatchMode).toEqual({ type: "queue_after_active" });
                  expect(command.createdBy).toBe("agent");
                  expect(command.creationSource).toBe("server");
                }
                if (rejectContinuation || rejectedCommands.has(command.commandId)) {
                  rejectContinuation = false;
                  rejectedCommands.add(command.commandId);
                  return Effect.fail(
                    new OrchestratorCommandPreviouslyRejectedError({
                      commandId: command.commandId,
                      commandType: command.type,
                      detail: "Previously rejected notification",
                    }),
                  );
                }
                if (continuationUnavailable) {
                  continuationUnavailable = false;
                  return Effect.fail(
                    new ThreadManagement.ThreadManagementProviderInstanceDisabledError({
                      projectId,
                      instanceId: ProviderInstanceId.make("claudeAgent"),
                    }),
                  );
                }
                dispatched.push(command.commandId);
                return Effect.succeed({ sequence: 1, storedEvents: [] });
              },
            ),
        }),
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
        expect(
          (yield* service.change({ action: "handoff", threadId: attached.threadId })).blocked,
        ).toBe(true);
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
        alive = interrupt;
        files.delete(child);
        if (mode === "retry" || mode === "rejected") {
          yield* service
            .change({ action: "handoff", threadId: attached.threadId, interrupt })
            .pipe(Effect.flip);
          const pending = yield* projections.getThread(attached.threadId);
          expect(pending.externalSession).toBeUndefined();
          expect(pending.handoffContinuationId).toBeDefined();
          expect(interruptions).toBe(1);
        }
        expect(
          yield* service.change({ action: "handoff", threadId: attached.threadId, interrupt }),
        ).toEqual({
          threadId: attached.threadId,
          attached: false,
          readOnly: false,
        });
        expect(interruptions).toBe(interrupt ? 1 : 0);
        expect(dispatched.length).toBe(interrupt ? 1 : 0);
        const after = yield* projections.getThread(attached.threadId);
        expect(after.handoffContinuationId).toBeUndefined();
        expect(after.externalSession).toBeUndefined();
        expect(after.worktreePath).toBe("/work/later");
        expect(after.modelSelection.model).toBe("claude-sonnet-4-6");
        expect(after.activeProviderThreadId).toBe(before.activeProviderThreadId);
        const records = yield* projections.getThreadRecords(attached.threadId, [
          "messages",
          "providerThreads",
        ]);
        expect(records.messages.map((message) => message.text).sort()).toEqual(["first", "latest"]);
        expect(records.providerThreads[0]?.nativeThreadRef?.nativeId).toBe("session");
        expect(records.providerThreads[0]?.nativeMetadata?.claudeResumeTranscriptPath).toBe(root);
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
        expect(dispatched.length).toBe(interrupt ? 1 : 0);
        if (mode === "legacy") {
          const sink = yield* EventSink.EventSinkV2;
          const now = yield* DateTime.now;
          const binding = records.providerThreads[0]!;
          yield* sink.write({
            events: [
              attachmentEvent(
                after.id,
                "provider-thread.updated",
                {
                  ...binding,
                  nativeMetadata: null,
                },
                now,
                "legacy-binding-fixture",
              ),
              attachmentEvent(
                after.id,
                "thread.metadata-updated",
                {
                  ...after,
                  worktreePath: "/work/later",
                },
                now,
                "legacy-workspace-fixture",
              ),
            ],
          });
          yield* service.change({ action: "handoff", threadId: after.id });
          expect((yield* projections.getThread(after.id)).worktreePath).toBe("/work/later");
          expect(dispatched.length).toBe(2);
          yield* service.change({ action: "handoff", threadId: after.id });
          expect(dispatched.length).toBe(2);
        }
      }).pipe(Effect.provide(Attachment.layer.pipe(Layer.provideMerge(deps))));
    }),
);

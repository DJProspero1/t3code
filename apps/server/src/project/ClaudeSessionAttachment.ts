import {
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_MODEL,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  MessageId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Subagent,
  type ClaudeSessionAttachmentInput,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";
import { forkParked } from "../serverActivation.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectService from "./ProjectService.ts";
import * as Source from "./ClaudeAttachmentSource.ts";
import {
  attachmentEvent,
  attachmentDetails,
  attachmentNodeId,
  attachmentThreadId,
  projectAttachmentLine,
} from "./claudeAttachmentProjection.ts";

export class ClaudeSessionAttachmentError extends Schema.TaggedError<ClaudeSessionAttachmentError>()(
  "ClaudeSessionAttachmentError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "Could not attach the Claude session. Use its exact live name or session ID and an existing T3 project. The Claude process was not changed.";
  }
}
export class ClaudeSessionAttachment extends Context.Service<
  ClaudeSessionAttachment,
  {
    readonly change: (input: ClaudeSessionAttachmentInput) => Effect.Effect<
      {
        threadId: ThreadId;
        attached: boolean;
        readOnly: boolean;
      },
      ClaudeSessionAttachmentError
    >;
    readonly sync: Effect.Effect<void, ClaudeSessionAttachmentError>;
  }
>()("t3/project/ClaudeSessionAttachment") {}

const make = Effect.gen(function* () {
  const source = yield* Source.ClaudeAttachmentSource;
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectService.ProjectService;
  const sql = yield* SqlClient.SqlClient;
  const home = yield* HostProcess.HomeDirectory;
  const environment = yield* HostProcess.Environment;
  const path = yield* Path.Path;
  const configDirectory = path.resolve(environment.CLAUDE_CONFIG_DIR ?? path.join(home, ".claude"));
  const gate = yield* Semaphore.make(1);
  const providerInstanceId = ProviderInstanceId.make("claudeAgent");

  const create = Effect.fn("ClaudeSessionAttachment.create")(function* (
    parent: OrchestrationV2AppThread | null,
    data: {
      sessionId: string;
      transcriptPath: string;
      title: string;
      projectId: OrchestrationV2AppThread["projectId"];
      agentId?: string;
      confirmedParent?: boolean;
      model?: string;
    },
  ) {
    const threadId = attachmentThreadId(configDirectory, data.sessionId, data.agentId);
    const existing = yield* projections.getThread(threadId).pipe(Effect.option);
    if (Option.isSome(existing)) {
      if (
        existing.value.projectId !== data.projectId ||
        !existing.value.externalSession ||
        existing.value.deletedAt !== null
      ) {
        return yield* new ClaudeSessionAttachmentError({
          cause: "Session already belongs to another project or was deleted",
        });
      }
      if (
        parent &&
        data.confirmedParent &&
        parent.id !== threadId &&
        (existing.value.lineage.parentThreadId !== parent.id || existing.value.title !== data.title)
      ) {
        let ancestor: OrchestrationV2AppThread | null = parent;
        for (let depth = 0; ancestor && depth < 128; depth++) {
          if (ancestor.id === threadId)
            return yield* new ClaudeSessionAttachmentError({ cause: "Cyclic subagent ancestry" });
          ancestor = ancestor.lineage.parentThreadId
            ? yield* projections.getThread(ancestor.lineage.parentThreadId)
            : null;
        }
        if (ancestor)
          return yield* new ClaudeSessionAttachmentError({ cause: "Subagent ancestry too deep" });
        const now = yield* DateTime.now;
        const updated = {
          ...existing.value,
          title: data.title,
          lineage: { ...existing.value.lineage, parentThreadId: parent.id },
        };
        const related = yield* projections.getThreadRecords(
          parent.lineage.rootThreadId,
          ["subagents", "nodes"],
          {
            subagentIds: [attachmentNodeId(threadId)],
            nodeIds: [attachmentNodeId(threadId)],
          },
        );
        yield* sink.write({
          skipExistingEventIds: true,
          events: [
            ...related.subagents.map((item) =>
              attachmentEvent(
                item.threadId,
                "subagent.updated",
                {
                  ...item,
                  parentNodeId: attachmentNodeId(parent.id),
                  title: data.title,
                  updatedAt: now,
                },
                now,
                `${threadId}:${parent.id}:${data.title}`,
              ),
            ),
            ...related.nodes.map((item) =>
              attachmentEvent(
                item.threadId,
                "node.updated",
                {
                  ...item,
                  parentNodeId: attachmentNodeId(parent.id),
                },
                now,
                `${threadId}:${parent.id}`,
              ),
            ),
            attachmentEvent(
              threadId,
              "thread.metadata-updated",
              updated,
              now,
              `${parent.id}:${data.title}`,
            ),
          ],
        });
        return updated;
      }
      return existing.value;
    }
    const now = yield* DateTime.now;
    const thread: OrchestrationV2AppThread = {
      id: threadId,
      projectId: data.projectId,
      title: data.title,
      createdBy: "agent",
      creationSource: "provider",
      providerInstanceId,
      modelSelection: {
        instanceId: providerInstanceId,
        model:
          data.model ??
          DEFAULT_MODEL_BY_PROVIDER[ProviderDriverKind.make("claudeAgent")] ??
          DEFAULT_MODEL,
      },
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: parent ? null : ProviderThreadId.make(`${threadId}:provider`),
      externalSession: {
        provider: "claude",
        sessionId: data.sessionId,
        configDirectory,
        transcriptPath: data.transcriptPath,
        attached: true,
        projectionVersion: 2,
        state: "connecting",
      },
      lineage: {
        parentThreadId: parent?.id ?? null,
        rootThreadId: parent?.lineage.rootThreadId ?? threadId,
        relationshipToParent: parent ? "subagent" : null,
      },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: "active",
      settledAt: null,
      unsettledAt: now,
      snoozedUntil: null,
      snoozedAt: null,
      pinnedAt: null,
      pinOrderKey: null,
      activeOrderKey: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    const binding: OrchestrationV2ProviderThread = {
      id: ProviderThreadId.make(`${threadId}:provider`),
      driver: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        nativeId: data.sessionId,
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      createdAt: now,
      updatedAt: now,
    };
    const committed = yield* sink.write({
      skipExistingEventIds: true,
      ...(parent ? {} : { unlessNativeThreadOwned: binding }),
      events: [
        attachmentEvent(threadId, "thread.created", thread, now, threadId),
        ...(parent
          ? []
          : [attachmentEvent(threadId, "provider-thread.updated", binding, now, threadId)]),
      ],
    });
    if (committed.length === 0)
      return yield* new ClaudeSessionAttachmentError({
        cause: "Another T3 thread already owns this session",
      });
    return thread;
  });

  const setState = Effect.fn("ClaudeSessionAttachment.setState")(function* (
    threadId: ThreadId,
    state: NonNullable<OrchestrationV2AppThread["externalSession"]>["state"],
    cursor?: Source.TranscriptCursor,
    model?: string,
  ) {
    const thread = yield* projections.getThread(threadId);
    const externalSession = thread.externalSession;
    if (
      !externalSession ||
      thread.deletedAt !== null ||
      (externalSession.state === state &&
        (!cursor ||
          (externalSession.projectionVersion === 2 &&
            cursor.offset === externalSession.cursor?.offset &&
            cursor.identity === externalSession.cursor?.identity)) &&
        (!model || model === thread.modelSelection.model))
    )
      return;
    const now = yield* DateTime.now;
    yield* sink.write({
      skipExistingEventIds: true,
      events: [
        attachmentEvent(
          threadId,
          "thread.metadata-updated",
          {
            ...thread,
            ...(model ? { modelSelection: { ...thread.modelSelection, model } } : {}),
            externalSession: {
              ...externalSession,
              attached: state !== "detached",
              state,
              ...(cursor ? { cursor, projectionVersion: 2 } : {}),
            },
            updatedAt: now,
          },
          now,
          `${state}:${externalSession.state}:${externalSession.projectionVersion ?? 0}:2:${cursor?.identity ?? ""}:${cursor?.offset ?? -1}:${model ?? ""}:${DateTime.toEpochMillis(now)}`,
        ),
      ],
    });
  });

  const followFile = Effect.fn("ClaudeSessionAttachment.followFile")(function* (
    thread: OrchestrationV2AppThread,
    parent: OrchestrationV2AppThread | null,
    agentId: string | null,
  ) {
    const external = thread.externalSession!;
    const old =
      external.projectionVersion === 2 ? (external.cursor ?? { offset: 0 }) : { offset: 0 };
    const read = yield* source.read(external.configDirectory, external.transcriptPath, old);
    let model: string | undefined;
    for (const line of read.lines) {
      const details = attachmentDetails(line, external.sessionId);
      if (details?.model) model = details.model;
      if (details?.spawn) {
        const root = parent ?? thread;
        yield* create(thread, {
          projectId: thread.projectId,
          sessionId: external.sessionId,
          agentId: details.spawn.agentId,
          transcriptPath: path.join(
            root.externalSession!.transcriptPath.slice(0, -6),
            "subagents",
            `agent-${details.spawn.agentId}.jsonl`,
          ),
          title: details.spawn.description?.trim() || `Claude subagent ${details.spawn.agentId}`,
          confirmedParent: true,
          ...(details.spawn.resolvedModel ? { model: details.spawn.resolvedModel } : {}),
        });
      }
    }
    let previousSubagent: OrchestrationV2Subagent | undefined = parent
      ? (yield* projections.getThreadRecords(parent.id, ["subagents"], {
          subagentIds: [attachmentNodeId(thread.id)],
        })).subagents[0]
      : undefined;
    let ordinal = old.offset;
    const events = (yield* Effect.forEach(
      read.lines,
      Effect.fnUntraced(function* (line) {
        ordinal += Buffer.byteLength(line) + 1;
        const details = attachmentDetails(line, external.sessionId);
        const existingMessage =
          details?.isPeer && details.uuid
            ? (yield* projections.getThreadRecords(thread.id, ["messages"], {
                messageIds: [
                  MessageId.make(details.uuid),
                  MessageId.make(`${thread.id}:${details.uuid}`),
                ],
              })).messages[0]
            : undefined;
        const projected = projectAttachmentLine({
          sentMessage: existingMessage?.creationSource !== "provider" ? existingMessage : undefined,
          thread,
          parent,
          agentId,
          line,
          ordinal,
          previousSubagent,
          includeActivity: false,
        });
        const child = projected.find((event) => event.type === "subagent.updated");
        if (child?.type === "subagent.updated") previousSubagent = child.payload;
        return projected;
      }),
    )).flat();
    for (let i = 0; i < events.length; i += 100)
      yield* sink.write({ skipExistingEventIds: true, events: events.slice(i, i + 100) });
    yield* setState(thread.id, read.caughtUp ? "following" : "connecting", read.cursor, model);
    return read.caughtUp;
  });

  const activitySnapshots = new Map<string, string>();
  const refreshActivity = Effect.fn("ClaudeSessionAttachment.refreshActivity")(function* (
    thread: OrchestrationV2AppThread,
    parent: OrchestrationV2AppThread | null,
    agentId: string | null,
  ) {
    const external = thread.externalSession!;
    const tail = yield* source.tail(external.configDirectory, external.transcriptPath);
    const now = yield* DateTime.now;
    const activityStatus =
      DateTime.toEpochMillis(now) - tail.modifiedAt < 120_000
        ? ("running" as const)
        : ("idle" as const);
    const stamp = `${tail.size}:${tail.modifiedAt}:${activityStatus}:${thread.lineage.parentThreadId}`;
    if (activitySnapshots.get(thread.id) === stamp) return;
    const previousSubagent = parent
      ? (yield* projections.getThreadRecords(parent.id, ["subagents"], {
          subagentIds: [attachmentNodeId(thread.id)],
        })).subagents[0]
      : undefined;
    for (const line of [...tail.lines].reverse()) {
      const events = projectAttachmentLine({
        thread,
        parent,
        agentId,
        line,
        ordinal: tail.size,
        previousSubagent,
        activityStatus,
        activityKey: stamp,
      }).filter((event) => event.type === "node.updated" || event.type === "subagent.updated");
      if (events.length) {
        yield* sink.write({ skipExistingEventIds: true, events });
        break;
      }
    }
    activitySnapshots.set(thread.id, stamp);
  });
  const childOffsets = new Map<string, number>();
  const sync = Effect.gen(function* () {
    const rows = yield* sql<{ thread_id: string }>`
      SELECT thread_id FROM orchestration_v2_projection_threads
      WHERE json_extract(payload_json, '$.externalSession.attached') = 1
        AND json_extract(payload_json, '$.lineage.parentThreadId') IS NULL
        AND json_extract(payload_json, '$.deletedAt') IS NULL
    `;
    for (const row of rows) {
      const thread = yield* projections.getThread(ThreadId.make(row.thread_id));
      const external = thread.externalSession!;
      yield* Effect.gen(function* () {
        // Prioritize root history, including one-time reclassification of older imports.
        for (let pass = 0; pass < 8; pass++) {
          const current = yield* projections.getThread(thread.id);
          if (yield* followFile(current, null, null)) break;
        }
        yield* refreshActivity(thread, null, null);
        const children = yield* source.children(external.configDirectory, external.transcriptPath);
        // Bound each pass so a large agent tree cannot starve detach or other sessions.
        const start = childOffsets.get(thread.id) ?? 0;
        const batch = Array.from(
          { length: Math.min(16, children.length) },
          (_, i) => children[(start + i) % children.length]!,
        );
        childOffsets.set(thread.id, children.length ? (start + batch.length) % children.length : 0);
        for (const child of children) {
          yield* Effect.gen(function* () {
            const childThread = yield* create(thread, {
              sessionId: external.sessionId,
              transcriptPath: child.path,
              title: `Claude subagent ${child.agentId}`,
              projectId: thread.projectId,
              agentId: child.agentId,
            });
            if (batch.includes(child)) yield* followFile(childThread, thread, child.agentId);
            yield* refreshActivity(childThread, thread, child.agentId);
          }).pipe(Effect.catch(() => setState(thread.id, "unavailable")));
        }
      }).pipe(Effect.catch(() => setState(thread.id, "unavailable")));
    }
  }).pipe(
    gate.withPermit,
    Effect.mapError((cause) => new ClaudeSessionAttachmentError({ cause })),
  );

  const change = Effect.fn("ClaudeSessionAttachment.change")(
    function* (input: ClaudeSessionAttachmentInput) {
      if (input.action === "detach") {
        const thread = yield* projections.getThread(input.threadId);
        if (!thread.externalSession || thread.lineage.parentThreadId !== null) {
          return yield* new ClaudeSessionAttachmentError({
            cause: "Detach the root attached session",
          });
        }
        yield* setState(input.threadId, "detached");
        const descendants = yield* sql<{ thread_id: string }>`
          SELECT thread_id FROM orchestration_v2_projection_threads
          WHERE json_extract(payload_json, '$.lineage.rootThreadId') = ${input.threadId}
            AND json_extract(payload_json, '$.externalSession.attached') = 1
        `;
        for (const child of descendants)
          yield* setState(ThreadId.make(child.thread_id), "detached");
        return { threadId: input.threadId, attached: false, readOnly: true as const };
      }
      const project = yield* projects.getById(input.projectId);
      if (Option.isNone(project))
        return yield* new ClaudeSessionAttachmentError({ cause: "Project does not exist" });
      const discovered = yield* source.discover(configDirectory, input.session);
      const thread = yield* create(null, { ...discovered, projectId: input.projectId });
      yield* setState(thread.id, "connecting");
      return { threadId: thread.id, attached: true, readOnly: false };
    },
    gate.withPermit,
    Effect.mapError((cause) => new ClaudeSessionAttachmentError({ cause })),
  );
  yield* sync.pipe(Effect.ignore(), Effect.repeat(Schedule.spaced("2 seconds")), forkParked);
  return ClaudeSessionAttachment.of({ change, sync });
});
export const layer = Layer.effect(ClaudeSessionAttachment, make);

import {
  ClaudeSettings,
  resolveProviderInstanceEnabled,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_MODEL,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  MessageId,
  TurnItemId,
  type OrchestrationV2TurnItem,
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
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";
import { forkParked } from "../serverActivation.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { resolveClaudeHomePath } from "../provider/Drivers/ClaudeHome.ts";
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
  {
    cause: Schema.Defect(),
    reason: Schema.optional(Schema.Literals(["unsupported-provider", "unavailable-instance"])),
  },
) {
  override get message() {
    if (this.reason === "unsupported-provider")
      return "Live attachment is not implemented for this provider. The existing session was not changed.";
    if (this.reason === "unavailable-instance")
      return "Select an enabled provider instance configured in this T3 environment.";
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

const isAttachmentError = Schema.is(ClaudeSessionAttachmentError);

const decodeClaudeSettings = Schema.decodeUnknownEffect(ClaudeSettings);

const make = Effect.gen(function* () {
  const source = yield* Source.ClaudeAttachmentSource;
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectService.ProjectService;
  const sql = yield* SqlClient.SqlClient;
  const environment = yield* HostProcess.Environment;
  const home = yield* HostProcess.HomeDirectory;
  const platform = yield* HostProcess.Platform;
  const path = yield* Path.Path;
  const settings = yield* ServerSettings.ServerSettingsService;
  const gate = yield* Semaphore.make(1);
  const syncedFiles = new Map<string, string>();

  const create = Effect.fn("ClaudeSessionAttachment.create")(function* (
    parent: OrchestrationV2AppThread | null,
    data: {
      configDirectory: string;
      providerInstanceId: ProviderInstanceId;
      sessionId: string;
      transcriptPath: string;
      title: string;
      projectId: OrchestrationV2AppThread["projectId"];
      agentId?: string;
      confirmedParent?: boolean;
      model?: string;
    },
  ) {
    const { configDirectory, providerInstanceId } = data;
    const threadId = attachmentThreadId(configDirectory, data.sessionId, data.agentId);
    const existing = yield* projections.getThread(threadId).pipe(Effect.option);
    if (Option.isSome(existing)) {
      if (
        existing.value.projectId !== data.projectId ||
        existing.value.providerInstanceId !== providerInstanceId ||
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
        syncedFiles.delete(threadId);
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
        const committed = yield* sink.write({
          expectedThread: existing.value,
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
        return committed.length ? updated : yield* projections.getThread(threadId);
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
        projectionVersion: 4,
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
    if (state === "connecting" || state === "detached") syncedFiles.delete(threadId);
    const thread = yield* projections.getThread(threadId);
    const externalSession = thread.externalSession;
    if (
      !externalSession ||
      thread.deletedAt !== null ||
      (!externalSession.attached && state !== "connecting" && state !== "detached") ||
      (externalSession.state === state &&
        (!cursor ||
          (externalSession.projectionVersion === 4 &&
            cursor.offset === externalSession.cursor?.offset &&
            cursor.identity === externalSession.cursor?.identity)) &&
        (!model || model === thread.modelSelection.model))
    )
      return;
    const now = yield* DateTime.now;
    const committed = yield* sink.write({
      expectedThread: thread,
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
              ...(cursor ? { cursor, projectionVersion: 4 } : {}),
            },
            updatedAt: now,
          },
          now,
          `${state}:${externalSession.state}:${externalSession.projectionVersion ?? 0}:4:${cursor?.identity ?? ""}:${cursor?.offset ?? -1}:${model ?? ""}:${DateTime.toEpochMillis(now)}`,
        ),
      ],
    });
    if (!committed.length)
      return yield* new ClaudeSessionAttachmentError({
        cause: "Thread changed during attachment update; retry",
      });
  });

  const liveRecords = new Map<
    string,
    { identity?: string; size: number; offsets: Map<string, number> }
  >();
  const projectLines = Effect.fn("ClaudeSessionAttachment.projectLines")(function* (
    thread: OrchestrationV2AppThread,
    parent: OrchestrationV2AppThread | null,
    agentId: string | null,
    lines: ReadonlyArray<string>,
    offset: number,
    live = false,
  ) {
    const external = thread.externalSession!;
    let model: string | undefined;
    for (const line of lines) {
      const details = attachmentDetails(line, external.sessionId);
      if (details?.model) model = details.model;
      if (details?.spawn) {
        const root = parent ?? thread;
        yield* create(thread, {
          projectId: thread.projectId,
          configDirectory: external.configDirectory,
          providerInstanceId: thread.providerInstanceId,
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
    let ordinal = offset;
    const tools = new Map<string, OrchestrationV2TurnItem>();
    const events = (yield* Effect.forEach(
      lines,
      Effect.fnUntraced(function* (line) {
        ordinal += Buffer.byteLength(line) + 1;
        const details = attachmentDetails(line, external.sessionId);
        const offsets = liveRecords.get(thread.id)?.offsets;
        if (details?.uuid) {
          if (live) offsets?.set(details.uuid, ordinal);
          else if ((offsets?.get(details.uuid) ?? -1) > ordinal) return [];
        }
        const existingMessage =
          details?.isPeer && details.uuid
            ? (yield* projections.getThreadRecords(thread.id, ["messages"], {
                messageIds: [
                  MessageId.make(details.uuid),
                  MessageId.make(`${thread.id}:${details.uuid}`),
                ],
              })).messages[0]
            : undefined;
        for (const toolId of details?.toolIds ?? []) {
          if (tools.has(toolId)) continue;
          const previous = yield* projections.getTurnItem({
            threadId: thread.id,
            itemId: TurnItemId.make(`${thread.id}:tool:${toolId}`),
          });
          if (previous) tools.set(toolId, previous);
        }
        const projected = projectAttachmentLine({
          previousTools: tools,
          sentMessage: existingMessage?.creationSource !== "provider" ? existingMessage : undefined,
          thread,
          parent,
          agentId,
          line,
          ordinal,
          previousSubagent,
          includeActivity: false,
        });
        for (const event of projected) {
          if (
            event.type === "turn-item.updated" &&
            event.payload.type === "dynamic_tool" &&
            event.payload.nativeItemRef?.nativeId
          )
            tools.set(event.payload.nativeItemRef.nativeId, event.payload);
        }
        const child = projected.find((event) => event.type === "subagent.updated");
        if (child?.type === "subagent.updated") previousSubagent = child.payload;
        return projected;
      }),
    )).flat();
    for (let i = 0; i < events.length; i += 100)
      yield* sink.write({ skipExistingEventIds: true, events: events.slice(i, i + 100) });
    return model;
  });

  const followFile = Effect.fn("ClaudeSessionAttachment.followFile")(function* (
    thread: OrchestrationV2AppThread,
    parent: OrchestrationV2AppThread | null,
    agentId: string | null,
  ) {
    const external = thread.externalSession!;
    const old =
      external.projectionVersion === 4 ? (external.cursor ?? { offset: 0 }) : { offset: 0 };
    const read = yield* source.read(external.configDirectory, external.transcriptPath, old);
    const offset = Math.max(
      0,
      read.cursor.offset -
        read.lines.reduce((bytes, line) => bytes + Buffer.byteLength(line) + 1, 0),
    );
    const model = yield* projectLines(thread, parent, agentId, read.lines, offset);
    yield* setState(thread.id, read.caughtUp ? "following" : "connecting", read.cursor, model);
    const offsets = liveRecords.get(thread.id)?.offsets;
    if (offsets)
      for (const [id, position] of offsets) if (position <= read.cursor.offset) offsets.delete(id);
    return read.caughtUp;
  });

  const activitySnapshots = new Map<string, string>();
  type Tail = {
    lines: ReadonlyArray<string>;
    offset: number;
    size: number;
    modifiedAt: number;
    identity?: string;
  };
  const refreshActivity = Effect.fn("ClaudeSessionAttachment.refreshActivity")(function* (
    thread: OrchestrationV2AppThread,
    parent: OrchestrationV2AppThread | null,
    agentId: string | null,
    snapshot?: Tail,
  ) {
    const external = thread.externalSession!;
    const tail =
      snapshot ?? (yield* source.tail(external.configDirectory, external.transcriptPath));
    const now = yield* DateTime.now;
    const activityStatus =
      DateTime.toEpochMillis(now) - tail.modifiedAt < 120_000
        ? ("running" as const)
        : ("idle" as const);
    const stamp = `${tail.identity ?? ""}:${tail.size}:${tail.modifiedAt}:${activityStatus}:${thread.lineage.parentThreadId}`;
    if (activitySnapshots.get(thread.id) === stamp) return false;
    // Publish current messages before historical backfill, without advancing its cursor.
    const previousLive = liveRecords.get(thread.id);
    if (!previousLive || previousLive.identity !== tail.identity || previousLive.size > tail.size)
      liveRecords.set(thread.id, {
        ...(tail.identity ? { identity: tail.identity } : {}),
        size: tail.size,
        offsets: new Map(),
      });
    else previousLive.size = tail.size;
    yield* projectLines(thread, parent, agentId, tail.lines, tail.offset, true);
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
    return true;
  });
  const childOffsets = new Map<string, number>();
  const syncGate = yield* Semaphore.make(1);
  let backfillPending = false;
  const sync = Effect.gen(function* () {
    backfillPending = false;
    const rows = yield* sql<{ thread_id: string }>`
      SELECT thread_id FROM orchestration_v2_projection_threads
      WHERE json_extract(payload_json, '$.externalSession.attached') = 1
        AND json_extract(payload_json, '$.lineage.parentThreadId') IS NULL
        AND json_extract(payload_json, '$.deletedAt') IS NULL
    `;
    const activeRoots = new Set(rows.map((row) => row.thread_id));
    for (const rootId of childOffsets.keys()) {
      if (!activeRoots.has(rootId)) childOffsets.delete(rootId);
    }
    if (!rows.length) {
      activitySnapshots.clear();
      syncedFiles.clear();
      liveRecords.clear();
    }
    for (const row of rows) {
      const thread = yield* projections.getThread(ThreadId.make(row.thread_id));
      const external = thread.externalSession!;
      yield* Effect.gen(function* () {
        yield* Effect.gen(function* () {
          const current = yield* projections.getThread(thread.id);
          if (!current.externalSession?.attached || current.deletedAt !== null) return;
          const changed = yield* refreshActivity(current, null, null);
          if (
            (changed || current.externalSession.state !== "following") &&
            !(yield* followFile(current, null, null))
          )
            backfillPending = true;
        }).pipe(gate.withPermit);
        const children = yield* source.children(external.configDirectory, external.transcriptPath);
        // Bound each pass so a large agent tree cannot starve detach or other sessions.
        const start = childOffsets.get(thread.id) ?? 0;
        const batch = Array.from(
          { length: Math.min(64, children.length) },
          (_, i) => children[(start + i) % children.length]!,
        );
        childOffsets.set(thread.id, children.length ? (start + batch.length) % children.length : 0);
        for (const child of children) {
          yield* Effect.gen(function* () {
            const childId = attachmentThreadId(
              external.configDirectory,
              external.sessionId,
              child.agentId,
            );
            const tail = yield* source.tail(external.configDirectory, child.path);
            const now = yield* DateTime.now;
            const stamp = `${tail.identity ?? ""}:${tail.size}:${tail.modifiedAt}:${DateTime.toEpochMillis(now) - tail.modifiedAt < 120_000}`;
            if (syncedFiles.get(childId) === stamp) return;
            const currentRoot = yield* projections.getThread(thread.id);
            if (!currentRoot.externalSession?.attached || currentRoot.deletedAt !== null) return;
            const childThread = yield* create(thread, {
              configDirectory: external.configDirectory,
              providerInstanceId: thread.providerInstanceId,
              sessionId: external.sessionId,
              transcriptPath: child.path,
              title: `Claude subagent ${child.agentId}`,
              projectId: thread.projectId,
              agentId: child.agentId,
            });
            const changed = yield* refreshActivity(childThread, thread, child.agentId, tail);
            if (batch.includes(child)) {
              const caughtUp =
                !changed && childThread.externalSession?.state === "following"
                  ? true
                  : yield* followFile(childThread, thread, child.agentId);
              if (caughtUp) syncedFiles.set(childId, stamp);
              else backfillPending = true;
            } else {
              yield* setState(childThread.id, "connecting");
              backfillPending = true;
            }
          }).pipe(
            gate.withPermit,
            Effect.catch(() => setState(thread.id, "unavailable")),
          );
        }
      }).pipe(Effect.catch(() => setState(thread.id, "unavailable")));
    }
  }).pipe(
    syncGate.withPermit,
    Effect.mapError((cause) =>
      isAttachmentError(cause) ? cause : new ClaudeSessionAttachmentError({ cause }),
    ),
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
      const providerInstanceId = input.providerInstanceId ?? ProviderInstanceId.make("claudeAgent");
      const configured = (yield* settings.getSettings).providerInstances[providerInstanceId];
      if (configured && configured.driver !== "claudeAgent")
        return yield* new ClaudeSessionAttachmentError({
          reason: "unsupported-provider",
          cause: "Provider has no live attachment adapter",
        });
      if (
        (!configured && providerInstanceId !== "claudeAgent") ||
        (configured && !resolveProviderInstanceEnabled(configured))
      )
        return yield* new ClaudeSessionAttachmentError({
          reason: "unavailable-instance",
          cause: "Provider instance is missing or disabled",
        });
      const config = yield* decodeClaudeSettings(configured?.config ?? {});
      const instanceEnvironment = yield* mergeProviderInstanceEnvironment(
        configured?.environment,
        environment,
      ).pipe(
        Effect.provideService(HostProcess.HomeDirectory, home),
        Effect.provideService(HostProcess.Platform, platform),
      );
      const configDirectory = yield* resolveClaudeHomePath(config, instanceEnvironment).pipe(
        Effect.provideService(Path.Path, path),
        Effect.provideService(HostProcess.HomeDirectory, home),
      );
      const discovered = yield* source.discover(configDirectory, input.session);
      const thread = yield* create(null, {
        ...discovered,
        configDirectory,
        providerInstanceId,
        projectId: input.projectId,
      });
      yield* setState(thread.id, "connecting");
      return { threadId: thread.id, attached: true, readOnly: false };
    },
    gate.withPermit,
    Effect.mapError((cause) =>
      isAttachmentError(cause) ? cause : new ClaudeSessionAttachmentError({ cause }),
    ),
  );
  yield* Effect.gen(function* () {
    yield* sync.pipe(Effect.ignore());
    yield* Effect.sleep(backfillPending ? "10 millis" : "2 seconds");
  }).pipe(Effect.forever, forkParked);
  return ClaudeSessionAttachment.of({ change, sync });
});
export const layer = Layer.effect(ClaudeSessionAttachment, make);

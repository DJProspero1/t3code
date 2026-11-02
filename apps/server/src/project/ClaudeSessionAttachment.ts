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
import * as EventStore from "../orchestration-v2/EventStore.ts";
import { claudeLiveMessageEventId, claudeLiveAliasMessageId } from "./claudeLiveProjection.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { resolveClaudeHomePath } from "../provider/Drivers/ClaudeHome.ts";
import * as ProjectService from "./ProjectService.ts";
import * as Source from "./ClaudeAttachmentSource.ts";
import {
  attachmentEvent,
  attachmentDetails,
  attachmentReasoningId,
  attachmentNodeId,
  attachmentThreadId,
  projectAttachmentLine,
} from "./claudeAttachmentProjection.ts";

const PROJECTION_VERSION = 5;

const isSourceError = Schema.is(Source.ClaudeAttachmentSourceError);

export class ClaudeSessionAttachmentError extends Schema.TaggedError<ClaudeSessionAttachmentError>()(
  "ClaudeSessionAttachmentError",
  {
    cause: Schema.Defect(),
    reason: Schema.optional(
      Schema.Literals(["unsupported-provider", "unavailable-instance", "handoff-blocked"]),
    ),
  },
) {
  override get message() {
    if (isSourceError(this.cause)) return this.cause.message;
    if (this.reason === "handoff-blocked" && typeof this.cause === "string") return this.cause;
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
  const eventStore = yield* EventStore.EventStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectService.ProjectService;
  const sql = yield* SqlClient.SqlClient;
  const environment = yield* HostProcess.Environment;
  const home = yield* HostProcess.HomeDirectory;
  const platform = yield* HostProcess.Platform;
  const path = yield* Path.Path;
  const settings = yield* ServerSettings.ServerSettingsService;
  const gate = yield* Semaphore.make(1);
  const handoffGate = yield* Semaphore.make(1);
  const syncedFiles = new Map<string, string>();

  const resolveConfigDirectory = Effect.fn("ClaudeSessionAttachment.resolveConfigDirectory")(
    function* (providerInstanceId: ProviderInstanceId) {
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
      return yield* resolveClaudeHomePath(config, instanceEnvironment).pipe(
        Effect.provideService(Path.Path, path),
        Effect.provideService(HostProcess.HomeDirectory, home),
      );
    },
  );

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
        projectionVersion: PROJECTION_VERSION,
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
          (externalSession.projectionVersion === PROJECTION_VERSION &&
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
              ...(cursor ? { cursor, projectionVersion: PROJECTION_VERSION } : {}),
            },
            updatedAt: now,
          },
          now,
          `${state}:${externalSession.state}:${externalSession.projectionVersion ?? 0}:${PROJECTION_VERSION}:${cursor?.identity ?? ""}:${cursor?.offset ?? -1}:${model ?? ""}:${DateTime.toEpochMillis(now)}`,
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
    const detailsByLine = lines.map((line) => attachmentDetails(line, external.sessionId));
    const aliases = new Map(
      (yield* eventStore.readByIds(
        detailsByLine.flatMap((details) =>
          details?.uuid ? [claudeLiveMessageEventId(thread.id, details.uuid)] : [],
        ),
      )).flatMap((row) =>
        row.event.type === "message.updated" ? [[row.event.id, row.event.payload.id] as const] : [],
      ),
    );
    const journal = yield* source.aliases(
      external.configDirectory,
      external.sessionId,
      detailsByLine.flatMap((details) =>
        details?.uuid && !aliases.has(claudeLiveMessageEventId(thread.id, details.uuid))
          ? [details.uuid]
          : [],
      ),
    );
    for (const alias of journal) {
      if (alias.agentId !== agentId) continue;
      const messageId = claudeLiveAliasMessageId(thread.id, alias.epoch, alias);
      if (messageId) aliases.set(claudeLiveMessageEventId(thread.id, alias.uuid), messageId);
    }
    const candidateIds = [
      ...new Set(
        detailsByLine.flatMap((details) => {
          if (!details?.uuid) return [];
          const alias = aliases.get(claudeLiveMessageEventId(thread.id, details.uuid));
          return [
            ...(alias ? [alias] : []),
            ...(details.isPeer
              ? [MessageId.make(details.uuid), MessageId.make(`${thread.id}:${details.uuid}`)]
              : []),
          ];
        }),
      ),
    ];
    const messages = new Map(
      (candidateIds.length === 0
        ? []
        : (yield* projections.getThreadRecords(thread.id, ["messages"], {
            messageIds: candidateIds,
          })).messages
      ).map((message) => [message.id, message]),
    );
    const reasoningIds = detailsByLine.flatMap((details) =>
      details?.uuid
        ? details.thinkingBlocks.map((index) =>
            attachmentReasoningId(
              thread.id,
              details.uuid!,
              index,
              aliases.get(claudeLiveMessageEventId(thread.id, details.uuid!)),
            ),
          )
        : [],
    );
    const reasoning = new Map(
      (reasoningIds.length === 0
        ? []
        : (yield* projections.getThreadRecords(thread.id, ["turnItems"], {
            turnItemIds: reasoningIds,
          })).turnItems
      ).map((item) => [item.id, item]),
    );
    let ordinal = offset;
    let lineIndex = 0;
    const tools = new Map<string, OrchestrationV2TurnItem>();
    const events = (yield* Effect.forEach(
      lines,
      Effect.fnUntraced(function* (line) {
        ordinal += Buffer.byteLength(line) + 1;
        const details = detailsByLine[lineIndex++];
        const offsets = liveRecords.get(thread.id)?.offsets;
        if (details?.uuid) {
          if (live) offsets?.set(details.uuid, ordinal);
          else if ((offsets?.get(details.uuid) ?? -1) > ordinal) return [];
        }
        const alias = details?.uuid
          ? aliases.get(claudeLiveMessageEventId(thread.id, details.uuid))
          : undefined;
        const existingMessage = alias
          ? messages.get(alias)
          : details?.isPeer && details.uuid
            ? (messages.get(MessageId.make(details.uuid)) ??
              messages.get(MessageId.make(`${thread.id}:${details.uuid}`)))
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
          previousReasoning: reasoning,
          ...(alias ? { messageId: alias } : {}),
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
          if (event.type === "turn-item.updated" && event.payload.type === "reasoning")
            reasoning.set(event.payload.id, event.payload);
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
      external.projectionVersion === PROJECTION_VERSION
        ? (external.cursor ?? { offset: 0 })
        : { offset: 0 };
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
      if (input.action === "handoff") {
        const initial = yield* projections.getThread(input.threadId);
        const external = initial.externalSession;
        if (!external && initial.lineage.parentThreadId === null && initial.deletedAt === null)
          return { threadId: initial.id, attached: false, readOnly: false };
        if (!external || initial.lineage.parentThreadId !== null || initial.deletedAt !== null)
          return yield* new ClaudeSessionAttachmentError({
            reason: "handoff-blocked",
            cause: "Handoff requires an attached root thread",
          });
        const configDirectory = yield* resolveConfigDirectory(initial.providerInstanceId);
        if (path.resolve(configDirectory) !== path.resolve(external.configDirectory))
          return yield* new ClaudeSessionAttachmentError({
            reason: "handoff-blocked",
            cause:
              "The provider account changed since attachment. Restore its original configuration before handoff.",
          });
        yield* source.assertStopped(external.configDirectory, external.sessionId);
        // Block new inbox sends before draining history; failed handoffs remain detached for retry.
        yield* setState(initial.id, "detached").pipe(gate.withPermit);
        const pending = yield* sql<{ effect_id: string }>`
          SELECT effect_id FROM orchestration_v2_effect_outbox
          WHERE thread_id = ${initial.id} AND effect_type = 'external-session.send'
            AND status IN ('pending', 'running') LIMIT 1
        `;
        if (pending.length)
          return yield* new ClaudeSessionAttachmentError({
            reason: "handoff-blocked",
            cause: "Wait for pending inbox deliveries to settle before retrying handoff",
          });
        let cwd: string | undefined;
        const firstRecords = yield* source.read(external.configDirectory, external.transcriptPath, {
          offset: 0,
        });
        for (const line of firstRecords.lines)
          cwd = attachmentDetails(line, external.sessionId)?.cwd ?? cwd;
        const importRemaining = Effect.fn("ClaudeSessionAttachment.importHandoffHistory")(
          function* (
            thread: OrchestrationV2AppThread,
            parent: OrchestrationV2AppThread | null,
            agentId: string | null,
          ) {
            const binding = thread.externalSession!;
            liveRecords.delete(thread.id);
            let cursor =
              binding.projectionVersion === PROJECTION_VERSION
                ? (binding.cursor ?? { offset: 0 })
                : { offset: 0 };
            let model: string | undefined;
            while (true) {
              const read = yield* source.read(
                binding.configDirectory,
                binding.transcriptPath,
                cursor,
              );
              const start =
                read.cursor.offset -
                read.lines.reduce((bytes, line) => bytes + Buffer.byteLength(line) + 1, 0);
              model = (yield* projectLines(thread, parent, agentId, read.lines, start)) ?? model;
              if (parent === null)
                for (const line of read.lines)
                  cwd = attachmentDetails(line, binding.sessionId)?.cwd ?? cwd;
              cursor = read.cursor;
              if (read.caughtUp) break;
              yield* Effect.yieldNow;
            }
            const tail = yield* source.tail(binding.configDirectory, binding.transcriptPath);
            if (
              cursor.offset !== tail.size ||
              (cursor.identity && cursor.identity !== tail.identity)
            )
              return yield* new ClaudeSessionAttachmentError({
                reason: "handoff-blocked",
                cause:
                  "Transcript changed or has an incomplete final record; retry after Claude exits cleanly",
              });
            if (parent === null)
              for (const line of tail.lines)
                cwd = attachmentDetails(line, binding.sessionId)?.cwd ?? cwd;
            yield* setState(thread.id, "detached", cursor, model);
          },
        );
        yield* importRemaining(initial, null, null);
        for (const child of yield* source.children(
          external.configDirectory,
          external.transcriptPath,
        )) {
          const thread = yield* create(initial, {
            configDirectory: external.configDirectory,
            providerInstanceId: initial.providerInstanceId,
            sessionId: external.sessionId,
            transcriptPath: child.path,
            title: `Claude subagent ${child.agentId}`,
            projectId: initial.projectId,
            agentId: child.agentId,
          });
          yield* importRemaining(thread, initial, child.agentId);
        }
        const descendants = yield* sql<{ thread_id: string }>`
          SELECT thread_id FROM orchestration_v2_projection_threads
          WHERE json_extract(payload_json, '$.lineage.rootThreadId') = ${initial.id}
            AND json_extract(payload_json, '$.externalSession') IS NOT NULL
        `;
        for (const row of descendants) {
          const thread = yield* projections.getThread(ThreadId.make(row.thread_id));
          yield* setState(thread.id, "detached");
          const records = yield* projections.getThreadRecords(thread.id, ["nodes", "subagents"]);
          const at = yield* DateTime.now;
          const events = [
            ...records.nodes
              .filter((node) => node.status === "running")
              .map((node) =>
                attachmentEvent(
                  thread.id,
                  "node.updated",
                  { ...node, status: "idle" },
                  at,
                  `handoff:${node.id}`,
                ),
              ),
            ...records.subagents
              .filter((agent) => agent.status === "running")
              .map((agent) =>
                attachmentEvent(
                  thread.id,
                  "subagent.updated",
                  {
                    ...agent,
                    status: "idle",
                    updatedAt: at,
                    progress: "Original Claude process exited; final outcome was not recorded.",
                  },
                  at,
                  `handoff:${agent.id}`,
                ),
              ),
          ];
          if (events.length) yield* sink.write({ events, skipExistingEventIds: true });
        }
        yield* source.assertStopped(external.configDirectory, external.sessionId);
        if (!cwd || !path.isAbsolute(cwd))
          return yield* new ClaudeSessionAttachmentError({
            reason: "handoff-blocked",
            cause: "The original workspace could not be determined from the transcript",
          });
        const current = yield* projections.getThread(initial.id);
        const now = yield* DateTime.now;
        const committed = yield* sink.write({
          expectedThread: current,
          events: [
            attachmentEvent(
              current.id,
              "thread.metadata-updated",
              { ...current, externalSession: undefined, worktreePath: cwd, updatedAt: now },
              now,
              `handoff:${DateTime.toEpochMillis(now)}`,
            ),
          ],
        });
        if (!committed.length)
          return yield* new ClaudeSessionAttachmentError({
            reason: "handoff-blocked",
            cause: "Thread changed during handoff",
          });
        return { threadId: current.id, attached: false, readOnly: false };
      }
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
      const configDirectory = yield* resolveConfigDirectory(providerInstanceId);
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
    // Large stopped-session imports must not stall live followers of other sessions.
    (effect, input) => (input.action === "handoff" ? handoffGate : gate).withPermit(effect),
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

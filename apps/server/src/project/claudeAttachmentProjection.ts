// @effect-diagnostics-next-line nodeBuiltinImport:off - deterministic event identities keep the projection pure and replayable.
import * as NodeCrypto from "node:crypto";
import {
  EventId,
  ProviderDriverKind,
  MessageId,
  NodeId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const Record = Schema.Struct({
  type: Schema.String,
  uuid: Schema.optional(Schema.String),
  timestamp: Schema.optional(Schema.String),
  sessionId: Schema.optional(Schema.String),
  isMeta: Schema.optional(Schema.Boolean),
  isCompactSummary: Schema.optional(Schema.Boolean),
  isVisibleInTranscriptOnly: Schema.optional(Schema.Boolean),
  promptSource: Schema.optional(Schema.String),
  origin: Schema.optional(Schema.Struct({ kind: Schema.String })),
  toolUseResult: Schema.optional(Schema.Unknown),
  message: Schema.optional(
    Schema.Struct({
      role: Schema.optional(Schema.String),
      model: Schema.optional(Schema.String),
      content: Schema.optional(
        Schema.Union([
          Schema.String,
          Schema.Array(
            Schema.Struct({
              type: Schema.String,
              text: Schema.optional(Schema.String),
            }),
          ),
        ]),
      ),
      stop_reason: Schema.optional(Schema.NullOr(Schema.String)),
    }),
  ),
});
const decodeRecord = Schema.decodeUnknownOption(Schema.fromJsonString(Record));
const decodeSpawn = Schema.decodeUnknownOption(
  Schema.Struct({
    agentId: Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_-]+$/)),
    description: Schema.optional(Schema.String),
    resolvedModel: Schema.optional(Schema.String),
  }),
);
export function attachmentDetails(line: string, sessionId: string) {
  const record = decodeRecord(line);
  if (Option.isNone(record) || record.value.sessionId !== sessionId) return null;
  const spawn = decodeSpawn(record.value.toolUseResult);
  return {
    uuid: record.value.uuid,
    isPeer: record.value.origin?.kind === "peer",
    model: record.value.message?.model,
    spawn: Option.getOrNull(spawn),
  };
}

const attachmentKey = (...parts: ReadonlyArray<string>) =>
  NodeCrypto.createHash("sha256").update(parts.join("\0")).digest("hex");
export const attachmentThreadId = (home: string, session: string, agent = "") =>
  ThreadId.make(`claude-attached-${attachmentKey(home, session, agent)}`);
export const attachmentNodeId = (thread: ThreadId) => NodeId.make(`${thread}:external`);

export function attachmentEvent<T extends OrchestrationV2DomainEvent["type"]>(
  threadId: ThreadId,
  type: T,
  payload: OrchestrationV2DomainEvent["payload"],
  at: DateTime.Utc,
  key: string,
): OrchestrationV2DomainEvent {
  // The event type and payload are paired at callers; metadata event variants share a schema.
  return {
    id: EventId.make(attachmentKey(threadId, type, key)),
    threadId,
    type,
    payload,
    occurredAt: at,
  } as OrchestrationV2DomainEvent;
}

export function projectAttachmentLine(input: {
  thread: OrchestrationV2AppThread;
  parent: OrchestrationV2AppThread | null;
  agentId: string | null;
  line: string;
  ordinal: number;
  includeActivity?: boolean;
  activityStatus?: "idle" | "running";
  activityKey?: string;
  messageId?: MessageId;
  sentMessage?: OrchestrationV2ConversationMessage | undefined;
  previousSubagent?: OrchestrationV2Subagent | undefined;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const decoded = decodeRecord(input.line);
  if (Option.isNone(decoded)) return [];
  const record = decoded.value;
  if (
    !record.uuid ||
    !record.timestamp ||
    !record.message ||
    (record.type !== "user" && record.type !== "assistant") ||
    record.sessionId !== input.thread.externalSession?.sessionId
  )
    return [];
  const parsedAt = DateTime.make(record.timestamp);
  if (Option.isNone(parsedAt)) return [];
  const at = parsedAt.value;
  const { thread, parent } = input;
  const content = record.message.content;
  const rawText =
    typeof content === "string"
      ? content
      : (content ?? [])
          .filter((block) => block.type === "text")
          .map((block) => block.text ?? "")
          .join("\n");
  const text = input.sentMessage?.text ?? rawText;
  // Claude stores harness traffic using the user API role. Provenance, not API role,
  // determines whether it belongs in a human chat bubble. Never rewrite human text.
  const internal =
    record.type === "user" &&
    !input.sentMessage &&
    (record.isMeta ||
      record.isCompactSummary ||
      record.isVisibleInTranscriptOnly ||
      record.promptSource === "system" ||
      (record.origin !== undefined && record.origin.kind !== "human") ||
      (record.origin === undefined &&
        /^(?:<task-notification>|<command-name>|<local-command-|\[Request interrupted by user)/.test(
          text,
        )));
  const internalTitle = record.isCompactSummary
    ? "Claude context summary"
    : record.origin?.kind === "peer"
      ? "Claude agent message"
      : record.origin?.kind === "task-notification" || text.startsWith("<task-notification>")
        ? "Claude task notification"
        : "Claude internal event";
  const events: Array<OrchestrationV2DomainEvent> = [];
  const key = attachmentKey("projection-v2", record.uuid, input.line);
  if (text) {
    const messageId =
      input.sentMessage?.id ?? input.messageId ?? MessageId.make(`${thread.id}:${record.uuid}`);
    const role = internal
      ? ("system" as const)
      : record.type === "user"
        ? ("user" as const)
        : ("assistant" as const);
    const message = {
      id: messageId,
      threadId: thread.id,
      runId: null,
      nodeId: null,
      role,
      text,
      attachments: [],
      streaming: false,
      createdAt: at,
      updatedAt: at,
      createdBy:
        input.sentMessage?.createdBy ??
        (internal ? ("system" as const) : role === "user" ? ("user" as const) : ("agent" as const)),
      creationSource: input.sentMessage?.creationSource ?? ("provider" as const),
    };
    events.push(attachmentEvent(thread.id, "message.updated", message, at, key));
    const common = {
      id: TurnItemId.make(`${messageId}:item`),
      threadId: thread.id,
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: input.ordinal,
      status: "completed" as const,
      title: null,
      startedAt: at,
      completedAt: at,
      updatedAt: at,
      messageId,
      text,
    };
    events.push(
      attachmentEvent(
        thread.id,
        "turn-item.updated",
        internal
          ? { ...common, type: "system_notice", title: internalTitle, message: internalTitle }
          : role === "user"
            ? {
                ...common,
                type: "user_message",
                createdBy: message.createdBy,
                creationSource: message.creationSource,
                inputIntent: "turn_start",
                attachments: [],
              }
            : { ...common, type: "assistant_message", streaming: false },
        at,
        key,
      ),
    );
  }
  if (input.includeActivity === false) return events;
  const activityKey = `${key}:activity-v2:${input.activityKey ?? "history"}`;
  {
    const ownNodeId = parent ? NodeId.make(`${thread.id}:root`) : attachmentNodeId(thread.id);
    const completed =
      record.type === "assistant" &&
      ["end_turn", "stop_sequence"].includes(record.message.stop_reason ?? "");
    events.push(
      attachmentEvent(
        thread.id,
        "node.updated",
        {
          id: ownNodeId,
          threadId: thread.id,
          runId: null,
          parentNodeId: null,
          rootNodeId: ownNodeId,
          kind: "root_turn",
          status: completed ? "completed" : (input.activityStatus ?? "running"),
          countsForRun: false,
          providerThreadId: thread.activeProviderThreadId,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: null,
          completedAt: completed ? at : null,
        },
        at,
        activityKey,
      ),
    );
  }
  if (parent && input.agentId) {
    const completed =
      record.type === "assistant" &&
      ["end_turn", "stop_sequence"].includes(record.message.stop_reason ?? "");
    const subagent: OrchestrationV2Subagent = {
      id: attachmentNodeId(thread.id),
      threadId: parent.id,
      runId: null,
      parentNodeId: attachmentNodeId(thread.lineage.parentThreadId ?? parent.id),
      origin: "provider_native",
      createdBy: "agent",
      driver: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: thread.providerInstanceId,
      providerThreadId: null,
      childThreadId: thread.id,
      nativeTaskRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        nativeId: input.agentId,
        strength: "strong",
      },
      prompt: input.previousSubagent?.prompt || (record.type === "user" ? text : ""),
      title: thread.title,
      ...(!completed && input.activityStatus === "idle"
        ? { progress: "No recent activity; final outcome not recorded by Claude." }
        : {}),
      model: record.message.model ?? thread.modelSelection.model,
      status: completed ? "completed" : (input.activityStatus ?? "running"),
      result: completed ? text : null,
      startedAt: input.previousSubagent?.startedAt ?? at,
      completedAt: completed ? at : null,
      updatedAt: at,
    };
    events.push(attachmentEvent(parent.id, "subagent.updated", subagent, at, activityKey));
    events.push(
      attachmentEvent(
        parent.id,
        "node.updated",
        {
          id: subagent.id,
          threadId: parent.id,
          runId: null,
          parentNodeId: subagent.parentNodeId,
          rootNodeId: attachmentNodeId(parent.id),
          kind: "subagent",
          status: subagent.status,
          countsForRun: false,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: subagent.nativeTaskRef,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: null,
          completedAt: subagent.completedAt,
        },
        at,
        activityKey,
      ),
    );
  }
  return events;
}

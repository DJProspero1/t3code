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
  return { model: record.value.message?.model, spawn: Option.getOrNull(spawn) };
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
  const text =
    typeof content === "string"
      ? content
      : (content ?? [])
          .filter((block) => block.type === "text")
          .map((block) => block.text ?? "")
          .join("\n");
  const events: Array<OrchestrationV2DomainEvent> = [];
  const key = attachmentKey(record.uuid, input.line);
  if (text) {
    const messageId = MessageId.make(`${thread.id}:${record.uuid}`);
    const role = record.type === "user" ? ("user" as const) : ("assistant" as const);
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
      createdBy: role === "user" ? ("user" as const) : ("agent" as const),
      creationSource: "provider" as const,
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
        role === "user"
          ? {
              ...common,
              type: "user_message",
              createdBy: "user",
              creationSource: "provider",
              inputIntent: "turn_start",
              attachments: [],
            }
          : { ...common, type: "assistant_message", streaming: false },
        at,
        key,
      ),
    );
  }
  {
    const ownNodeId = parent ? NodeId.make(`${thread.id}:root`) : attachmentNodeId(thread.id);
    const completed = record.type === "assistant" && record.message.stop_reason === "end_turn";
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
          status: completed ? "completed" : "running",
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
        key,
      ),
    );
  }
  if (parent && input.agentId) {
    const completed = record.type === "assistant" && record.message.stop_reason === "end_turn";
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
      model: record.message.model ?? thread.modelSelection.model,
      status: completed ? "completed" : "running",
      result: completed ? text : null,
      startedAt: input.previousSubagent?.startedAt ?? at,
      completedAt: completed ? at : null,
      updatedAt: at,
    };
    events.push(attachmentEvent(parent.id, "subagent.updated", subagent, at, key));
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
        key,
      ),
    );
  }
  return events;
}

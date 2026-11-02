// @effect-diagnostics-next-line nodeBuiltinImport:off - stable native identities allow replay without allocating duplicate messages.
import * as NodeCrypto from "node:crypto";
import {
  EventId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
  type OrchestrationV2Subagent,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { field } from "../orchestration-v2/unknownField.ts";
import type { ClaudeLiveBatch } from "../provider/ClaudeLiveTransport.ts";
import { attachmentNodeId, projectAttachmentLine } from "./claudeAttachmentProjection.ts";

type Frame = ClaudeLiveBatch["events"][number];
const key = (...parts: ReadonlyArray<string | number>) =>
  NodeCrypto.createHash("sha256").update(JSON.stringify(parts)).digest("hex");
export const claudeLiveMessageEventId = (threadId: ThreadId, uuid: string) =>
  EventId.make(key("claude-live-message", threadId, uuid));
export const claudeLiveFrameEventId = (
  threadId: ThreadId,
  epoch: string,
  frame: Frame,
  index = 0,
) => {
  const uuid = field(frame.payload, "uuid");
  return frame.type === "message" && typeof uuid === "string" && index === 0
    ? claudeLiveMessageEventId(threadId, uuid)
    : EventId.make(key("claude-live-frame", threadId, epoch, frame.sequence, index));
};
export const claudeLiveStepMessageId = (
  threadId: ThreadId,
  epoch: string,
  turnId: string,
  index: number,
) => MessageId.make(key("claude-live-step", threadId, epoch, turnId, index));

export function claudeLiveAliasMessageId(
  threadId: ThreadId,
  epoch: string,
  payload: unknown,
): MessageId | undefined {
  const role = field(payload, "role");
  const commandId = field(payload, "commandId");
  if (role === "user" && typeof commandId === "string") return MessageId.make(commandId);
  const step = field(payload, "step");
  const turn = field(step, "turnId");
  const index = field(step, "index");
  const block = field(step, "blockIndex");
  if (
    role === "assistant" &&
    typeof turn === "string" &&
    typeof index === "number" &&
    typeof block === "number"
  )
    return MessageId.make(`${claudeLiveStepMessageId(threadId, epoch, turn, index)}:text:${block}`);
  return undefined;
}

/** Project native observations into existing message, reasoning and tool rows. */
export function projectClaudeLiveFrame(input: {
  readonly thread: OrchestrationV2AppThread;
  readonly parent: OrchestrationV2AppThread | null;
  readonly agentId: string | null;
  readonly previousSubagent?: OrchestrationV2Subagent | undefined;
  readonly epoch: string;
  readonly frame: Frame;
  readonly at: DateTime.Utc;
  readonly messages: ReadonlyMap<MessageId, OrchestrationV2ConversationMessage>;
  readonly items: ReadonlyMap<TurnItemId, OrchestrationV2TurnItem>;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const { thread, frame, at } = input;
  const payload = frame.payload;
  const events: OrchestrationV2DomainEvent[] = [];
  const emit = (
    type: OrchestrationV2DomainEvent["type"],
    value: OrchestrationV2DomainEvent["payload"],
  ) => {
    events.push({
      id: claudeLiveFrameEventId(thread.id, input.epoch, frame, events.length),
      threadId: thread.id,
      type,
      payload: value,
      occurredAt: at,
    } as OrchestrationV2DomainEvent);
  };
  const base = (id: TurnItemId, nativeId: string) => {
    const previous = input.items.get(id);
    return {
      id,
      threadId: thread.id,
      runId: null,
      nodeId: null,
      providerThreadId: thread.activeProviderThreadId,
      providerTurnId: null,
      nativeItemRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        nativeId,
        strength: "strong" as const,
      },
      parentItemId: null,
      ordinal: previous?.ordinal ?? DateTime.toEpochMillis(at),
      title: null,
      status: "running" as const,
      startedAt: previous?.startedAt ?? at,
      completedAt: null,
      updatedAt: at,
    };
  };
  const turnId = field(payload, "turnId");
  const stepIndex = field(payload, "index");
  const stepId =
    typeof turnId === "string" && typeof stepIndex === "number"
      ? claudeLiveStepMessageId(thread.id, input.epoch, turnId, stepIndex)
      : undefined;
  const projectText = (id: MessageId, text: string, completed: boolean) => {
    const previous = input.messages.get(id);
    const message: OrchestrationV2ConversationMessage = {
      id,
      threadId: thread.id,
      runId: null,
      nodeId: null,
      role: "assistant",
      text,
      attachments: [],
      streaming: !completed,
      createdAt: previous?.createdAt ?? at,
      updatedAt: at,
      createdBy: "agent",
      creationSource: "provider",
    };
    emit("message.updated", message);
    emit("turn-item.updated", {
      ...base(TurnItemId.make(`${id}:item`), id),
      type: "assistant_message",
      messageId: id,
      text,
      streaming: !completed,
      status: completed ? "completed" : "running",
      completedAt: completed ? at : null,
    });
  };
  if (frame.type === "snapshot" && input.parent && input.agentId) {
    const nativeStatus = field(payload, "status");
    const status = nativeStatus === "killed" ? "interrupted" : nativeStatus;
    if (
      status !== "pending" &&
      status !== "running" &&
      status !== "waiting" &&
      status !== "idle" &&
      status !== "completed" &&
      status !== "failed" &&
      status !== "interrupted"
    )
      return [];
    const previous = input.previousSubagent;
    const terminal = status === "completed" || status === "failed" || status === "interrupted";
    const description = field(payload, "description");
    const subagent: OrchestrationV2Subagent = {
      id: attachmentNodeId(thread.id),
      threadId: input.parent.id,
      runId: null,
      parentNodeId: attachmentNodeId(thread.lineage.parentThreadId ?? input.parent.id),
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
      prompt: previous?.prompt ?? "",
      title: typeof description === "string" && description ? description : thread.title,
      model: previous?.model ?? thread.modelSelection.model,
      status,
      result: previous?.result ?? null,
      startedAt: previous?.startedAt ?? at,
      completedAt: terminal ? (previous?.completedAt ?? at) : null,
      updatedAt: at,
    };
    // The subagent card belongs to the root's orchestration, even for nested agents.
    events.push({
      id: claudeLiveFrameEventId(thread.id, input.epoch, frame, events.length),
      threadId: input.parent.id,
      type: "subagent.updated",
      payload: subagent,
      occurredAt: at,
    });
    events.push({
      id: claudeLiveFrameEventId(thread.id, input.epoch, frame, events.length),
      threadId: input.parent.id,
      type: "node.updated",
      occurredAt: at,
      payload: {
        id: subagent.id,
        threadId: input.parent.id,
        runId: null,
        parentNodeId: subagent.parentNodeId,
        rootNodeId: attachmentNodeId(input.parent.id),
        kind: "subagent",
        status,
        countsForRun: false,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: subagent.nativeTaskRef,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: subagent.startedAt,
        completedAt: subagent.completedAt,
      },
    });
    const id = NodeId.make(`${thread.id}:root`);
    emit("node.updated", {
      id,
      threadId: thread.id,
      runId: null,
      parentNodeId: null,
      rootNodeId: id,
      kind: "root_turn",
      status,
      countsForRun: false,
      providerThreadId: thread.activeProviderThreadId,
      providerTurnId: null,
      nativeItemRef: subagent.nativeTaskRef,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: subagent.startedAt,
      completedAt: subagent.completedAt,
    });
  } else if (
    frame.type === "turn.start" ||
    frame.type === "step.start" ||
    frame.type === "turn.complete"
  ) {
    const complete = frame.type === "turn.complete";
    const reason = field(payload, "reason");
    const status = !complete
      ? "running"
      : reason === "aborted"
        ? "interrupted"
        : reason === "error" || reason === "refusal"
          ? "failed"
          : "completed";
    const id = input.parent ? NodeId.make(`${thread.id}:root`) : attachmentNodeId(thread.id);
    emit("node.updated", {
      id,
      threadId: thread.id,
      runId: null,
      parentNodeId: null,
      rootNodeId: id,
      kind: "root_turn",
      status,
      countsForRun: false,
      providerThreadId: thread.activeProviderThreadId,
      providerTurnId: null,
      nativeItemRef:
        typeof turnId === "string"
          ? {
              driver: ProviderDriverKind.make("claudeAgent"),
              nativeId: turnId,
              strength: "strong",
            }
          : null,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: null,
      completedAt: complete ? at : null,
    });
  } else if (
    frame.type === "tool.start" ||
    frame.type === "tool.complete" ||
    frame.type === "tool.failed"
  ) {
    const nativeId = field(payload, "tool_use_id");
    if (typeof nativeId !== "string") return [];
    const id = TurnItemId.make(`${thread.id}:tool:${nativeId}`);
    const previous = input.items.get(id);
    const toolName = field(payload, "tool");
    const result = field(payload, "result");
    const denied = field(result, "deny");
    const complete = frame.type !== "tool.start";
    const toolInput =
      typeof payload === "object" && payload !== null
        ? Object.fromEntries(
            Object.entries(payload).filter(
              ([key]) => !["tool", "tool_use_id", "agentId"].includes(key),
            ),
          )
        : {};
    emit("turn-item.updated", {
      ...base(id, nativeId),
      type: "dynamic_tool",
      toolName:
        typeof toolName === "string"
          ? toolName
          : previous?.type === "dynamic_tool"
            ? previous.toolName
            : null,
      input: complete && previous?.type === "dynamic_tool" ? previous.input : toolInput,
      ...(frame.type === "tool.complete"
        ? { output: denied ?? field(result, "result") ?? result }
        : {}),
      status: !complete
        ? "running"
        : frame.type === "tool.failed"
          ? "failed"
          : denied !== undefined
            ? "cancelled"
            : "completed",
      completedAt: complete ? at : null,
    });
  } else if (frame.type === "step.chunk" && stepId) {
    const chunk = field(payload, "chunk");
    const kind = field(chunk, "kind");
    const text = field(chunk, "text");
    if (kind === "text" && typeof text === "string") {
      const block = field(chunk, "index");
      if (typeof block !== "number") return [];
      const id = MessageId.make(`${stepId}:text:${block}`);
      // JSONL may finish this block before a delayed live batch arrives.
      if (input.messages.get(id)?.streaming === false) return [];
      projectText(id, (input.messages.get(id)?.text ?? "") + text, false);
    } else if (kind === "thinking" && typeof text === "string") {
      const block = field(chunk, "index");
      if (typeof block !== "number") return [];
      const id = TurnItemId.make(`${stepId}:thinking:${block}`);
      const previous = input.items.get(id);
      emit("turn-item.updated", {
        ...base(id, id),
        type: "reasoning",
        title: "Thinking",
        text: (previous?.type === "reasoning" ? previous.text : "") + text,
        streaming: true,
      });
    } else if (kind === "tool") {
      const nativeId = field(chunk, "id");
      const name = field(chunk, "name");
      if (typeof nativeId !== "string" || typeof name !== "string") return [];
      emit("turn-item.updated", {
        ...base(TurnItemId.make(`${thread.id}:tool:${nativeId}`), nativeId),
        type: "dynamic_tool",
        toolName: name,
        input: {},
      });
    }
  } else if ((frame.type === "step.complete" || frame.type === "step.failed") && stepId) {
    // Claude commits each response block under its own transcript UUID. Never
    // replace those messages with the step's combined answer.
    for (const message of input.messages.values()) {
      if (message.id.startsWith(`${stepId}:text:`) && message.streaming)
        projectText(message.id, message.text, true);
    }
    for (const item of input.items.values()) {
      if (
        item.type !== "reasoning" ||
        !item.id.startsWith(`${stepId}:thinking:`) ||
        !item.streaming
      )
        continue;
      emit("turn-item.updated", {
        ...item,
        streaming: false,
        status: frame.type === "step.failed" ? "failed" : "completed",
        completedAt: at,
        updatedAt: at,
      });
    }
  } else if (frame.type === "message") {
    const uuid = field(payload, "uuid");
    const message = field(payload, "message");
    const nativeType = field(message, "type");
    if (nativeType !== undefined && nativeType !== "user" && nativeType !== "assistant") return [];
    const role = field(message, "role") ?? nativeType;
    if (typeof uuid !== "string" || (role !== "user" && role !== "assistant")) return [];
    const step = field(payload, "step");
    const nativeTurn = field(step, "turnId");
    const nativeIndex = field(step, "index");
    const blockIndex = field(step, "blockIndex");
    const commandId = field(payload, "commandId");
    const messageId =
      role === "user" && typeof commandId === "string"
        ? MessageId.make(commandId)
        : role === "assistant" &&
            typeof nativeTurn === "string" &&
            typeof nativeIndex === "number" &&
            typeof blockIndex === "number"
          ? MessageId.make(
              `${claudeLiveStepMessageId(thread.id, input.epoch, nativeTurn, nativeIndex)}:text:${blockIndex}`,
            )
          : MessageId.make(`${thread.id}:${uuid}`);
    const previous = input.messages.get(messageId);
    const tools = new Map(
      [...input.items.values()].flatMap((item) =>
        item.type === "dynamic_tool" && item.id.startsWith(`${thread.id}:tool:`)
          ? [[item.id.slice(`${thread.id}:tool:`.length), item] as const]
          : [],
      ),
    );
    const projected = projectAttachmentLine({
      thread,
      parent: input.parent,
      agentId: input.agentId,
      ordinal:
        input.items.get(TurnItemId.make(`${messageId}:item`))?.ordinal ??
        DateTime.toEpochMillis(at),
      includeActivity: false,
      messageId,
      ...(previous?.role === "user" && previous.creationSource !== "provider"
        ? { sentMessage: previous }
        : {}),
      previousTools: tools,
      line: JSON.stringify({
        type: role,
        uuid,
        sessionId: thread.externalSession?.sessionId,
        timestamp: DateTime.formatIso(at),
        message,
        origin:
          field(payload, "door") === "prompt" &&
          field(field(payload, "origin"), "kind") === "unclassified"
            ? { kind: "human" }
            : field(payload, "origin"),
      }),
    });
    for (const event of projected) {
      emit(
        event.type,
        event.type === "message.updated" && previous
          ? { ...event.payload, createdAt: previous.createdAt }
          : event.payload,
      );
    }
  }
  return events;
}

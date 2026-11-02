import {
  EventId,
  NodeId,
  RuntimeRequestId,
  TurnItemId,
  ProviderDriverKind,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type ProviderSessionId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import {
  claudeUserInputQuestions,
  claudeSdkUserInputAnswers,
  claudeMcpElicitationAcceptance,
  permissionResultFromDecision,
  providerRequestKindFromClaudeTool,
} from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { field } from "../orchestration-v2/unknownField.ts";
import type {
  ClaudeLiveRequestPayload,
  ClaudeLiveRequestReply,
} from "../provider/ClaudeLiveTransport.ts";
import { attachmentNodeId } from "./claudeAttachmentProjection.ts";
import { claudeLiveMessageEventId } from "./claudeLiveProjection.ts";

export const liveRequestId = (thread: OrchestrationV2AppThread, input: ClaudeLiveRequestPayload) =>
  RuntimeRequestId.make(
    claudeLiveMessageEventId(thread.id, JSON.stringify([input.epoch, input.requestId])),
  );

/** Uses the same persisted request, node and card shapes as normal Claude controls. */
export function projectClaudeLiveRequest(input: {
  thread: OrchestrationV2AppThread;
  providerSessionId: ProviderSessionId;
  request: ClaudeLiveRequestPayload;
  at: DateTime.Utc;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const { thread, request: source, at } = input;
  const id = liveRequestId(thread, source);
  const nodeId = NodeId.make(`${id}:node`);
  const nativeItemRef = {
    driver: ProviderDriverKind.make("claudeAgent"),
    nativeId: source.requestId,
    strength: "strong" as const,
  };
  const tool = field(source.payload, "tool_name");
  const toolInput = field(source.payload, "tool_input");
  const questions =
    source.kind === "question" ? claudeUserInputQuestions(source.payload) : undefined;
  const kind =
    source.kind === "question"
      ? "user_input"
      : source.kind === "elicitation"
        ? "mcp-elicitation"
        : providerRequestKindFromClaudeTool(typeof tool === "string" ? tool : "");
  const request: OrchestrationV2RuntimeRequest = {
    id,
    nodeId,
    providerTurnId: null,
    nativeRequestRef: nativeItemRef,
    kind,
    status: "pending",
    responseCapability: {
      type: "live",
      providerSessionId: input.providerSessionId,
      ...(source.kind === "permission" ? { supportsFeedback: true } : {}),
    },
    createdAt: at,
    resolvedAt: null,
  };
  const command = field(toolInput, "command");
  const message = field(source.payload, "message");
  const prompt =
    typeof command === "string"
      ? command
      : typeof message === "string"
        ? message
        : `${typeof tool === "string" ? tool : "Tool"}: ${JSON.stringify(toolInput ?? {})}`;
  const card: OrchestrationV2TurnItem = {
    id: TurnItemId.make(`${id}:item`),
    threadId: thread.id,
    runId: null,
    nodeId,
    providerThreadId: thread.activeProviderThreadId,
    providerTurnId: null,
    nativeItemRef,
    parentItemId: null,
    ordinal: DateTime.toEpochMillis(at),
    status: "waiting",
    title: null,
    startedAt: at,
    completedAt: null,
    updatedAt: at,
    ...(questions === undefined
      ? {
          type: "approval_request",
          requestId: id,
          requestKind: kind === "user_input" ? "command" : kind,
          prompt,
          // One request at a time; standing rules need native permission suggestions.
          options: [
            { decision: "accept", label: "Approve" },
            { decision: "decline", label: "Decline" },
          ],
        }
      : { type: "user_input_request", requestId: id, questions }),
  };
  const event = (
    type: OrchestrationV2DomainEvent["type"],
    payload: OrchestrationV2DomainEvent["payload"],
    suffix: string,
  ) =>
    ({
      id: EventId.make(`${id}:${suffix}`),
      threadId: thread.id,
      type,
      payload,
      occurredAt: at,
    }) as OrchestrationV2DomainEvent;
  return [
    event(
      "node.updated",
      {
        id: nodeId,
        threadId: thread.id,
        runId: null,
        parentNodeId: thread.lineage.parentThreadId
          ? NodeId.make(`${thread.id}:root`)
          : attachmentNodeId(thread.id),
        rootNodeId: thread.lineage.parentThreadId
          ? NodeId.make(`${thread.id}:root`)
          : attachmentNodeId(thread.id),
        kind: questions === undefined ? "approval_request" : "user_input_request",
        status: "waiting",
        countsForRun: false,
        providerThreadId: thread.activeProviderThreadId,
        providerTurnId: null,
        nativeItemRef,
        runtimeRequestId: id,
        checkpointScopeId: null,
        startedAt: at,
        completedAt: null,
      },
      "node",
    ),
    event("runtime-request.updated", request, "request"),
    event("turn-item.updated", card, "card"),
  ];
}

/** Match the normal Claude adapter: do not offer consent for a form T3 cannot answer. */
export function claudeLiveElicitationAcceptance(source: ClaudeLiveRequestPayload) {
  const schema = field(source.payload, "requested_schema");
  const message = field(source.payload, "message");
  const serverName = field(source.payload, "mcp_server_name");
  return typeof schema === "object" &&
    schema !== null &&
    !Array.isArray(schema) &&
    typeof message === "string" &&
    typeof serverName === "string" &&
    field(source.payload, "mode") !== "url"
    ? claudeMcpElicitationAcceptance({
        mode: "form",
        message,
        serverName,
        requestedSchema: schema as NonNullable<
          Parameters<typeof claudeMcpElicitationAcceptance>[0]["requestedSchema"]
        >,
      })
    : undefined;
}

export function claudeLiveRequestReply(
  source: ClaudeLiveRequestPayload,
  request: OrchestrationV2RuntimeRequest,
): ClaudeLiveRequestReply {
  if (request.status === "pending") return { type: "pending" };
  if (request.status !== "resolved") return { type: "cancelled" };
  if (source.kind === "question")
    return {
      type: "resolved",
      response: {
        answers: claudeSdkUserInputAnswers(request.answers ?? {}),
      },
    };
  if (source.kind === "elicitation") {
    if (request.decision !== "accept")
      return {
        type: "resolved",
        response: {
          action: request.decision === "cancel" ? "cancel" : "decline",
        },
      };
    const acceptance = claudeLiveElicitationAcceptance(source);
    return { type: "resolved", response: acceptance ?? { action: "decline" } };
  }
  const tool = field(source.payload, "tool_name");
  const raw = field(source.payload, "tool_input");
  return {
    type: "resolved",
    response: permissionResultFromDecision({
      toolName: typeof tool === "string" ? tool : "",
      decision: request.decision ?? "decline",
      toolInput:
        typeof raw === "object" && raw !== null && !Array.isArray(raw)
          ? (raw as Record<string, unknown>)
          : {},
      toolUseID: source.requestId,
      ...(request.feedback === undefined ? {} : { feedback: request.feedback }),
    }),
  };
}

import {
  MessageId,
  TurnItemId,
  type ProviderSessionId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { attachmentNodeId } from "./claudeAttachmentProjection.ts";
import { field } from "../orchestration-v2/unknownField.ts";
import {
  ClaudeLiveTransportError,
  type ClaudeLiveBatch,
  type ClaudeLiveRequestPayload,
  type ClaudeLiveRequestReply,
} from "../provider/ClaudeLiveTransport.ts";
import {
  claudeLiveFrameEventId,
  claudeLiveStepMessageId,
  projectClaudeLiveFrame,
} from "./claudeLiveProjection.ts";

import {
  liveRequestId,
  claudeLiveElicitationAcceptance,
  projectClaudeLiveRequest,
  claudeLiveRequestReply,
} from "./claudeLiveRequests.ts";

const isTransportError = Schema.is(ClaudeLiveTransportError);

interface Input {
  readonly root: OrchestrationV2AppThread;
  readonly batch: ClaudeLiveBatch;
  readonly resolveChild: (
    agentId: string,
    parentAgentId?: string,
  ) => Effect.Effect<OrchestrationV2AppThread, ClaudeLiveTransportError>;
}
export class ClaudeLiveEventIngestor extends Context.Service<
  ClaudeLiveEventIngestor,
  {
    readonly ingest: (input: Input) => Effect.Effect<number, ClaudeLiveTransportError>;
    readonly request: (input: {
      thread: OrchestrationV2AppThread;
      providerSessionId: ProviderSessionId;
      request: ClaudeLiveRequestPayload;
    }) => Effect.Effect<ClaudeLiveRequestReply, ClaudeLiveTransportError>;
  }
>()("t3/project/ClaudeLiveEventIngestor") {}

export const layer = Layer.effect(
  ClaudeLiveEventIngestor,
  Effect.gen(function* () {
    const store = yield* EventStore.EventStoreV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    return ClaudeLiveEventIngestor.of({
      request: Effect.fn("ClaudeLiveEventIngestor.request")(
        function* (input) {
          const current = yield* projections.getThread(input.thread.id);
          if (
            !current.externalSession?.attached ||
            current.deletedAt !== null ||
            current.externalSession.sessionId !== input.request.sessionId
          )
            return yield* new ClaudeLiveTransportError({ reason: "unavailable" });
          if (
            input.request.kind === "elicitation" &&
            claudeLiveElicitationAcceptance(input.request) === undefined
          )
            return { type: "resolved" as const, response: { action: "decline" } };
          const id = liveRequestId(current, input.request);
          const existing = yield* projections.getRuntimeRequest(current.id, id);
          if (existing) return claudeLiveRequestReply(input.request, existing);
          const at = yield* DateTime.now;
          yield* sink.write({
            expectedThread: current,
            skipExistingEventIds: true,
            events: projectClaudeLiveRequest({ ...input, thread: current, at }),
          });
          const stored = yield* projections.getRuntimeRequest(current.id, id);
          if (!stored) return yield* new ClaudeLiveTransportError({ reason: "persistence-failed" });
          return claudeLiveRequestReply(input.request, stored);
        },
        (effect) =>
          effect.pipe(
            Effect.mapError((cause) =>
              isTransportError(cause)
                ? cause
                : new ClaudeLiveTransportError({ reason: "persistence-failed" }),
            ),
          ),
      ),
      ingest: Effect.fn("ClaudeLiveEventIngestor.ingest")(
        function* ({ root, batch, resolveChild }: Input) {
          if (!root.externalSession?.attached || root.externalSession.sessionId !== batch.sessionId)
            return yield* new ClaudeLiveTransportError({ reason: "unavailable" });
          const groups = new Map<
            string,
            {
              thread: OrchestrationV2AppThread;
              agentId: string | null;
              frames: Array<ClaudeLiveBatch["events"][number]>;
            }
          >();
          const expanded = batch.events.flatMap((frame) => {
            const agents = field(frame.payload, "agents");
            if (frame.type !== "snapshot" || !Array.isArray(agents)) return [frame];
            return [
              frame,
              ...agents.map((agent) => ({
                ...frame,
                payload: { ...Object(agent), agentId: field(agent, "id") },
              })),
            ];
          });
          for (const frame of expanded) {
            const agent = field(frame.payload, "agentId") ?? field(frame.payload, "agent_id");
            if (
              agent !== undefined &&
              (typeof agent !== "string" || !/^[a-zA-Z0-9_-]{1,256}$/.test(agent))
            )
              return yield* new ClaudeLiveTransportError({ reason: "invalid-batch" });
            const agentId = typeof agent === "string" ? agent : null;
            const key = agentId ?? "";
            let group = groups.get(key);
            if (!group) {
              group = {
                thread:
                  agentId === null
                    ? root
                    : yield* resolveChild(
                        agentId,
                        typeof field(frame.payload, "parentId") === "string"
                          ? (field(frame.payload, "parentId") as string)
                          : undefined,
                      ),
                agentId,
                frames: [],
              };
              groups.set(key, group);
            }
            group.frames.push(frame);
          }
          const persisted = new Set(
            (yield* store.readByIds(
              [...groups.values()].flatMap((group) =>
                group.frames.map((frame) =>
                  claudeLiveFrameEventId(group.thread.id, batch.epoch, frame),
                ),
              ),
            )).map((row) => row.event.id),
          );
          const subagents = new Map(
            (yield* projections.getThreadRecords(root.id, ["subagents"], {
              subagentIds: [...groups.values()]
                .filter((group) => group.agentId !== null)
                .map((group) => attachmentNodeId(group.thread.id)),
            })).subagents.map((agent) => [agent.id, agent]),
          );
          const events: OrchestrationV2DomainEvent[] = [];
          const now = yield* DateTime.now;
          for (const group of groups.values()) {
            const frames = group.frames.filter(
              (frame) =>
                !persisted.has(claudeLiveFrameEventId(group.thread.id, batch.epoch, frame)),
            );
            if (frames.length === 0) continue;
            const messageIds = new Set<MessageId>();
            const itemIds = new Set<TurnItemId>();
            const rememberMessage = (id: MessageId) => {
              messageIds.add(id);
              itemIds.add(TurnItemId.make(`${id}:item`));
            };
            for (const frame of frames) {
              const payload = frame.payload;
              const step = frame.type === "message" ? field(payload, "step") : payload;
              const turn = field(step, "turnId");
              const index = field(step, "index");
              const chunk = field(payload, "chunk");
              const block =
                frame.type === "message" ? field(step, "blockIndex") : field(chunk, "index");
              if (
                typeof turn === "string" &&
                typeof index === "number" &&
                typeof block === "number"
              ) {
                const base = claudeLiveStepMessageId(group.thread.id, batch.epoch, turn, index);
                rememberMessage(MessageId.make(`${base}:text:${block}`));
                itemIds.add(TurnItemId.make(`${base}:thinking:${block}`));
              }
              const commandId = field(payload, "commandId");
              const uuid = field(payload, "uuid");
              if (typeof commandId === "string") rememberMessage(MessageId.make(commandId));
              if (typeof uuid === "string")
                rememberMessage(MessageId.make(`${group.thread.id}:${uuid}`));
              const toolId =
                field(payload, "tool_use_id") ??
                (field(chunk, "kind") === "tool" ? field(chunk, "id") : undefined);
              if (typeof toolId === "string")
                itemIds.add(TurnItemId.make(`${group.thread.id}:tool:${toolId}`));
              const content = field(field(payload, "message"), "content");
              if (Array.isArray(content))
                for (const block of content) {
                  const toolId =
                    field(block, "tool_use_id") ??
                    (field(block, "type") === "tool_use" ? field(block, "id") : undefined);
                  if (typeof toolId === "string")
                    itemIds.add(TurnItemId.make(`${group.thread.id}:tool:${toolId}`));
                }
            }
            const finishing = frames.some(
              (frame) => frame.type === "step.complete" || frame.type === "step.failed",
            );
            const active = finishing
              ? (yield* projections.getThreadRecords(group.thread.id, ["turnItems"], {
                  turnItemTypes: ["reasoning", "assistant_message"],
                  turnItemStatuses: ["running"],
                })).turnItems
              : [];
            for (const item of active)
              if (item.type === "assistant_message") messageIds.add(item.messageId);
            const records = yield* projections.getThreadRecords(
              group.thread.id,
              ["messages", "turnItems"],
              {
                messageIds: [...messageIds],
                turnItemIds: [...itemIds],
              },
            );
            const messages = new Map<MessageId, OrchestrationV2ConversationMessage>(
              records.messages.map((message) => [message.id, message]),
            );
            const items = new Map<TurnItemId, OrchestrationV2TurnItem>(
              [...records.turnItems, ...active].map((item) => [item.id, item]),
            );
            for (const frame of frames) {
              const parsedAt = frame.at === undefined ? Option.some(now) : DateTime.make(frame.at);
              if (Option.isNone(parsedAt))
                return yield* new ClaudeLiveTransportError({ reason: "invalid-batch" });
              if (frame.type === "request.closed") {
                const nativeRequestId = field(frame.payload, "requestId");
                if (typeof nativeRequestId !== "string")
                  return yield* new ClaudeLiveTransportError({ reason: "invalid-batch" });
                const context = yield* projections.getRuntimeResponseContext(
                  group.thread.id,
                  liveRequestId(group.thread, { epoch: batch.epoch, requestId: nativeRequestId }),
                );
                // A withdrawn native callback cannot consume an answer. Preserve a response
                // already accepted by T3, but close unanswered cards and their execution node.
                if (context.request?.status === "pending") {
                  const closed: OrchestrationV2DomainEvent[] = [];
                  const emit = (
                    type: OrchestrationV2DomainEvent["type"],
                    payload: OrchestrationV2DomainEvent["payload"],
                  ) => {
                    closed.push({
                      id: claudeLiveFrameEventId(
                        group.thread.id,
                        batch.epoch,
                        frame,
                        closed.length,
                      ),
                      threadId: group.thread.id,
                      type,
                      payload,
                      occurredAt: parsedAt.value,
                    } as OrchestrationV2DomainEvent);
                  };
                  emit("runtime-request.updated", {
                    ...context.request,
                    status: "cancelled",
                    resolvedAt: parsedAt.value,
                  });
                  if (context.node)
                    emit("node.updated", {
                      ...context.node,
                      status: "cancelled",
                      completedAt: parsedAt.value,
                    });
                  if (context.item)
                    emit("turn-item.updated", {
                      ...context.item,
                      status: "cancelled",
                      completedAt: parsedAt.value,
                      updatedAt: parsedAt.value,
                    });
                  events.push(...closed);
                }
                continue;
              }
              const projected = projectClaudeLiveFrame({
                thread: group.thread,
                parent: group.agentId === null ? null : root,
                agentId: group.agentId,
                epoch: batch.epoch,
                previousSubagent: subagents.get(attachmentNodeId(group.thread.id)),
                frame,
                at: parsedAt.value,
                messages,
                items,
              });
              for (const event of projected) {
                if (event.type === "subagent.updated")
                  subagents.set(event.payload.id, event.payload);
                if (event.type === "message.updated") messages.set(event.payload.id, event.payload);
                if (event.type === "turn-item.updated") items.set(event.payload.id, event.payload);
              }
              events.push(...projected);
            }
          }
          const current = yield* projections.getThread(root.id);
          if (!current.externalSession?.attached || current.deletedAt !== null)
            return yield* new ClaudeLiveTransportError({ reason: "unavailable" });
          if (events.length > 0) {
            const written = yield* sink.write({
              expectedThread: current,
              skipExistingEventIds: true,
              events,
            });
            // A metadata race must be retried from fresh projections, not acknowledged.
            if (written.length !== events.length)
              return yield* new ClaudeLiveTransportError({ reason: "persistence-failed" });
          }
          return batch.events.at(-1)?.sequence ?? batch.acknowledged;
        },
        (effect) =>
          effect.pipe(
            Effect.mapError((cause) =>
              isTransportError(cause)
                ? cause
                : new ClaudeLiveTransportError({ reason: "persistence-failed" }),
            ),
          ),
      ),
    });
  }),
);

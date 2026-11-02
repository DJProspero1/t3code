import {
  EventId,
  TurnItemId,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";

const observerCommands = new Set<OrchestrationV2ServerCommand["type"]>([
  "thread.visit",
  "thread.metadata.update",
  "thread.archive",
  "thread.unarchive",
  "thread.delete",
  "thread.pin",
  "thread.unpin",
  "thread.settle",
  "thread.unsettle",
  "thread.snooze",
  "thread.unsnooze",
  "thread.mark-unread",
]);

export const canMutateExternalSession = (type: OrchestrationV2ServerCommand["type"]) =>
  observerCommands.has(type);

export function isUnchangedExternalSessionSetting(
  command: OrchestrationV2ServerCommand,
  thread: OrchestrationV2AppThread,
): boolean {
  switch (command.type) {
    case "thread.runtime-mode.set":
      return command.runtimeMode === thread.runtimeMode;
    case "thread.interaction-mode.set":
      return command.interactionMode === thread.interactionMode;
    case "thread.model-selection.set":
      return modelSelectionsEqual(command.modelSelection, thread.modelSelection);
    default:
      return false;
  }
}

export function projectExternalMessage(
  command: Extract<OrchestrationV2ServerCommand, { type: "message.dispatch" }>,
  now: DateTime.Utc,
): ReadonlyArray<OrchestrationV2DomainEvent> {
  const createdBy = command.createdBy ?? "user";
  const creationSource = command.creationSource ?? "web";
  return [
    {
      id: EventId.make(`external:${command.commandId}:message`),
      threadId: command.threadId,
      type: "message.updated",
      occurredAt: now,
      payload: {
        id: command.messageId,
        threadId: command.threadId,
        runId: null,
        nodeId: null,
        role: "user",
        text: command.text,
        attachments: [],
        streaming: false,
        createdAt: now,
        updatedAt: now,
        createdBy,
        creationSource,
      },
    },
    {
      id: EventId.make(`external:${command.commandId}:item`),
      threadId: command.threadId,
      type: "turn-item.updated",
      occurredAt: now,
      payload: {
        id: TurnItemId.make(`${command.messageId}:item`),
        type: "user_message",
        threadId: command.threadId,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: DateTime.toEpochMillis(now),
        status: "pending",
        title: "Sending to external session",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        messageId: command.messageId,
        text: command.text,
        createdBy,
        creationSource,
        inputIntent: "turn_start",
        attachments: [],
      },
    },
  ];
}

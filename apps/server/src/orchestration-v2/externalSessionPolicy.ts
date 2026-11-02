import type { OrchestrationV2ServerCommand } from "@t3tools/contracts";

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

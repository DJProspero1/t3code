import { OrchestrationV2AppThread, ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { attachmentThreadId } from "./claudeAttachmentProjection.ts";

export const claudeLiveThread = Schema.decodeSync(OrchestrationV2AppThread)({
  id: attachmentThreadId("/claude", "session"),
  projectId: ProjectId.make("project"),
  title: "External session",
  providerInstanceId: ProviderInstanceId.make("claudeAgent"),
  modelSelection: {
    instanceId: ProviderInstanceId.make("claudeAgent"),
    model: "claude-sonnet-4-6",
  },
  createdBy: "agent",
  creationSource: "provider",
  runtimeMode: "approval-required",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: {
    parentThreadId: null,
    rootThreadId: attachmentThreadId("/claude", "session"),
    relationshipToParent: null,
  },
  forkedFrom: null,
  createdAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z"),
  updatedAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z"),
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  unsettledAt: null,
  snoozedUntil: null,
  snoozedAt: null,
  pinnedAt: null,
  pinOrderKey: null,
  activeOrderKey: null,
  lastVisitedAt: null,
  deletedAt: null,
  externalSession: {
    provider: "claude",
    sessionId: "session",
    configDirectory: "/claude",
    transcriptPath: "/claude/projects/session.jsonl",
    attached: true,
    state: "following",
  },
});

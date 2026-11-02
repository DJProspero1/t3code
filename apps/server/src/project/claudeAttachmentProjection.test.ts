import { describe, expect, it } from "@effect/vitest";
import {
  OrchestrationV2AppThread,
  OrchestrationV2DomainEvent,
  ProjectId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { attachmentThreadId, projectAttachmentLine } from "./claudeAttachmentProjection.ts";

const thread = Schema.decodeSync(OrchestrationV2AppThread)({
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
const isEvent = Schema.is(OrchestrationV2DomainEvent);
const record = {
  type: "assistant",
  uuid: "message-1",
  sessionId: "session",
  timestamp: "2026-01-01T00:01:00Z",
  message: { content: [{ type: "text", text: "Finished the repair." }], stop_reason: "end_turn" },
};

describe("external Claude projection", () => {
  it("projects native child history and completion without creating a provider turn or replaying a prompt", () => {
    const events = projectAttachmentLine({
      thread,
      parent: { ...thread, id: attachmentThreadId("/claude", "parent") },
      agentId: "child",
      line: JSON.stringify(record),
      ordinal: 1,
    });
    for (const event of events) expect(isEvent(event)).toBe(true);
    expect(events.map((e) => e.type)).toEqual([
      "message.updated",
      "turn-item.updated",
      "node.updated",
      "subagent.updated",
      "node.updated",
    ]);
    const subagent = events.find((e) => e.type === "subagent.updated");
    expect(subagent?.payload.status).toBe("completed");
    expect(subagent?.payload.origin).toBe("provider_native");
    expect(subagent?.payload.childThreadId).toBe(thread.id);
    const again = projectAttachmentLine({
      thread,
      parent: { ...thread, id: attachmentThreadId("/claude", "parent") },
      agentId: "child",
      line: JSON.stringify(record),
      ordinal: 1,
    });
    expect(again).toEqual(events);
  });
  it("does not expose another session or turn tool-result strings into user prompts", () => {
    const project = (value: unknown) =>
      projectAttachmentLine({
        thread,
        parent: null,
        agentId: null,
        line: JSON.stringify(value),
        ordinal: 1,
      });
    expect(project({ ...record, sessionId: "unrelated" })).toEqual([]);
    expect(
      project({
        ...record,
        type: "user",
        message: { content: [{ type: "tool_result", content: "sensitive tool output" }] },
      }),
    ).toEqual(expect.arrayContaining([expect.objectContaining({ type: "node.updated" })]));
    expect(project({ ...record, timestamp: "invalid" })).toEqual([]);
  });
});

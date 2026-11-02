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
  it("joins native tool results to their calls without claiming unfinished history is running", () => {
    const call = projectAttachmentLine({
      thread,
      parent: null,
      agentId: null,
      ordinal: 10,
      includeActivity: false,
      line: JSON.stringify({
        ...record,
        uuid: "tool-call",
        message: {
          content: [
            { type: "tool_use", id: "call-1", name: "Read", input: { file_path: "file.txt" } },
          ],
        },
      }),
    });
    const item = call.find((event) => event.type === "turn-item.updated")!.payload;
    expect(item).toMatchObject({
      type: "dynamic_tool",
      status: "idle",
      toolName: "Read",
      input: { file_path: "file.txt" },
    });
    const result = projectAttachmentLine({
      thread,
      parent: null,
      agentId: null,
      ordinal: 20,
      includeActivity: false,
      previousTools: new Map([["call-1", item]]),
      line: JSON.stringify({
        ...record,
        type: "user",
        uuid: "tool-result",
        message: {
          content: [
            {
              type: "tool_result",
              tool_use_id: "call-1",
              content: "File contents",
              is_error: false,
            },
          ],
        },
      }),
    });
    expect(result).toHaveLength(1);
    expect(result[0]?.payload).toMatchObject({
      id: item.id,
      type: "dynamic_tool",
      status: "completed",
      ordinal: 10,
      toolName: "Read",
      input: { file_path: "file.txt" },
      output: "File contents",
    });
    for (const event of [...call, ...result]) expect(isEvent(event)).toBe(true);
  });

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
  it("keeps human wording exact and classifies harness traffic as system activity", () => {
    const project = (extra: object) =>
      projectAttachmentLine({
        thread,
        parent: null,
        agentId: null,
        ordinal: 1,
        includeActivity: false,
        line: JSON.stringify({
          ...record,
          type: "user",
          message: { content: "  My exact\nmessage  " },
          ...extra,
        }),
      });
    const human = project({ origin: { kind: "human" }, promptSource: "typed" });
    expect(human.find((e) => e.type === "message.updated")?.payload).toMatchObject({
      role: "user",
      text: "  My exact\nmessage  ",
    });
    const sentMessage = human.find((e) => e.type === "message.updated")!.payload;
    const echoed = projectAttachmentLine({
      thread,
      parent: null,
      agentId: null,
      ordinal: 2,
      includeActivity: false,
      sentMessage: { ...sentMessage, creationSource: "web" },
      line: JSON.stringify({
        ...record,
        type: "user",
        isMeta: true,
        origin: { kind: "peer" },
        message: { content: "Another Claude session sent a message: wrapped transport text" },
      }),
    });
    expect(echoed.find((e) => e.type === "message.updated")?.payload).toMatchObject({
      role: "user",
      text: "  My exact\nmessage  ",
      creationSource: "web",
    });
    for (const extra of [
      { origin: { kind: "task-notification" }, promptSource: "system" },
      { origin: { kind: "peer" }, isMeta: true },
      { isCompactSummary: true, isVisibleInTranscriptOnly: true },
      { isMeta: true },
      { message: { content: "<command-name>/login</command-name>" } },
    ]) {
      const events = project(extra);
      expect(events.every(isEvent)).toBe(true);
      expect(events.find((e) => e.type === "message.updated")?.payload).toMatchObject({
        role: "system",
        createdBy: "system",
      });
      expect(events.find((e) => e.type === "turn-item.updated")?.payload.type).toBe(
        "system_notice",
      );
    }
  });
  it("does not let old backfill mark agents running and treats stale unfinished tails as unknown idle", () => {
    const input = {
      thread,
      parent: thread,
      agentId: "child",
      ordinal: 1,
      line: JSON.stringify({ ...record, message: { content: "Working", stop_reason: "tool_use" } }),
    };
    expect(projectAttachmentLine({ ...input, includeActivity: false }).map((e) => e.type)).toEqual([
      "message.updated",
      "turn-item.updated",
    ]);
    const stale = projectAttachmentLine({ ...input, activityStatus: "idle" }).find(
      (e) => e.type === "subagent.updated",
    );
    expect(stale?.payload).toMatchObject({
      status: "idle",
      result: null,
      progress: "No recent activity; final outcome not recorded by Claude.",
    });
    const finished = projectAttachmentLine({
      ...input,
      activityStatus: "idle",
      line: JSON.stringify({
        ...record,
        message: { content: "Done", stop_reason: "stop_sequence" },
      }),
    }).find((e) => e.type === "subagent.updated");
    expect(finished?.payload).toMatchObject({ status: "completed", result: "Done" });
    expect(finished?.payload.progress).toBeUndefined();
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

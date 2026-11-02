import { claudeLiveThread as thread } from "./claudeLive.testkit.ts";
import { expect, it } from "@effect/vitest";
import {
  OrchestrationV2DomainEvent,
  MessageId,
  type TurnItemId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { attachmentThreadId } from "./claudeAttachmentProjection.ts";
import {
  projectClaudeLiveFrame,
  claudeLiveStepMessageId,
  claudeLiveMessageEventId,
} from "./claudeLiveProjection.ts";
import type { ClaudeLiveBatch } from "../provider/ClaudeLiveTransport.ts";

const isDomainEvent = Schema.is(OrchestrationV2DomainEvent);

const at = DateTime.makeUnsafe("2026-11-02T12:00:00Z");
function projection(target = thread, agentId: string | null = null) {
  const messages = new Map<MessageId, OrchestrationV2ConversationMessage>();
  const items = new Map<TurnItemId, OrchestrationV2TurnItem>();
  const events: OrchestrationV2DomainEvent[] = [];
  let sequence = 0;
  return {
    messages,
    items,
    events,
    accept(type: ClaudeLiveBatch["events"][number]["type"], payload: unknown) {
      const result = projectClaudeLiveFrame({
        thread: target,
        parent: agentId === null ? null : thread,
        agentId,
        epoch: "epoch",
        frame: { sequence: ++sequence, type, payload },
        at,
        messages,
        items,
      });
      for (const event of result) {
        expect(isDomainEvent(event)).toBe(true);
        if (event.type === "message.updated") messages.set(event.payload.id, event.payload);
        if (event.type === "turn-item.updated") items.set(event.payload.id, event.payload);
      }
      events.push(...result);
      return result;
    },
  };
}

it("streams exact text and thinking into native rows and commits the transcript to the same message", () => {
  const p = projection();
  const step = { turnId: "turn", index: 0 };
  p.accept("step.chunk", { ...step, chunk: { kind: "thinking", index: 0, text: "Considering " } });
  p.accept("step.chunk", { ...step, chunk: { kind: "text", index: 1, text: "Hello " } });
  p.accept("step.chunk", { ...step, chunk: { kind: "text", index: 1, text: "world" } });
  expect([...p.messages.values()]).toMatchObject([{ text: "Hello world", streaming: true }]);
  expect([...p.items.values()].find((item) => item.type === "reasoning")).toMatchObject({
    text: "Considering ",
    streaming: true,
  });
  p.accept("step.complete", { ...step, result: { answer: "Hello world" } });
  const message = p.accept("message", {
    uuid: "native-uuid",
    door: "response",
    step: { ...step, blockIndex: 1 },
    message: {
      type: "assistant",
      role: "assistant",
      content: [{ type: "text", text: "Hello world" }],
    },
    origin: { kind: "model" },
  });
  expect(p.messages.size).toBe(1);
  expect(
    p.messages.get(
      MessageId.make(`${claudeLiveStepMessageId(thread.id, "epoch", "turn", 0)}:text:1`),
    ),
  ).toMatchObject({
    text: "Hello world",
    streaming: false,
  });
  expect(message[0]?.id).toBe(claudeLiveMessageEventId(thread.id, "native-uuid"));
  expect([...p.items.values()].filter((item) => item.type === "reasoning")).toMatchObject([
    { streaming: false, status: "completed" },
  ]);
});

it("keeps child streams separate even when their native turn and block indices match", () => {
  const root = projection();
  const childThread = { ...thread, id: attachmentThreadId("/claude", "session", "child") };
  const child = projection(childThread, "child");
  const step = { turnId: "same-native-turn", index: 0 };
  root.accept("step.chunk", { ...step, chunk: { kind: "text", index: 0, text: "Root" } });
  child.accept("step.chunk", { ...step, chunk: { kind: "text", index: 0, text: "Child" } });
  expect([...root.messages.keys()][0]).not.toBe([...child.messages.keys()][0]);
  expect([...root.messages.values()][0]?.text).toBe("Root");
  expect([...child.messages.values()][0]?.text).toBe("Child");
});

it("keeps partial output on an interrupted step and stops its thinking indicator", () => {
  const p = projection();
  const step = { turnId: "turn", index: 0 };
  p.accept("step.chunk", { ...step, chunk: { kind: "text", index: 0, text: "Partial" } });
  p.accept("step.chunk", { ...step, chunk: { kind: "thinking", index: 1, text: "Unfinished" } });
  p.accept("step.failed", step);
  expect([...p.messages.values()]).toMatchObject([{ text: "Partial", streaming: false }]);
  expect([...p.items.values()].find((item) => item.type === "reasoning")).toMatchObject({
    streaming: false,
    status: "failed",
  });
});

it("keeps two response blocks as two native messages instead of overwriting either block", () => {
  const p = projection();
  const step = { turnId: "turn", index: 0 };
  for (const [index, text] of ["First block.", "Second block."].entries()) {
    p.accept("step.chunk", { ...step, chunk: { kind: "text", index, text } });
    p.accept("message", {
      uuid: `native-${index}`,
      door: "response",
      step: { ...step, blockIndex: index },
      message: { type: "assistant", role: "assistant", content: [{ type: "text", text }] },
      origin: { kind: "model" },
    });
  }
  p.accept("step.complete", { ...step, result: { answer: "First block.\nSecond block." } });
  expect([...p.messages.values()].map((message) => message.text)).toEqual([
    "First block.",
    "Second block.",
  ]);
  expect(p.messages.size).toBe(2);
});

it("keeps native CLI prompt text human and excludes engine attachment records", () => {
  const p = projection();
  p.accept("message", {
    uuid: "cli-prompt",
    door: "prompt",
    origin: { kind: "unclassified" },
    message: {
      type: "user",
      role: "user",
      content: [{ type: "text", text: "<task-notification>Literal words</task-notification>" }],
    },
  });
  p.accept("message", {
    uuid: "engine-attachment",
    door: "attachment",
    origin: { kind: "engine" },
    message: {
      type: "attachment",
      role: "user",
      content: [{ type: "text", text: "Internal model instructions" }],
    },
  });
  expect([...p.messages.values()]).toMatchObject([
    { role: "user", text: "<task-notification>Literal words</task-notification>" },
  ]);
  expect(p.messages.size).toBe(1);
});

it("does not append delayed stream chunks to a block already completed by transcript backfill", () => {
  const p = projection();
  const step = { turnId: "turn", index: 0, blockIndex: 0 };
  p.accept("message", {
    uuid: "final",
    step,
    door: "response",
    message: {
      type: "assistant",
      role: "assistant",
      content: [{ type: "text", text: "Complete answer" }],
    },
  });
  p.accept("step.chunk", {
    turnId: "turn",
    index: 0,
    chunk: { kind: "text", index: 0, text: "Complete " },
  });
  p.accept("step.chunk", {
    turnId: "turn",
    index: 0,
    chunk: { kind: "text", index: 0, text: "answer" },
  });
  expect([...p.messages.values()]).toMatchObject([{ text: "Complete answer", streaming: false }]);
  expect(p.messages.size).toBe(1);
});

it("restores a known root turn from the reconnect snapshot without guessing that unknown work finished", () => {
  const p = projection();
  expect(p.accept("snapshot", { agents: [] })).toHaveLength(0);
  expect(
    p.accept("snapshot", { rootTurn: { turnId: "native-running", state: "running" } })[0]?.payload,
  ).toMatchObject({
    status: "running",
    nativeItemRef: { nativeId: "native-running" },
    completedAt: null,
  });
  expect(
    p.accept("snapshot", {
      rootTurn: { turnId: "native-running", state: "completed", reason: "aborted" },
    })[0]?.payload,
  ).toMatchObject({ status: "interrupted" });
});

it("replaces partial thinking with its complete native block and ignores delayed deltas", () => {
  const p = projection();
  const step = { turnId: "turn", index: 0, blockIndex: 0 };
  p.accept("step.chunk", { ...step, chunk: { kind: "thinking", index: 0, text: "Partial" } });
  const first = [...p.items.values()][0]!;
  p.accept("message", {
    uuid: "native-thinking",
    step,
    message: {
      type: "assistant",
      role: "assistant",
      content: [{ type: "thinking", thinking: "Complete native thinking" }],
    },
  });
  expect(p.items.size).toBe(1);
  expect(p.items.get(first.id)).toMatchObject({
    text: "Complete native thinking",
    streaming: false,
    ordinal: first.ordinal,
  });
  expect(
    p.accept("step.chunk", { ...step, chunk: { kind: "thinking", index: 0, text: " delayed" } }),
  ).toHaveLength(0);
  expect(p.messages.size).toBe(0);
});

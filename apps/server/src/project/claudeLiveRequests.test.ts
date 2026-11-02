import { expect, it } from "@effect/vitest";
import { OrchestrationV2DomainEvent, ProviderSessionId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { claudeLiveThread as thread } from "./claudeLive.testkit.ts";
import {
  claudeLiveRequestReply,
  liveRequestId,
  projectClaudeLiveRequest,
} from "./claudeLiveRequests.ts";
import type { ClaudeLiveRequestPayload } from "../provider/ClaudeLiveTransport.ts";

const isDomainEvent = Schema.is(OrchestrationV2DomainEvent);

const source: ClaudeLiveRequestPayload = {
  sessionId: "session",
  epoch: "process",
  requestId: "permission",
  kind: "permission",
  payload: { tool_name: "Bash", tool_input: { command: "printf hello" } },
};
const artifacts = (request = source) =>
  projectClaudeLiveRequest({
    thread,
    request,
    providerSessionId: ProviderSessionId.make("live-session"),
    at: thread.createdAt,
  });

it("uses native T3 approval records and preserves the exact command being approved", () => {
  const rows = artifacts();
  for (const row of rows) expect(isDomainEvent(row)).toBe(true);
  const request = rows.find((row) => row.type === "runtime-request.updated")!;
  expect(request.payload).toMatchObject({
    status: "pending",
    kind: "command",
    responseCapability: {
      type: "live",
      providerSessionId: "live-session",
      supportsFeedback: true,
    },
  });
  expect(rows.find((row) => row.type === "turn-item.updated")?.payload).toMatchObject({
    type: "approval_request",
    prompt: "printf hello",
  });
  expect(
    claudeLiveRequestReply(source, { ...request.payload, status: "resolved", decision: "accept" }),
  ).toMatchObject({
    type: "resolved",
    response: { behavior: "allow", updatedInput: { command: "printf hello" } },
  });
  expect(
    claudeLiveRequestReply(source, {
      ...request.payload,
      status: "resolved",
      decision: "decline",
      feedback: "Use the fixture",
    }),
  ).toMatchObject({ type: "resolved", response: { behavior: "deny", message: "Use the fixture" } });
  expect(liveRequestId(thread, source)).not.toBe(
    liveRequestId(thread, { ...source, epoch: "another-process" }),
  );
});

it("projects native question cards and returns answers in Claude's format", () => {
  const request = {
    ...source,
    kind: "question" as const,
    payload: {
      questions: [
        {
          question: "Which format?",
          header: "Format",
          options: [
            { label: "JSON", description: "Structured data" },
            { label: "CSV", description: "Spreadsheet" },
          ],
        },
      ],
    },
  };
  const rows = artifacts(request);
  for (const row of rows) expect(isDomainEvent(row)).toBe(true);
  const card = rows.find((row) => row.type === "turn-item.updated")!;
  expect(card.payload).toMatchObject({
    type: "user_input_request",
    questions: [{ question: "Which format?" }],
  });
  const pending = rows.find((row) => row.type === "runtime-request.updated")!;
  expect(
    claudeLiveRequestReply(request, {
      ...pending.payload,
      status: "resolved",
      answers: {
        "Which format?": ["JSON"],
      },
    }),
  ).toMatchObject({ type: "resolved", response: { answers: { "Which format?": "JSON" } } });
});

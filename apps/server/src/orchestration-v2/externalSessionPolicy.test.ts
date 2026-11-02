import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import { canMutateExternalSession } from "./externalSessionPolicy.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as EventSink from "./EventSink.ts";
import * as Registry from "./ProviderAdapterRegistry.ts";
import * as Harness from "./testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";

it("permits organization but refuses execution and provider changes for external sessions", () => {
  expect(canMutateExternalSession("thread.visit")).toBe(true);
  expect(canMutateExternalSession("thread.archive")).toBe(true);
  expect(canMutateExternalSession("message.dispatch")).toBe(false);
  expect(canMutateExternalSession("subagent.stop")).toBe(false);
  expect(canMutateExternalSession("thread.stop")).toBe(false);
});

const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("External attachment must never launch a provider"),
} as Adapter.ProviderAdapterV2["Service"];
const layer = Harness.layerWithRegistry(
  { name: "external-read-only" },
  Registry.layerFromAdapters([adapter]),
  { runEffectWorker: false },
);

it.effect("rejects a new prompt at the orchestrator before a run can be created", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;

    const sink = yield* EventSink.EventSinkV2;
    const threadId = ThreadId.make("external-thread");
    const modelSelection = { instanceId, model: "gpt-5.1-codex" };
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-external"),
      threadId,
      projectId: ProjectId.make("project"),
      title: "External session",
      modelSelection,
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const thread = yield* orchestrator
      .getThreadRecords(threadId, [])
      .pipe(Effect.map((records) => records.thread));
    yield* sink.write({
      events: [
        {
          id: EventId.make("attach-external"),
          type: "thread.metadata-updated",
          threadId,
          occurredAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z"),
          payload: {
            ...thread,
            externalSession: {
              provider: "claude",
              sessionId: "session",
              configDirectory: "/claude",
              transcriptPath: "/claude/projects/session.jsonl",
              attached: true,
              state: "following",
            },
          },
        },
      ],
    });
    const error = yield* orchestrator
      .dispatch({
        type: "message.dispatch",
        dispatchMode: { type: "start_immediately" },
        commandId: CommandId.make("cannot-send"),
        threadId,
        messageId: MessageId.make("cannot-send"),
        text: "Do work",
        attachments: [],
        modelSelection,
        createdBy: "user",
        creationSource: "web",
      })
      .pipe(Effect.flip);
    expect(error.message).toContain("external Claude");
    expect((yield* orchestrator.getThreadRecords(threadId, ["runs"])).runs).toHaveLength(0);
  }).pipe(Effect.provide(layer)),
);

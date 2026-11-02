import { expect, it } from "@effect/vitest";
import { ProviderSessionId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Transport from "../provider/ClaudeLiveTransport.ts";
import * as Ingestor from "./ClaudeLiveEventIngestor.ts";
import { attachmentEvent, attachmentThreadId } from "./claudeAttachmentProjection.ts";
import { claudeLiveThread as thread } from "./claudeLive.testkit.ts";

const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
);
const persistence = Layer.merge(stores, EventSink.layer.pipe(Layer.provide(stores)));
const layer = Layer.merge(Ingestor.layer.pipe(Layer.provideMerge(persistence)), Transport.layer);
const token = "local-test-token-".repeat(4);
const batch: Transport.ClaudeLiveBatch = {
  protocol: 1,
  sessionId: "session",
  epoch: "source-process",
  acknowledged: 0,
  events: [
    {
      sequence: 1,
      type: "step.chunk",
      payload: { turnId: "turn", index: 0, chunk: { kind: "text", index: 0, text: "Hello " } },
    },
    {
      sequence: 2,
      type: "step.chunk",
      payload: { turnId: "turn", index: 0, chunk: { kind: "text", index: 0, text: "world" } },
    },
  ],
};

it.effect(
  "replays a lost acknowledgement from persisted projections without duplicating text",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const ingest = yield* Ingestor.ClaudeLiveEventIngestor;
      const transport = yield* Transport.ClaudeLiveTransport;
      yield* sink.write({
        events: [attachmentEvent(thread.id, "thread.created", thread, thread.createdAt, "create")],
      });
      let replies = 0;
      yield* transport.register({
        id: "binding",
        sessionId: "session",
        token,
        request: () => Effect.succeed({ type: "pending" }),
        receive: (incoming) =>
          Effect.gen(function* () {
            const acknowledged = yield* ingest.ingest({
              root: thread,
              batch: incoming,
              resolveChild: () =>
                Effect.fail(new Transport.ClaudeLiveTransportError({ reason: "unavailable" })),
            });
            // The transaction committed, but the connection died before its reply arrived.
            if (++replies === 1)
              return yield* new Transport.ClaudeLiveTransportError({
                reason: "persistence-failed",
              });
            return { acknowledged };
          }),
      });
      yield* transport.receive("binding", token, batch).pipe(Effect.flip);
      expect((yield* projections.getThreadRecords(thread.id, ["messages"])).messages).toMatchObject(
        [{ text: "Hello world", streaming: true }],
      );
      const next: Transport.ClaudeLiveBatch = {
        ...batch,
        events: [
          ...batch.events,
          {
            sequence: 3,
            type: "step.chunk",
            payload: { turnId: "turn", index: 0, chunk: { kind: "text", index: 0, text: "!" } },
          },
        ],
      };
      expect(yield* transport.receive("binding", token, next)).toEqual({ acknowledged: 3 });
      const messages = (yield* projections.getThreadRecords(thread.id, ["messages"])).messages;
      expect(messages).toHaveLength(1);
      expect(messages[0]?.text).toBe("Hello world!");
      expect(yield* transport.receive("binding", token, next)).toEqual({ acknowledged: 3 });
      expect((yield* projections.getThreadRecords(thread.id, ["messages"])).messages[0]?.text).toBe(
        "Hello world!",
      );
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("does not append live data after the attachment is detached", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const ingest = yield* Ingestor.ClaudeLiveEventIngestor;
    const detached = {
      ...thread,
      externalSession: { ...thread.externalSession!, attached: false },
    };
    yield* sink.write({
      events: [attachmentEvent(thread.id, "thread.created", detached, thread.createdAt, "create")],
    });
    const error = yield* ingest
      .ingest({
        root: thread,
        batch,
        resolveChild: () =>
          Effect.fail(new Transport.ClaudeLiveTransportError({ reason: "unavailable" })),
      })
      .pipe(Effect.flip);
    expect(error.reason).toBe("unavailable");
    expect((yield* projections.getThreadRecords(thread.id, ["messages"])).messages).toEqual([]);
  }).pipe(Effect.provide(layer)),
);

it.effect("projects authoritative agent snapshots without declaring unlisted agents finished", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const ingest = yield* Ingestor.ClaudeLiveEventIngestor;
    const child = {
      ...thread,
      id: attachmentThreadId("/claude", "session", "child"),
      lineage: { ...thread.lineage, parentThreadId: thread.id },
    };
    yield* sink.write({
      events: [thread, child].map((row) =>
        attachmentEvent(row.id, "thread.created", row, row.createdAt, "create"),
      ),
    });
    const input = {
      root: thread,
      resolveChild: () => Effect.succeed(child),
      batch: {
        ...batch,
        events: [
          {
            sequence: 1,
            type: "snapshot" as const,
            payload: {
              agents: [{ id: "child", description: "Repair validation", status: "waiting" }],
            },
          },
        ],
      },
    };
    yield* ingest.ingest(input);
    expect((yield* projections.getThreadRecords(thread.id, ["subagents"])).subagents).toMatchObject(
      [{ childThreadId: child.id, status: "waiting", title: "Repair validation" }],
    );
    yield* ingest.ingest({
      ...input,
      batch: { ...batch, events: [{ sequence: 2, type: "snapshot", payload: { agents: [] } }] },
    });
    expect(
      (yield* projections.getThreadRecords(thread.id, ["subagents"])).subagents[0]?.status,
    ).toBe("waiting");
    yield* ingest.ingest({
      ...input,
      batch: {
        ...batch,
        events: [
          {
            sequence: 3,
            type: "snapshot",
            payload: {
              agents: [{ id: "child", description: "Repair validation", status: "idle" }],
            },
          },
        ],
      },
    });
    expect(
      (yield* projections.getThreadRecords(thread.id, ["subagents"])).subagents[0]?.status,
    ).toBe("idle");
  }).pipe(Effect.provide(layer)),
);

it.effect("reuses a persisted pending request and delivers its saved answer after reconnect", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const ingest = yield* Ingestor.ClaudeLiveEventIngestor;
    yield* sink.write({
      events: [attachmentEvent(thread.id, "thread.created", thread, thread.createdAt, "create")],
    });
    const input = {
      thread,
      providerSessionId: ProviderSessionId.make("live-session"),
      request: {
        sessionId: "session",
        epoch: "original-process",
        requestId: "approval",
        kind: "permission" as const,
        payload: { tool_name: "Bash", tool_input: { command: "printf fixture" } },
      },
    };
    expect(yield* ingest.request(input)).toEqual({ type: "pending" });
    expect(yield* ingest.request(input)).toEqual({ type: "pending" });
    const rows = yield* projections.getThreadRecords(thread.id, ["runtimeRequests", "turnItems"]);
    expect(rows.runtimeRequests).toHaveLength(1);
    expect(rows.turnItems).toHaveLength(1);
    const request = rows.runtimeRequests[0]!;
    // Normal command dispatch persists this resolution before executing its response effect.
    yield* sink.write({
      events: [
        attachmentEvent(
          thread.id,
          "runtime-request.updated",
          {
            ...request,
            status: "resolved",
            decision: "accept",
            resolvedAt: thread.createdAt,
          },
          thread.createdAt,
          "answer",
        ),
      ],
    });
    const reply = yield* ingest.request(input);
    expect(reply).toMatchObject({
      type: "resolved",
      response: {
        behavior: "allow",
        updatedInput: { command: "printf fixture" },
      },
    });
    expect(yield* ingest.request(input)).toEqual(reply);
    expect(
      (yield* projections.getThreadRecords(thread.id, ["runtimeRequests"])).runtimeRequests,
    ).toHaveLength(1);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "declines unsupported elicitation without displaying an approval that cannot be fulfilled",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const ingest = yield* Ingestor.ClaudeLiveEventIngestor;
      yield* sink.write({
        events: [attachmentEvent(thread.id, "thread.created", thread, thread.createdAt, "create")],
      });
      for (const payload of [
        {
          mode: "url",
          message: "Authorize",
          mcp_server_name: "fixture",
          url: "https://example.invalid",
        },
        {
          mode: "form",
          message: "Enter token",
          mcp_server_name: "fixture",
          requested_schema: {
            type: "object",
            properties: { token: { type: "string" } },
            required: ["token"],
          },
        },
      ]) {
        expect(
          yield* ingest.request({
            thread,
            providerSessionId: ProviderSessionId.make("live-session"),
            request: {
              sessionId: "session",
              epoch: "original-process",
              requestId: "unsupported",
              kind: "elicitation",
              payload,
            },
          }),
        ).toEqual({ type: "resolved", response: { action: "decline" } });
      }
      const rows = yield* projections.getThreadRecords(thread.id, ["runtimeRequests", "turnItems"]);
      expect(rows.runtimeRequests).toHaveLength(0);
      expect(rows.turnItems).toHaveLength(0);
    }).pipe(Effect.provide(layer)),
);

import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import {
  ClaudeLiveTransport,
  ClaudeLiveTransportError,
  layer,
  type ClaudeLiveBatch,
  type ClaudeLiveBinding,
} from "./ClaudeLiveTransport.ts";

const token = "fixture-token-".repeat(4);
const batch: ClaudeLiveBatch = {
  protocol: 1,
  sessionId: "native-session",
  epoch: "original-process",
  acknowledged: 0,
  events: [{ sequence: 1, type: "turn.start", payload: { turnId: "native-turn" } }],
};
const binding = (receive: ClaudeLiveBinding["receive"]): ClaudeLiveBinding => ({
  id: "binding",
  sessionId: batch.sessionId,
  token,
  receive,
  request: () => Effect.succeed({ type: "pending" }),
});

it.effect("authenticates a single native session before exposing its payload to the adapter", () =>
  Effect.gen(function* () {
    const transport = yield* ClaudeLiveTransport;
    let calls = 0;
    yield* transport.register(
      binding(() => {
        calls++;
        return Effect.succeed({ acknowledged: 1 });
      }),
    );
    expect((yield* transport.receive("binding", "wrong", batch).pipe(Effect.flip)).reason).toBe(
      "unauthorized",
    );
    expect(
      (yield* transport
        .receive("binding", token, { ...batch, sessionId: "another" })
        .pipe(Effect.flip)).reason,
    ).toBe("unauthorized");
    expect(calls).toBe(0);
    expect(yield* transport.receive("binding", token, batch)).toEqual({ acknowledged: 1 });
    expect(calls).toBe(1);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("never acknowledges failed persistence and permits replay of the identical batch", () =>
  Effect.gen(function* () {
    const transport = yield* ClaudeLiveTransport;
    const inputs: ClaudeLiveBatch[] = [];
    yield* transport.register(
      binding((input) => {
        inputs.push(input);
        return inputs.length === 1
          ? Effect.fail(new ClaudeLiveTransportError({ reason: "persistence-failed" }))
          : Effect.succeed({ acknowledged: 1 });
      }),
    );
    expect((yield* transport.receive("binding", token, batch).pipe(Effect.flip)).reason).toBe(
      "persistence-failed",
    );
    expect(yield* transport.receive("binding", token, batch)).toEqual({ acknowledged: 1 });
    expect(inputs[0]).toEqual(inputs[1]);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("serializes cursor writes without holding up approval responses", () =>
  Effect.gen(function* () {
    const transport = yield* ClaudeLiveTransport;
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let active = 0;
    let maximum = 0;
    yield* transport.register(
      binding(() =>
        Effect.gen(function* () {
          active++;
          maximum = Math.max(maximum, active);
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
          active--;
          return { acknowledged: 1 };
        }),
      ),
    );
    const first = yield* transport.receive("binding", token, batch).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    const second = yield* transport.receive("binding", token, batch).pipe(Effect.forkChild);
    expect(
      yield* transport.request("binding", token, {
        sessionId: batch.sessionId,
        epoch: batch.epoch,
        requestId: "approval",
        kind: "permission",
        payload: {},
      }),
    ).toEqual({ type: "pending" });
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(first);
    yield* Fiber.join(second);
    expect(maximum).toBe(1);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect(
  "rejects lost or reordered records unless the source explicitly requests reconciliation",
  () =>
    Effect.gen(function* () {
      const transport = yield* ClaudeLiveTransport;
      yield* transport.register(
        binding((input) =>
          Effect.succeed({ acknowledged: input.events.at(-1)?.sequence ?? input.acknowledged }),
        ),
      );
      for (const events of [
        [{ sequence: 2, type: "turn.start", payload: {} }],
        [
          { sequence: 1, type: "turn.start", payload: {} },
          { sequence: 1, type: "turn.complete", payload: {} },
        ],
      ]) {
        expect(
          (yield* transport.receive("binding", token, { ...batch, events }).pipe(Effect.flip))
            .reason,
        ).toBe("invalid-batch");
      }
      expect(
        yield* transport.receive("binding", token, {
          ...batch,
          events: [{ sequence: 500, type: "resync", payload: {} }],
        }),
      ).toEqual({ acknowledged: 500 });
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("closing the T3 registration leaves the source alone and allows reconnect", () =>
  Effect.gen(function* () {
    const transport = yield* ClaudeLiveTransport;
    const connectionScope = yield* Scope.make();
    yield* transport
      .register(binding(() => Effect.succeed({ acknowledged: 1 })))
      .pipe(Effect.provideService(Scope.Scope, connectionScope));
    yield* Scope.close(connectionScope, Exit.void);
    expect((yield* transport.receive("binding", token, batch).pipe(Effect.flip)).reason).toBe(
      "unavailable",
    );
    yield* transport.register(binding(() => Effect.succeed({ acknowledged: 1 })));
    expect(yield* transport.receive("binding", token, batch)).toEqual({ acknowledged: 1 });
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

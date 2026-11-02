import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { HttpRouter, HttpServerRequest } from "effect/http";
import { handle } from "./ClaudeLiveHttp.ts";
import { ClaudeLiveTransport, layer } from "./ClaudeLiveTransport.ts";

const token = "fixture-token-".repeat(4);
const invoke = (authorization: string, body: string, origin?: string, requestOnly = false) =>
  handle(requestOnly).pipe(
    Effect.provideService(
      HttpServerRequest.HttpServerRequest,
      HttpServerRequest.fromWeb(
        new Request("http://127.0.0.1/api/provider-live/claude/binding", {
          method: "POST",
          body,
          headers: {
            authorization,
            "content-type": "application/json",
            ...(origin ? { origin } : {}),
          },
        }),
      ),
    ),
    Effect.provideService(HttpRouter.RouteContext, {
      params: { bindingId: "binding" },
      route: HttpRouter.route("POST", "/api/provider-live/claude/:bindingId", Effect.die("unused")),
    }),
  );

it.effect("rejects unauthorized and browser requests before parsing their bodies", () =>
  Effect.gen(function* () {
    const transport = yield* ClaudeLiveTransport;
    let called = false;
    yield* transport.register({
      id: "binding",
      sessionId: "session",
      token,
      receive: () => {
        called = true;
        return Effect.succeed({ acknowledged: 0 });
      },
      request: () => Effect.succeed({ type: "pending" }),
    });
    expect((yield* invoke("Bearer wrong", "invalid JSON")).status).toBe(401);
    expect((yield* invoke(`Bearer ${token}`, "invalid JSON", "http://localhost")).status).toBe(403);
    expect(called).toBe(false);
    expect((yield* invoke(`Bearer ${token}`, "invalid JSON")).status).toBe(400);
    expect(
      (yield* invoke(
        `Bearer ${token}`,
        JSON.stringify({
          protocol: 1,
          sessionId: "session",
          epoch: "epoch",
          acknowledged: 0,
          events: [],
        }),
      )).status,
    ).toBe(200);
    expect(called).toBe(true);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("keeps requests retryable while server bindings are being restored", () =>
  Effect.gen(function* () {
    expect((yield* invoke(`Bearer ${token}`, "{}", undefined, true)).status).toBe(503);
  }).pipe(Effect.provide(layer)),
);

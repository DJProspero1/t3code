import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { FetchHttpClient } from "effect/http";
import * as Waiter from "./ClaudeLiveRequestWaiter.ts";

const request = {
  url: "http://127.0.0.1:12345/live/request",
  token: "test-token",
  sessionId: "session",
  epoch: "epoch",
  requestId: "approval",
  kind: "permission" as const,
  payload: { tool_name: "Bash", tool_input: { command: "echo fixture" } },
};
const layer = Waiter.layer.pipe(Layer.provide(FetchHttpClient.layer));

it.live("keeps the same pending approval through an unavailable and restarting T3 server", () =>
  Effect.gen(function* () {
    const waiter = yield* Waiter.ClaudeLiveRequestWaiter;
    let attempts = 0;
    const bodies: unknown[] = [];
    const result = yield* waiter.wait(request).pipe(
      Effect.provideService(FetchHttpClient.Fetch, async (_url, options) => {
        bodies.push(JSON.parse(String(options?.body)));
        attempts++;
        if (attempts === 1) throw new Error("Connection refused");
        if (attempts === 2) return new Response("Restarting", { status: 503 });
        if (attempts === 3) return new Response("Routes not ready", { status: 404 });
        return Response.json({ type: "resolved", response: { behavior: "allow" } });
      }),
    );
    expect(result).toEqual({ type: "resolved", response: { behavior: "allow" } });
    expect(bodies).toHaveLength(4);
    expect(bodies[0]).toEqual(bodies[1]);
    expect(bodies[1]).toEqual(bodies[2]);
    expect(bodies[2]).toEqual(bodies[3]);
    expect(bodies[0]).not.toHaveProperty("token");
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "rejects remote endpoints before any request and reports explicit detach as cancellation",
  () =>
    Effect.gen(function* () {
      const waiter = yield* Waiter.ClaudeLiveRequestWaiter;
      let calls = 0;
      const fetch: typeof globalThis.fetch = async () => {
        calls++;
        return new Response(null, { status: 410 });
      };
      const invalid = yield* waiter
        .wait({ ...request, url: "https://example.com" })
        .pipe(Effect.flip);
      expect(invalid.reason).toBe("invalid-endpoint");
      expect(calls).toBe(0);
      expect(
        yield* waiter.wait(request).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)),
      ).toEqual({ type: "cancelled" });
      expect(calls).toBe(1);
    }).pipe(Effect.provide(layer)),
);

it.effect("refuses redirects instead of forwarding an approval payload to another endpoint", () =>
  Effect.gen(function* () {
    const waiter = yield* Waiter.ClaudeLiveRequestWaiter;
    const error = yield* waiter.wait(request).pipe(
      Effect.provideService(FetchHttpClient.Fetch, async (_url, init) => {
        expect(init?.redirect).toBe("manual");
        return new Response(null, {
          status: 307,
          headers: { location: "https://example.com/collect" },
        });
      }),
      Effect.flip,
    );
    expect(error.reason).toBe("refused");
  }).pipe(Effect.provide(layer)),
);

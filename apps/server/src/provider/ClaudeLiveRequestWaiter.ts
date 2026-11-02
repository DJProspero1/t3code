import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

const Payload = Schema.Struct({
  sessionId: Schema.String,
  epoch: Schema.String,
  requestId: Schema.String,
  kind: Schema.Literals(["permission", "question", "elicitation"]),
  payload: Schema.Unknown,
});
export const ClaudeLiveRequest = Schema.Struct({
  ...Payload.fields,
  url: Schema.String,
  token: Schema.String,
});
const encodePayload = Schema.encodeEffect(Schema.fromJsonString(Payload));

const Response = Schema.Union([
  Schema.Struct({ type: Schema.Literal("resolved"), response: Schema.Unknown }),
  Schema.Struct({ type: Schema.Literal("pending") }),
  Schema.Struct({ type: Schema.Literal("cancelled") }),
]);
const decodeResponse = Schema.decodeUnknownEffect(Schema.fromJsonString(Response));
export const encodeResponse = Schema.encodeEffect(Schema.fromJsonString(Response));

export class ClaudeLiveRequestError extends Schema.TaggedError<ClaudeLiveRequestError>()(
  "ClaudeLiveRequestError",
  { reason: Schema.Literals(["invalid-endpoint", "refused", "invalid-response"]) },
) {
  override get message() {
    return `The Claude live request could not be delivered (${this.reason}).`;
  }
}

export class ClaudeLiveRequestWaiter extends Context.Service<
  ClaudeLiveRequestWaiter,
  {
    readonly wait: (
      request: typeof ClaudeLiveRequest.Type,
    ) => Effect.Effect<typeof Response.Type, ClaudeLiveRequestError>;
  }
>()("t3/provider/ClaudeLiveRequestWaiter") {}

const make = Effect.gen(function* () {
  const http = yield* HttpClient.HttpClient;
  const wait = Effect.fn("ClaudeLiveRequestWaiter.wait")(function* (
    request: typeof ClaudeLiveRequest.Type,
  ) {
    const url = yield* Effect.try({
      try: () => new URL(request.url),
      catch: () => new ClaudeLiveRequestError({ reason: "invalid-endpoint" }),
    });
    if (url.protocol !== "http:" || url.hostname !== "127.0.0.1" || url.username || url.password)
      return yield* new ClaudeLiveRequestError({ reason: "invalid-endpoint" });
    const { url: _, token, ...payload } = request;
    const body = yield* encodePayload(payload).pipe(
      Effect.mapError(() => new ClaudeLiveRequestError({ reason: "invalid-response" })),
    );
    const startedAt = yield* Clock.currentTimeMillis;
    // Claude limits one process.run to ten minutes; renew the same pending request before that.
    while ((yield* Clock.currentTimeMillis) - startedAt < 9 * 60 * 1_000) {
      const response = yield* HttpClientRequest.post(url.href).pipe(
        HttpClientRequest.setHeader("authorization", `Bearer ${token}`),
        HttpClientRequest.bodyText(body, "application/json"),
        http.execute,
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
        Effect.flatMap((response) =>
          Effect.map(response.text, (body) => ({ status: response.status, body })),
        ),
        Effect.timeout("30 seconds"),
        Effect.orElseSucceed(() => null),
      );
      if (response?.status === 200) {
        const result = yield* decodeResponse(response.body).pipe(
          Effect.mapError(() => new ClaudeLiveRequestError({ reason: "invalid-response" })),
        );
        if (result.type !== "pending") return result;
      } else if (response?.status === 410) {
        return { type: "cancelled" as const };
      } else if (
        response &&
        response.status < 500 &&
        ![202, 404, 408, 429].includes(response.status)
      ) {
        return yield* new ClaudeLiveRequestError({ reason: "refused" });
      }
      yield* Effect.sleep("250 millis");
    }
    return { type: "pending" as const };
  });
  return ClaudeLiveRequestWaiter.of({ wait });
});

export const layer = Layer.effect(ClaudeLiveRequestWaiter, make);

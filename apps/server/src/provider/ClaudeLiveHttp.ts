import * as ByteSize from "effect/ByteSize";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  HttpIncomingMessage,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import { ClaudeLiveTransport, ClaudeLiveTransportError } from "./ClaudeLiveTransport.ts";

/** Session-scoped bearer authentication; no cookies, forwarded identity, or browser CORS. */
export const handle = Effect.fn("ClaudeLiveHttp.handle")(
  function* (requestOnly: boolean) {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const transport = yield* ClaudeLiveTransport;
    const { bindingId } = yield* HttpRouter.params;
    const authorization = request.headers.authorization;
    if (!bindingId || !authorization?.startsWith("Bearer ") || authorization.length > 512)
      return HttpServerResponse.empty({ status: 401 });
    // Browsers cannot use this private provider endpoint, even from an authenticated T3 tab.
    if (request.headers.origin !== undefined) return HttpServerResponse.empty({ status: 403 });
    const token = authorization.slice(7);
    yield* transport.authenticate(bindingId, token);
    const body = yield* request.json.pipe(
      Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.mebibytes(2)),
      Effect.mapError(() => new ClaudeLiveTransportError({ reason: "invalid-batch" })),
    );
    const reply = requestOnly
      ? yield* transport.request(bindingId, token, body)
      : yield* transport.receive(bindingId, token, body);
    return HttpServerResponse.jsonUnsafe(reply, { headers: { "cache-control": "no-store" } });
  },
  (effect) =>
    effect.pipe(
      Effect.catchTags({
        ClaudeLiveTransportError: (error) =>
          Effect.succeed(
            HttpServerResponse.empty({
              status:
                error.reason === "unauthorized"
                  ? 401
                  : error.reason === "invalid-batch"
                    ? 400
                    : 503,
            }),
          ),
      }),
    ),
);

export const layer = Layer.mergeAll(
  HttpRouter.add("POST", "/api/provider-live/claude/:bindingId", handle(false)),
  HttpRouter.add("POST", "/api/provider-live/claude/:bindingId/request", handle(true)),
);

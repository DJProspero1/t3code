// @effect-diagnostics-next-line nodeBuiltinImport:off - constant-time authentication at the local provider transport boundary.
import * as NodeCrypto from "node:crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

const Identity = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const Sequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
export const ClaudeLiveBatch = Schema.Struct({
  protocol: Schema.Literal(1),
  sessionId: Identity,
  epoch: Identity,
  acknowledged: Sequence,
  events: Schema.Array(
    Schema.Struct({
      sequence: Schema.Int.check(Schema.isGreaterThan(0)),
      at: Schema.optional(Schema.String),
      type: Schema.Literals([
        "snapshot",
        "resync",
        "turn.start",
        "turn.complete",
        "step.start",
        "step.chunk",
        "step.complete",
        "step.failed",
        "message",
        "tool.start",
        "tool.complete",
        "tool.failed",
        "agent.start",
        "agent.stop",
        "request.open",
        "request.closed",
        "command.result",
      ]),
      payload: Schema.Unknown,
    }),
  ).check(Schema.isMaxLength(256)),
});
export type ClaudeLiveBatch = typeof ClaudeLiveBatch.Type;

export const ClaudeLiveRequestPayload = Schema.Struct({
  sessionId: Identity,
  epoch: Identity,
  requestId: Identity,
  kind: Schema.Literals(["permission", "question", "elicitation"]),
  payload: Schema.Unknown,
});
export type ClaudeLiveRequestPayload = typeof ClaudeLiveRequestPayload.Type;
export type ClaudeLiveRequestReply =
  | { readonly type: "pending" }
  | { readonly type: "cancelled" }
  | { readonly type: "resolved"; readonly response: unknown };
export interface ClaudeLiveReply {
  readonly acknowledged: number;
  readonly commands?: ReadonlyArray<{
    readonly id: string;
    readonly epoch: string;
    readonly type: string;
    readonly [key: string]: unknown;
  }>;
  readonly completedCommands?: ReadonlyArray<string>;
}

export class ClaudeLiveTransportError extends Schema.TaggedError<ClaudeLiveTransportError>()(
  "ClaudeLiveTransportError",
  {
    reason: Schema.Literals([
      "unavailable",
      "unauthorized",
      "conflict",
      "invalid-batch",
      "persistence-failed",
    ]),
  },
) {
  override get message() {
    return `Claude live transport: ${this.reason}.`;
  }
}

export interface ClaudeLiveBinding {
  readonly id: string;
  readonly sessionId: string;
  readonly token: string;
  /** Resolve only after native events and their replay identities have committed together. */
  readonly receive: (
    batch: ClaudeLiveBatch,
  ) => Effect.Effect<ClaudeLiveReply, ClaudeLiveTransportError>;
  /** Read/resolve the existing T3 runtime request, retaining its id across reconnects. */
  readonly request: (
    request: ClaudeLiveRequestPayload,
  ) => Effect.Effect<ClaudeLiveRequestReply, ClaudeLiveTransportError>;
}

export class ClaudeLiveTransport extends Context.Service<
  ClaudeLiveTransport,
  {
    readonly authenticate: (
      id: string,
      token: string,
    ) => Effect.Effect<void, ClaudeLiveTransportError>;
    /** Owns a connection registration only. Closing it never signals the provider process. */
    readonly register: (
      binding: ClaudeLiveBinding,
    ) => Effect.Effect<void, ClaudeLiveTransportError, Scope.Scope>;
    readonly receive: (
      id: string,
      token: string,
      input: unknown,
    ) => Effect.Effect<ClaudeLiveReply, ClaudeLiveTransportError>;
    readonly request: (
      id: string,
      token: string,
      input: unknown,
    ) => Effect.Effect<ClaudeLiveRequestReply, ClaudeLiveTransportError>;
  }
>()("t3/provider/ClaudeLiveTransport") {}

const decodeBatch = Schema.decodeUnknownEffect(ClaudeLiveBatch);
const decodeRequest = Schema.decodeUnknownEffect(ClaudeLiveRequestPayload);
const digest = (value: string) => NodeCrypto.createHash("sha256").update(value).digest();

export const layer = Layer.effect(
  ClaudeLiveTransport,
  Effect.sync(() => {
    const bindings = new Map<
      string,
      { binding: ClaudeLiveBinding; tokenHash: Uint8Array; gate: Semaphore.Semaphore }
    >();
    const authorized = Effect.fn("ClaudeLiveTransport.authorized")((id: string, token: string) =>
      Effect.suspend(() => {
        const entry = bindings.get(id);
        // A request may arrive while the server is restoring bindings. Do not cancel it.
        if (!entry) return Effect.fail(new ClaudeLiveTransportError({ reason: "unavailable" }));
        if (!NodeCrypto.timingSafeEqual(entry.tokenHash, digest(token)))
          return Effect.fail(new ClaudeLiveTransportError({ reason: "unauthorized" }));
        return Effect.succeed(entry);
      }),
    );
    return ClaudeLiveTransport.of({
      authenticate: Effect.fn("ClaudeLiveTransport.authenticate")((id: string, token: string) =>
        authorized(id, token).pipe(Effect.asVoid),
      ),
      register: Effect.fn("ClaudeLiveTransport.register")(function* (binding) {
        if (binding.token.length < 32)
          return yield* new ClaudeLiveTransportError({ reason: "unauthorized" });
        const entry = { binding, tokenHash: digest(binding.token), gate: yield* Semaphore.make(1) };
        yield* Effect.acquireRelease(
          Effect.suspend(() => {
            if (bindings.has(binding.id))
              return Effect.fail(new ClaudeLiveTransportError({ reason: "conflict" }));
            bindings.set(binding.id, entry);
            return Effect.void;
          }),
          () =>
            Effect.sync(() => {
              if (bindings.get(binding.id) === entry) bindings.delete(binding.id);
            }),
        );
      }),
      receive: Effect.fn("ClaudeLiveTransport.receive")(function* (id, token, input) {
        const entry = yield* authorized(id, token);
        const batch = yield* decodeBatch(input).pipe(
          Effect.mapError(() => new ClaudeLiveTransportError({ reason: "invalid-batch" })),
        );
        if (batch.sessionId !== entry.binding.sessionId)
          return yield* new ClaudeLiveTransportError({ reason: "unauthorized" });
        let previous = batch.acknowledged;
        for (const event of batch.events) {
          if (
            event.sequence <= previous ||
            (event.sequence !== previous + 1 && event.type !== "resync")
          )
            return yield* new ClaudeLiveTransportError({ reason: "invalid-batch" });
          previous = event.sequence;
        }
        return yield* Effect.gen(function* () {
          if (bindings.get(id) !== entry)
            return yield* new ClaudeLiveTransportError({ reason: "unavailable" });
          const reply = yield* entry.binding
            .receive(batch)
            .pipe(
              Effect.mapError(() => new ClaudeLiveTransportError({ reason: "persistence-failed" })),
            );
          if (
            !Number.isSafeInteger(reply.acknowledged) ||
            reply.acknowledged < batch.acknowledged ||
            reply.acknowledged > previous ||
            reply.commands?.some((command) => command.epoch !== batch.epoch)
          )
            return yield* new ClaudeLiveTransportError({ reason: "invalid-batch" });
          if (bindings.get(id) !== entry)
            return yield* new ClaudeLiveTransportError({ reason: "unavailable" });
          return reply;
        }).pipe(entry.gate.withPermits(1));
      }),
      request: Effect.fn("ClaudeLiveTransport.request")(function* (id, token, input) {
        const entry = yield* authorized(id, token);
        const request = yield* decodeRequest(input).pipe(
          Effect.mapError(() => new ClaudeLiveTransportError({ reason: "invalid-batch" })),
        );
        if (request.sessionId !== entry.binding.sessionId)
          return yield* new ClaudeLiveTransportError({ reason: "unauthorized" });
        // Approval polling must not hold the stream gate while the provider is waiting.
        return yield* entry.binding
          .request(request)
          .pipe(
            Effect.mapError(() => new ClaudeLiveTransportError({ reason: "persistence-failed" })),
          );
      }),
    });
  }),
);

import { NodeWS } from "@effect/platform-node/NodeSocket";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Socket from "effect/socket/Socket";
import * as CodexClient from "./client.ts";

const makeServer = Effect.fn("test.makeServer")(function* () {
  const server = yield* Effect.acquireRelease(
    Effect.sync(() => new NodeWS.WebSocketServer({ host: "127.0.0.1", port: 0 })),
    (server) =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            for (const client of server.clients) client.terminate();
            server.close(() => resolve());
          }),
      ),
  );
  if (!server.address()) {
    yield* Effect.promise(() => new Promise<void>((resolve) => server.once("listening", resolve)));
  }
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing server port");
  return { server, url: `ws://127.0.0.1:${address.port}` };
});

const connect = Effect.fn("test.connect")(function* (url: string, scope: Scope.Scope) {
  const context = yield* Layer.buildWithScope(
    CodexClient.layerWebSocket().pipe(
      Layer.provide(Socket.layerWebSocket(url)),
      Layer.provide(Socket.layerWebSocketConstructorGlobal),
    ),
    scope,
  );
  return yield* CodexClient.CodexAppServerClient.pipe(Effect.provide(context));
});

it.effect("routes live notifications and approval replies over WebSocket frames", () =>
  Effect.gen(function* () {
    const { server, url } = yield* makeServer();
    const approval = Promise.withResolvers<unknown>();
    server.on("connection", (peer) =>
      peer.on("message", (data, binary) => {
        expect(binary).toBe(false);
        const message = JSON.parse(data.toString());
        if (message.method === "initialize") {
          peer.send(
            JSON.stringify({
              id: message.id,
              result: {
                userAgent: "existing-server",
                codexHome: "/test",
                platformFamily: "unix",
                platformOs: "linux",
              },
            }),
          );
        } else if (message.method === "initialized") {
          peer.send(
            JSON.stringify({
              method: "item/agentMessage/delta",
              params: {
                threadId: "root",
                turnId: "turn",
                itemId: "item",
                delta: "Olá 🐈",
              },
            }),
          );
          peer.send(
            JSON.stringify({
              id: "approval",
              method: "item/tool/requestUserInput",
              params: {
                threadId: "root",
                turnId: "turn",
                itemId: "tool",
                isBlocking: true,
                questions: [],
              },
            }),
          );
        } else if (message.id === "approval") {
          approval.resolve(message.result);
        }
      }),
    );
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const client = yield* connect(url, scope);
    const deltas = yield* Queue.unbounded<string>();
    yield* client.handleServerNotification("item/agentMessage/delta", (event) =>
      Queue.offer(deltas, event.delta).pipe(Effect.asVoid),
    );
    yield* client.handleServerRequest("item/tool/requestUserInput", () =>
      Effect.succeed({ answers: { confirmation: { answers: ["yes"] } } }),
    );
    const initialized = yield* client.request("initialize", {
      clientInfo: { name: "t3-test", version: "0.0.0" },
    });
    expect(initialized.userAgent).toBe("existing-server");
    yield* client.notify("initialized", undefined);
    expect(yield* Queue.take(deltas)).toBe("Olá 🐈");
    expect(yield* Effect.promise(() => approval.promise)).toEqual({
      answers: { confirmation: { answers: ["yes"] } },
    });
  }),
);

it.effect("detaching one client preserves another connection and allows reconnect", () =>
  Effect.gen(function* () {
    const { server, url } = yield* makeServer();
    let connections = 0;
    server.on("connection", (peer) => {
      connections++;
      peer.on("message", (data) => {
        const message = JSON.parse(data.toString());
        peer.send(JSON.stringify({ id: message.id, result: { connectionCount: connections } }));
      });
    });
    const firstScope = yield* Scope.make();
    const secondScope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
    yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));
    const first = yield* connect(url, firstScope);
    const second = yield* connect(url, secondScope);
    expect(yield* first.raw.request("probe")).toEqual({ connectionCount: 2 });
    yield* Scope.close(firstScope, Exit.void);
    expect(yield* second.raw.request("probe")).toEqual({ connectionCount: 2 });
    const thirdScope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(thirdScope, Exit.void));
    const third = yield* connect(url, thirdScope);
    expect(yield* third.raw.request("probe")).toEqual({ connectionCount: 3 });
    expect(yield* second.raw.request("probe")).toEqual({ connectionCount: 3 });
  }),
);

it.effect("fails pending requests when the server disconnects", () =>
  Effect.gen(function* () {
    const { server, url } = yield* makeServer();
    server.on("connection", (peer) => peer.on("message", () => peer.close(1001, "restart")));
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const client = yield* connect(url, scope);
    const terminated = yield* Effect.forkChild(Effect.exit(client.awaitTermination));
    expect(Exit.isFailure(yield* Effect.exit(client.raw.request("probe")))).toBe(true);
    expect(Exit.isFailure(yield* Fiber.join(terminated))).toBe(true);
    expect(Exit.isFailure(yield* Effect.exit(client.raw.request("probe")))).toBe(true);
  }),
);

it.effect("detaching releases pending calls without waiting for the server", () =>
  Effect.gen(function* () {
    const { server, url } = yield* makeServer();
    const received = Promise.withResolvers<void>();
    server.on("connection", (peer) => peer.on("message", () => received.resolve()));
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const client = yield* connect(url, scope);
    const pending = yield* Effect.forkChild(Effect.exit(client.raw.request("probe")));
    yield* Effect.promise(() => received.promise);
    yield* Scope.close(scope, Exit.void);
    expect(Exit.isFailure(yield* Fiber.join(pending))).toBe(true);
    expect(Exit.isFailure(yield* Effect.exit(client.awaitTermination))).toBe(true);
  }),
);

it.effect("returns command approval decisions to the requesting connection", () =>
  Effect.gen(function* () {
    const { server, url } = yield* makeServer();
    const reply = Promise.withResolvers<unknown>();
    server.on("connection", (peer) =>
      peer.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.method === "initialized") {
          peer.send(
            JSON.stringify({
              id: 17,
              method: "item/commandExecution/requestApproval",
              params: {
                threadId: "child",
                turnId: "child-turn",
                itemId: "command",
                startedAtMs: 123,
                command: "git status",
                cwd: "/test",
              },
            }),
          );
        } else reply.resolve(message);
      }),
    );
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const client = yield* connect(url, scope);
    yield* client.handleServerRequest("item/commandExecution/requestApproval", (request) => {
      expect(request.threadId).toBe("child");
      expect(request.command).toBe("git status");
      return Effect.succeed({ decision: "decline" });
    });
    yield* client.notify("initialized", undefined);
    expect(yield* Effect.promise(() => reply.promise)).toEqual({
      id: 17,
      result: { decision: "decline" },
    });
  }),
);

// @effect-diagnostics-next-line nodeBuiltinImport:off - the fixture accepts WebSocket upgrades on a native Unix socket.
import * as NodeHttp from "node:http";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import { expect, it } from "@effect/vitest";
import { CodexSettings, ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import {
  NoOpProviderEventLoggers,
  ProviderEventLoggers,
} from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { ChildProcessSpawner } from "effect/process";
import { CodexAppServerClientFactory, layerAppServerClientFactory } from "./CodexAdapterV2.ts";

const decodeSettings = Schema.decodeEffect(CodexSettings);

it.effect(
  "connects to the existing Unix daemon without spawning it and leaves it alive on detach",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-codex-daemon-" });
      const socketPath = `${directory}/control with spaces:fixture.sock`;
      const http = NodeHttp.createServer();
      const server = yield* Effect.acquireRelease(
        Effect.sync(() => new NodeWS.WebSocketServer({ server: http })),
        (server) =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                for (const peer of server.clients) peer.terminate();
                server.close(() => http.close(() => resolve()));
              }),
          ),
      );
      yield* Effect.promise(() => new Promise<void>((resolve) => http.listen(socketPath, resolve)));
      let connections = 0;
      server.on("connection", (peer) => {
        connections++;
        peer.on("message", (data) => {
          const request = JSON.parse(data.toString());
          peer.send(
            JSON.stringify({
              id: request.id,
              result: { data: ["already-running"], nextCursor: null },
            }),
          );
        });
      });
      let spawns = 0;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const context = yield* Layer.build(
        layerAppServerClientFactory.pipe(
          Layer.provide(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
          Layer.provide(
            Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
              ...spawner,
              spawn: () => {
                spawns++;
                return Effect.die("Attachment must not spawn a provider");
              },
            }),
          ),
        ),
      );
      const factory = yield* CodexAppServerClientFactory.pipe(Effect.provide(context));
      const input = {
        instanceId: ProviderInstanceId.make("codex"),
        threadId: ThreadId.make("attached"),
        providerSessionId: ProviderSessionId.make("external-codex"),
        runtimePolicy: {
          runtimeMode: "approval-required" as const,
          interactionMode: "default" as const,
          cwd: null,
        },
        settings: yield* decodeSettings({}),
        environment: {},
        daemonSocketPath: socketPath,
      };
      const firstScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
      const first = yield* factory.open(input).pipe(Effect.provideService(Scope.Scope, firstScope));
      expect(yield* first.raw.request("thread/loaded/list", {})).toEqual({
        data: ["already-running"],
        nextCursor: null,
      });
      yield* Scope.close(firstScope, Exit.void);
      expect(http.listening).toBe(true);
      const second = yield* factory.open(input);
      expect(yield* second.raw.request("thread/loaded/list", {})).toEqual({
        data: ["already-running"],
        nextCursor: null,
      });
      expect(connections).toBe(2);
      expect(spawns).toBe(0);
      const invalid = yield* Effect.exit(
        factory.open({ ...input, daemonSocketPath: "relative.sock" }),
      );
      expect(Exit.isFailure(invalid)).toBe(true);
      expect(spawns).toBe(0);
    }).pipe(Effect.provide(NodeServices.layer)),
);

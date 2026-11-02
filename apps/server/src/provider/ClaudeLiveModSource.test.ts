import * as NodeVM from "node:vm";
// @effect-diagnostics-next-line nodeBuiltinImport:off - the isolated VM fixture provides Claude's Web Crypto global.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import { claudeLiveModSource } from "./ClaudeLiveModSource.ts";

type Data = Record<string, unknown>;
type Event = { sequence: number; type: string; payload: Data };
type Batch = { sessionId: string; epoch: string; acknowledged: number; events: Event[] };
type Handler = (...args: unknown[]) => unknown;

function harness(url = "http://127.0.0.1:12345/live", interactive = false) {
  const handlers = new Map<string, Handler>();
  const batches: Batch[] = [];
  const calls: Data[] = [];
  let outgoing: (batch: Batch) => Data[] = () => [];
  let response: unknown = { behavior: "allow" };
  const requests: Data[] = [];
  const aliases: Data[] = [];
  let tick: (() => Promise<void>) | undefined;
  let fail = false;
  let invalidAck = false;
  let agentStatus = "running";
  let failSnapshot = false;
  let configured = true;
  let time = 1_000_000;
  const engine = {
    plugin: { root: "/plugin" },
    session: {
      id: async () => "session",
      model: async () => "claude-test",
      version: async () => ({ version: "2.1.289" }),
    },
    agent: {
      list: async () => {
        if (failSnapshot) throw new Error("Provider snapshot unavailable");
        return [{ id: "child", status: agentStatus }];
      },
    },
    prompt: {
      submit: async (input: Data) => {
        calls.push(input);
        return input;
      },
    },
    fs: {
      write: async (_path: string, value: string) => {
        aliases.push(JSON.parse(value));
      },
      exists: async () => true,
      read: async () =>
        JSON.stringify(
          configured
            ? {
                session: {
                  url,
                  token: "a".repeat(32),
                  aliasesDirectory: "/plugin/aliases",
                  ...(interactive ? { waitCommand: ["fake-t3", "thread", "_bridge-request"] } : {}),
                },
              }
            : {},
        ),
    },
    process: {
      run: async (_argv: string[], init: { stdin: string }) => {
        requests.push(JSON.parse(init.stdin) as Data);
        return { exitCode: 0, stdout: JSON.stringify({ type: "resolved", response }) };
      },
    },
    clock: {
      every: (_ms: number, callback: () => Promise<void>) => {
        tick = callback;
        return {
          cancel: () => {
            tick = undefined;
          },
        };
      },
    },
    http: {
      fetch: async (_url: string, options: { body: string }) => {
        const batch = JSON.parse(options.body) as Batch;
        batches.push(batch);
        if (fail) throw new Error("T3 restarting");
        return {
          ok: true,
          text: JSON.stringify({
            acknowledged: invalidAck
              ? Number.MAX_SAFE_INTEGER
              : (batch.events.at(-1)?.sequence ?? batch.acknowledged),
            commands: outgoing(batch),
          }),
        };
      },
    },
  };
  const register = NodeVM.runInNewContext(
    claudeLiveModSource.replace("export function register", "function register") + "\nregister;",
    {
      URL,
      crypto: { randomUUID: NodeCrypto.randomUUID },
      Date: class extends Date {
        static override now() {
          return time;
        }
      },
    },
  ) as (on: (name: string, handler: Handler) => { catch: (fallback: Handler) => void }) => void;
  register((name, handler) => {
    handlers.set(name, handler);
    return {
      catch: (fallback) => {
        handlers.set(name, async (...args) => {
          try {
            return await handler(...args);
          } catch {
            return fallback(...args);
          }
        });
      },
    };
  });
  const call = (name: string, event: Data, next: Handler = async (e) => e) => {
    const handler = handlers.get(name);
    if (!handler) throw new Error(`Missing hook ${name}`);
    return handler(engine, event, next);
  };
  return {
    call,
    configure: (value: boolean) => {
      configured = value;
    },
    advance: (ms: number) => {
      time += ms;
    },
    batches,
    calls,
    requests,
    aliases,
    response: (value: unknown) => {
      response = value;
    },
    commands: (value: typeof outgoing) => {
      outgoing = value;
    },
    flush: async () => {
      await tick?.();
    },
    fail: (value: boolean) => {
      fail = value;
    },
    invalidAck: (value: boolean) => {
      invalidAck = value;
    },
    failSnapshot: () => {
      failSnapshot = true;
    },
    agentStatus: (value: string) => {
      agentStatus = value;
    },
  };
}

it("preserves root and child streaming chunks, including opaque provider chunks", async () => {
  const h = harness();
  await h.call("session.start", {});
  const chunks = [
    { kind: "engine", ref: 7 },
    { kind: "thinking", index: 0, text: "Considering" },
    { kind: "text", index: 1, text: "Answer" },
  ];
  const result = { turnId: "turn", index: 0, answer: "Answer" };
  for (const agentId of [undefined, "child"]) {
    const next = () =>
      Object.assign(
        (async function* () {
          for (const chunk of chunks) yield chunk;
          return result;
        })(),
        { result: Promise.resolve(result) },
      );
    const stream = h.call(
      "turn.step",
      { turnId: "turn", index: 0, agentId },
      next,
    ) as AsyncGenerator<Data, Data>;
    const seen = [];
    for (;;) {
      const item = await stream.next();
      if (item.done) {
        expect(item.value).toEqual(result);
        break;
      }
      seen.push(item.value);
    }
    expect(seen).toEqual(chunks);
  }
  await h.flush();
  const streamed = h.batches
    .flatMap((batch) => batch.events)
    .filter((event) => event.type === "step.chunk");
  expect(streamed.map((event) => event.payload.agentId)).toEqual([
    undefined,
    undefined,
    "child",
    "child",
  ]);
  expect(streamed.map((event) => event.payload.chunk)).toEqual([
    chunks[1],
    chunks[2],
    chunks[1],
    chunks[2],
  ]);
});

it("submits exact human text once and refuses commands from a prior connection epoch", async () => {
  const h = harness();
  await h.call("session.start", {});
  h.commands((batch) => [
    { id: "stale", epoch: "previous-process", type: "prompt", text: "Do not send" },
    {
      id: "prompt",
      epoch: batch.epoch,
      type: "prompt",
      text: "<task-notification>Literal user text</task-notification>",
    },
  ]);
  await h.flush();
  await h.flush();
  expect(h.calls).toEqual([
    { text: "<task-notification>Literal user text</task-notification>", asUser: true },
  ]);
  expect(
    h.batches.flatMap((batch) => batch.events).filter((event) => event.type === "command.result"),
  ).toHaveLength(1);
});

it("replays identical event IDs after disconnect and ignores an invalid acknowledgement", async () => {
  const h = harness();
  await h.call("session.start", {});
  h.fail(true);
  await h.flush();
  await h.call("turn.start", { turnId: "turn" });
  h.fail(false);
  h.invalidAck(true);
  await h.flush();
  h.invalidAck(false);
  await h.flush();
  expect(h.batches[1]?.events[0]).toEqual(h.batches[0]?.events[0]);
  expect(h.batches[2]).toEqual(h.batches[1]);
  await h.flush();
  expect(h.batches).toHaveLength(3);
});

it("records stored message content and actual child status without rewriting provider results", async () => {
  const h = harness();
  await h.call("session.start", {});
  const stored = { uuid: "message", message: { role: "user", content: "Literal input" } };
  expect(
    await h.call("session.append", { uuid: "message", agentId: "child" }, async () => stored),
  ).toBe(stored);
  h.agentStatus("idle");
  await h.call("classic.SubagentStop", { agent_id: "child" });
  await h.flush();
  const events = h.batches.flatMap((batch) => batch.events);
  expect(events.find((event) => event.type === "message")?.payload.message).toEqual(stored.message);
  expect(events.at(-1)?.payload.agents).toEqual([{ id: "child", status: "idle" }]);
});

it("bounds disconnected streaming history and explicitly requests transcript reconciliation", async () => {
  const h = harness();
  await h.call("session.start", {});
  for (let i = 0; i < 20; i++) {
    await h.call("session.append", {}, async () => ({
      uuid: String(i),
      message: { content: "x".repeat(100_000) },
    }));
  }
  await h.flush();
  expect(h.batches[0]?.events[0]?.type).toBe("resync");
  expect(JSON.stringify(h.batches[0]).length).toBeLessThan(1_100_000);
});

it("does not activate against a remote endpoint", async () => {
  const h = harness("https://example.com/collect");
  await h.call("session.start", {});
  await h.call("turn.start", { turnId: "turn" });
  await h.flush();
  expect(h.batches).toHaveLength(0);
});

it("routes approvals without overriding a decision from existing permission hooks", async () => {
  const h = harness(undefined, true);
  await h.call("session.start", {});
  const event = { tool_name: "Bash", tool_input: { command: "echo fixture" } };
  const denied = { decision: { behavior: "deny", message: "Existing policy" } };
  expect(await h.call("classic.PermissionRequest", event, async () => denied)).toBe(denied);
  expect(h.requests).toHaveLength(0);
  expect(await h.call("classic.PermissionRequest", event, async () => ({}))).toEqual({
    decision: { behavior: "allow" },
  });
  expect(h.requests[0]).toMatchObject({ sessionId: "session", kind: "permission", payload: event });
  h.response({ behavior: "invalid" });
  expect(await h.call("classic.PermissionRequest", event, async () => ({}))).toMatchObject({
    decision: { behavior: "deny" },
  });
});

it("answers questions and MCP elicitation through the same request transport", async () => {
  const h = harness(undefined, true);
  await h.call("session.start", {});
  h.response({ answers: { "Which?": "One" } });
  const questions = [
    { question: "Which?", header: "Pick", options: [{ label: "One" }, { label: "Two" }] },
  ];
  const question = await h.call(
    "tool.call",
    { tool: "AskUserQuestion", tool_use_id: "ask", questions },
    () => {
      throw new Error("Must not open a terminal dialog");
    },
  );
  expect(question).toEqual({ result: { questions, answers: { "Which?": "One" } } });
  h.response({ action: "accept", content: { answer: "One" } });
  expect(await h.call("classic.Elicitation", { message: "Choose" }, async () => ({}))).toEqual({
    action: "accept",
    content: { answer: "One" },
  });
  expect(h.requests.map((request) => request.kind)).toEqual(["question", "elicitation"]);
});

it("keeps the original work running when native status telemetry fails", async () => {
  const h = harness();
  h.failSnapshot();
  let calls = 0;
  const next = async () => {
    calls++;
    return { original: true };
  };
  expect(await h.call("session.start", {}, next)).toEqual({ original: true });
  expect(await h.call("turn.complete", { turnId: "turn" }, next)).toEqual({ original: true });
  expect(await h.call("classic.SubagentStop", { agent_id: "child" }, next)).toEqual({
    original: true,
  });
  await h.flush();
  expect(calls).toBe(3);
  expect(h.batches.flatMap((batch) => batch.events).map((event) => event.type)).toEqual([
    "turn.complete",
    "agent.stop",
  ]);
});

it("recovers a dropped command result without submitting its prompt again", async () => {
  const h = harness();
  await h.call("session.start", {});
  h.commands((batch) => [
    { id: "prompt", epoch: batch.epoch, type: "prompt", text: "Exactly once" },
  ]);
  await h.flush();
  // Overflow before the result is acknowledged, as during a long disconnected turn.
  for (let i = 0; i < 20; i++) {
    await h.call("session.append", {}, async () => ({
      uuid: String(i),
      message: { content: "x".repeat(100_000) },
    }));
  }
  await h.flush();
  expect(h.batches[1]?.events[0]?.type).toBe("resync");
  await h.flush();
  expect(h.calls).toEqual([{ text: "Exactly once", asUser: true }]);
  expect(
    h.batches[2]?.events.find((event) => event.type === "command.result")?.payload,
  ).toMatchObject({
    id: "prompt",
    result: { text: "Exactly once", asUser: true },
  });
});

it("correlates queued T3 prompts with native transcript UUIDs without changing stored rows", async () => {
  const h = harness();
  await h.call("session.start", {});
  h.commands((batch) => [
    { id: "first", epoch: batch.epoch, type: "prompt", text: "Same text" },
    { id: "second", epoch: batch.epoch, type: "prompt", text: "Same text" },
  ]);
  await h.flush();
  h.commands(() => []);
  const origin = { kind: "plugin", name: "t3-live-session", asUser: true };
  for (const uuid of ["native-first", "native-second"]) {
    const result = { uuid, message: { role: "user", content: "Same text" } };
    expect(await h.call("session.append", { door: "prompt", origin }, async () => result)).toBe(
      result,
    );
  }
  await h.flush();
  const messages = h.batches
    .flatMap((batch) => batch.events)
    .filter((event) => event.type === "message");
  expect(messages.map((event) => [event.payload.commandId, event.payload.uuid])).toEqual([
    ["first", "native-first"],
    ["second", "native-second"],
  ]);
});

it("flushes terminal events when the CLI exits between timer ticks", async () => {
  const h = harness();
  await h.call("session.start", {});
  await h.flush();
  await h.call("turn.complete", { turnId: "turn", reason: "answer" });
  await h.call("session.end", { reason: "prompt_input_exit" });
  expect(h.batches.at(-1)?.events.some((event) => event.type === "turn.complete")).toBe(true);
  const count = h.batches.length;
  await h.flush();
  expect(h.batches).toHaveLength(count);
});

it("publishes a prompt identity before Claude exposes its transcript row", async () => {
  const h = harness();
  await h.call("session.start", {});
  h.commands((batch) => [
    { id: "t3-message", epoch: batch.epoch, type: "prompt", text: "Exact text" },
  ]);
  await h.flush();
  const message = { type: "user", role: "user", content: [{ type: "text", text: "Exact text" }] };
  await h.call(
    "session.append",
    {
      uuid: "native-message",
      door: "prompt",
      message,
      origin: { kind: "plugin", name: "t3-live-session", asUser: true },
    },
    async () => {
      expect(h.aliases).toMatchObject([
        { uuid: "native-message", commandId: "t3-message", role: "user", agentId: null },
      ]);
      return { uuid: "native-message", message };
    },
  );
});

it("coalesces unsent text but never changes a frame whose acknowledgement was lost", async () => {
  const h = harness();
  await h.call("session.start", {});
  const next = () =>
    Object.assign(
      (async function* () {
        yield { kind: "text", index: 0, text: "A" };
        yield { kind: "text", index: 0, text: "B" };
        yield { kind: "text", index: 0, text: "C" };
      })(),
      { result: Promise.resolve({ answer: "ABC" }) },
    );
  const stream = h.call("turn.step", { turnId: "turn", index: 0 }, next) as AsyncGenerator<Data>;
  await stream.next();
  h.fail(true);
  await h.flush();
  await stream.next();
  await stream.next();
  h.fail(false);
  await h.flush();
  const frames = h.batches.at(-1)!.events.filter((event) => event.type === "step.chunk");
  expect(frames.map((frame) => (frame.payload.chunk as Data).text)).toEqual(["A", "BC"]);
  expect(frames[1]!.sequence).toBe(frames[0]!.sequence + 1);
  await stream.next();
});

it("activates a preloaded connection without reload and retains its replay epoch on reattachment", async () => {
  const h = harness();
  h.configure(false);
  await h.call("session.start", {});
  await h.flush();
  expect(h.batches).toHaveLength(0);
  h.configure(true);
  h.advance(2001);
  await h.flush();
  expect(h.batches[0]?.events[0]?.type).toBe("snapshot");
  const epoch = h.batches[0]?.epoch;
  h.configure(false);
  h.advance(2001);
  await h.flush();
  const before = h.batches.length;
  await h.call("turn.start", { turnId: "detached" });
  await h.flush();
  expect(h.batches).toHaveLength(before);
  h.configure(true);
  h.advance(2001);
  await h.flush();
  expect(h.batches.at(-1)?.epoch).toBe(epoch);
  expect(h.batches.at(-1)?.events.some((event) => event.type === "snapshot")).toBe(true);
  await h.call("session.end", {});
});

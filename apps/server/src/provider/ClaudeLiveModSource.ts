// Claude loads this source itself; bundling it would rewrite the capability calls its validator reads.
export const claudeLiveModSource = String.raw`
let connection;
let sessionId;
let epoch;
let sequence = 0;
let acknowledged = 0;
let sealedThrough = 0;
let events = [];
let queuedBytes = 0;
let sending = false;
let timer;
let timerInterval;
let lastConfigRead = 0;
let readingConfig = false;
let lastContact = 0;
let lastSnapshot = 0;
let snapshotFingerprint;
let snapshotting = false;
const commands = new Map();
const steps = new Map();
const pendingPrompts = [];

function jsonBytes(value) {
  let bytes = 0;
  for (const char of JSON.stringify(value)) {
    const code = char.codePointAt(0);
    bytes += code < 0x80 ? 1 : code < 0x800 ? 2 : code < 0x10000 ? 3 : 4;
  }
  return bytes;
}

function record(type, payload) {
  if (!connection) return;
  try {
    const previous = events[events.length - 1];
    const field = payload?.chunk?.kind === 'input' ? 'json' : 'text';
    if (type === 'step.chunk' && previous?.type === type && previous.sequence > sealedThrough &&
      previous.payload.turnId === payload.turnId && previous.payload.agentId === payload.agentId &&
      previous.payload.index === payload.index && previous.payload.chunk?.kind === payload.chunk?.kind &&
      previous.payload.chunk?.index === payload.chunk?.index &&
      typeof previous.payload.chunk?.[field] === 'string' && typeof payload.chunk?.[field] === 'string' &&
      previous.payload.chunk[field].length + payload.chunk[field].length <= 4096) {
      const merged = { ...previous, payload: { ...payload, chunk: { ...payload.chunk,
        [field]: previous.payload.chunk[field] + payload.chunk[field] } } };
      const bytes = queuedBytes - jsonBytes(previous) + jsonBytes(merged);
      if (bytes <= 1024 * 1024) {
        events[events.length - 1] = merged;
        queuedBytes = bytes;
        return;
      }
    }
    const event = { sequence: sequence + 1, at: new Date().toISOString(), type, payload };
    const size = jsonBytes(event);
    sequence = event.sequence;
    if (queuedBytes + size > 1024 * 1024) {
      events = [{ sequence: ++sequence, at: event.at, type: 'resync', payload: {} }];
      queuedBytes = 64;
      snapshotFingerprint = undefined;
      return;
    }
    events.push(event);
    queuedBytes += size;
  } catch {
    // Observing a provider event must never change whether its original work runs.
  }
}

async function snapshot($) {
  if (snapshotting) return;
  snapshotting = true;
  try {
    const payload = {
      model: await $.session.model(),
      agents: await $.agent.list(),
      version: await $.session.version(),
    };
    const fingerprint = JSON.stringify(payload);
    if (fingerprint !== snapshotFingerprint) {
      record('snapshot', payload);
      snapshotFingerprint = fingerprint;
    }
    lastSnapshot = Date.now();
  } catch {
    // Missing telemetry is not evidence that the session or its children stopped.
  } finally {
    snapshotting = false;
  }
}

async function execute($, command) {
  if (command.epoch !== epoch || typeof command.id !== 'string') return;
  const previous = commands.get(command.id);
  if (previous) {
    if (previous.state === 'completed') record('command.result', previous.outcome);
    return;
  }
  commands.set(command.id, { state: 'pending' });
  let outcome;
  try {
    let result;
    switch (command.type) {
      case 'prompt':
        if (typeof command.text !== 'string') throw new Error('Invalid prompt');
        pendingPrompts.push(command.id);
        try {
          result = await $.prompt.submit({ text: command.text, asUser: true });
          if (result.drop !== undefined) {
            const index = pendingPrompts.indexOf(command.id);
            if (index >= 0) pendingPrompts.splice(index, 1);
          }
        } catch (error) {
          const index = pendingPrompts.indexOf(command.id);
          if (index >= 0) pendingPrompts.splice(index, 1);
          throw error;
        }
        break;
      case 'model':
        if (typeof command.model !== 'string') throw new Error('Invalid model');
        result = await $.config.set({ key: 'model', value: command.model });
        break;
      case 'agent.send':
        if (typeof command.agentId !== 'string' || typeof command.text !== 'string') throw new Error('Invalid agent message');
        result = await $.session.send({ to: { agentId: command.agentId }, text: command.text });
        break;
      case 'agent.stop':
        if (typeof command.taskId !== 'string') throw new Error('Invalid task');
        result = await $.tool.call({ tool: 'TaskStop', task_id: command.taskId });
        break;
      case 'turn.abort':
        if (typeof command.turnId !== 'string') throw new Error('Invalid turn');
        result = await $.turn.abort({ turnId: command.turnId });
        break;
      default:
        throw new Error('Unsupported live command');
    }
    outcome = { id: command.id, result };
  } catch (error) {
    outcome = { id: command.id, error: String(error) };
  } finally {
    commands.set(command.id, { state: 'completed', outcome });
    record('command.result', outcome);
  }
}

async function request($, kind, payload) {
  const requestId = crypto.randomUUID();
  record('request.open', { requestId, kind, payload, agentId: payload.agentId ?? payload.agent_id });
  try {
    for (;;) {
      const result = await $.process.run(connection.waitCommand, {
        stdin: JSON.stringify({ url: connection.url + '/request', token: connection.token, sessionId, epoch, requestId, kind, payload }),
        timeoutMs: 600000,
      });
      if (result.exitCode !== 0 || result.isStdoutTruncated) throw new Error('Live request transport failed');
      const reply = JSON.parse(result.stdout);
      if (reply.type === 'pending') continue;
      if (reply.type === 'cancelled') return null;
      if (reply.type !== 'resolved') throw new Error('Invalid live request response');
      return reply.response;
    }
  } finally {
    record('request.closed', { requestId, agentId: payload.agentId ?? payload.agent_id });
  }
}

async function flush($) {
  if (!connection || sending || (events.length === 0 && Date.now() - lastContact < 500)) return;
  sending = true;
  const batch = events.slice(0, 256);
  // Even a failed HTTP response may have committed. Never mutate a transmitted frame.
  sealedThrough = Math.max(sealedThrough, batch[batch.length - 1]?.sequence ?? acknowledged);
  try {
    const response = await $.http.fetch(connection.url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: 'Bearer ' + connection.token },
      body: JSON.stringify({ protocol: 1, sessionId, epoch, acknowledged, events: batch }),
    });
    if (!response.ok) return;
    const reply = JSON.parse(response.text);
    const last = batch.length ? batch[batch.length - 1].sequence : acknowledged;
    if (!Number.isSafeInteger(reply.acknowledged) || reply.acknowledged < acknowledged || reply.acknowledged > last) return;
    acknowledged = reply.acknowledged;
    events = events.filter(event => event.sequence > acknowledged);
    queuedBytes = events.reduce((sum, event) => sum + jsonBytes(event), 0);
    lastContact = Date.now();
    if (Array.isArray(reply.commands)) {
      for (const command of reply.commands) {
        if (command && typeof command === 'object') void execute($, command);
      }
    }
    if (Array.isArray(reply.completedCommands)) {
      for (const id of reply.completedCommands) {
        if (commands.get(id)?.state === 'completed') commands.delete(id);
      }
    }
  } catch {
    // Keep unacknowledged events for reconnect; the provider never waits for T3.
  } finally {
    sending = false;
  }
}

function armTimer($, interval) {
  if (timer && timerInterval === interval) return;
  if (timer) timer.cancel();
  timerInterval = interval;
  timer = $.clock.every(interval, async () => {
    if (Date.now() - lastConfigRead >= 2000) await refreshConnection($);
    if (connection && Date.now() - lastSnapshot >= 2000) void snapshot($);
    await flush($);
  });
}

async function refreshConnection($) {
  if (readingConfig) return;
  readingConfig = true;
  lastConfigRead = Date.now();
  try {
    const path = $.plugin.root + '/connections.json';
    if (!(await $.fs.exists(path))) return;
    const configured = JSON.parse(await $.fs.read(path))[sessionId];
    if (!configured) {
      connection = undefined;
      armTimer($, 2000);
      return;
    }
    const url = new URL(configured.url);
    if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || url.username || url.password) return;
    if (typeof configured.token !== 'string' || configured.token.length < 32) return;
    const waitCommand = Array.isArray(configured.waitCommand) && configured.waitCommand.length > 0 && configured.waitCommand.every(arg => typeof arg === 'string') ? configured.waitCommand : undefined;
    const wasConnected = connection !== undefined;
    connection = { url: url.href, token: configured.token, waitCommand, aliasesDirectory: configured.aliasesDirectory };
    armTimer($, 50);
    if (!wasConnected) {
      snapshotFingerprint = undefined;
      await snapshot($);
    }
  } catch {
    // A transient read or atomic settings replacement must not cancel native work.
  } finally {
    readingConfig = false;
  }
}

export function register(on) {
  on('session.start', async ($, e, next) => {
    sessionId = await $.session.id();
    epoch = crypto.randomUUID();
    sequence = 0;
    acknowledged = 0;
    sealedThrough = 0;
    events = [];
    queuedBytes = 0;
    armTimer($, 2000);
    await refreshConnection($);
    return next(e);
  }).catch(($, e, next) => next(e));

  on('session.end', async ($, e, next) => {
    // The exiting CLI can otherwise discard the final step between timer ticks.
    const until = Date.now() + 200;
    while (sending && Date.now() < until) await $.clock.sleep(5);
    await flush($);
    if (timer) timer.cancel();
    return next(e);
  }).catch(($, e, next) => next(e));

  on('turn.start', async ($, e, next) => {
    record('turn.start', e);
    return next(e);
  });

  on('turn.step', async function* ($, e, next) {
    steps.set(e.agentId ?? '', { turnId: e.turnId, index: e.index });
    record('step.start', e);
    const stream = next(e);
    try {
      for await (const chunk of stream) {
        if (Number.isInteger(chunk.index)) steps.set(e.agentId ?? '', { turnId: e.turnId, index: e.index, blockIndex: chunk.index });
        if (chunk.kind !== 'engine') record('step.chunk', { turnId: e.turnId, agentId: e.agentId, index: e.index, chunk });
        yield chunk;
      }
      const result = await stream.result;
      record('step.complete', { turnId: e.turnId, agentId: e.agentId, index: e.index, result });
      return result;
    } catch (error) {
      record('step.failed', { turnId: e.turnId, agentId: e.agentId, index: e.index });
      throw error;
    }
  });

  on('session.append', async ($, e, next) => {
    const commandId = e.door === 'prompt' && e.origin?.kind === 'plugin' && e.origin.name === 't3-live-session' && e.origin.asUser === true
      ? pendingPrompts.shift() : undefined;
    const step = e.door === 'response' ? steps.get(e.agentId ?? '') : undefined;
    // Publish the identity before Claude makes the JSONL row visible to its follower.
    // This also survives a lost live batch or a server restart.
    if (connection && typeof connection.aliasesDirectory === 'string' && /^[a-zA-Z0-9_-]{1,128}$/.test(e.uuid) && (step || commandId)) {
      try {
        await $.fs.write(connection.aliasesDirectory + '/' + e.uuid + '.json', JSON.stringify({
          version: 1, epoch, uuid: e.uuid, role: e.message?.role ?? e.message?.type,
          agentId: e.agentId ?? null, step, commandId,
        }));
      } catch {
        // Failure to record observer metadata must not fail Claude's append.
      }
    }
    const result = await next(e);
    if (result.message?.type !== undefined && !['user', 'assistant'].includes(result.message.type)) return result;
    record('message', { ...e, message: result.message, uuid: result.uuid, commandId, step });
    return result;
  });

  on('tool.call', async ($, e, next) => {
    record('tool.start', e);
    try {
      let result;
      if (connection?.waitCommand && e.tool === 'AskUserQuestion') {
        const response = await request($, 'question', e);
        result = response === null ? { deny: 'Question cancelled' } : { result: { questions: e.questions, ...response } };
      } else {
        result = await next(e);
      }
      record('tool.complete', { tool_use_id: e.tool_use_id, agentId: e.agentId, result });
      return result;
    } catch (error) {
      record('tool.failed', { tool_use_id: e.tool_use_id, agentId: e.agentId });
      throw error;
    }
  }).catch(($, e, next) => e.tool === 'AskUserQuestion' && connection?.waitCommand
    ? { deny: 'The T3 question connection failed. Retry this request after reconnecting.' }
    : next(e));

  on('classic.PermissionRequest', async ($, e, next) => {
    const existing = await next(e);
    if (existing.decision || existing.block || existing.preventContinuation || !connection?.waitCommand) return existing;
    const decision = await request($, 'permission', e);
    if (decision === null) return { ...existing, decision: { behavior: 'deny', message: 'Approval cancelled' } };
    if (decision.behavior !== 'allow' && decision.behavior !== 'deny') throw new Error('Invalid permission response');
    return { ...existing, decision };
  }).catch(() => ({ decision: { behavior: 'deny', message: 'The T3 approval connection failed. Retry this request after reconnecting.' } }));

  on('classic.Elicitation', async ($, e, next) => {
    const existing = await next(e);
    if (existing.action || existing.block || existing.preventContinuation || !connection?.waitCommand) return existing;
    const response = await request($, 'elicitation', e);
    if (response === null) return { ...existing, action: 'cancel' };
    if (!['accept', 'decline', 'cancel'].includes(response.action)) throw new Error('Invalid elicitation response');
    return { ...existing, ...response };
  }).catch(() => ({ action: 'cancel' }));

  on('turn.complete', async ($, e, next) => {
    record('turn.complete', e);
    steps.delete(e.agentId ?? '');
    if (connection) await snapshot($);
    return next(e);
  });

  on('classic.SubagentStart', async ($, e, next) => {
    record('agent.start', e);
    const result = await next(e);
    if (connection) await snapshot($);
    return result;
  });

  on('classic.SubagentStop', async ($, e, next) => {
    record('agent.stop', e);
    if (connection) await snapshot($);
    return next(e);
  });
}
`;

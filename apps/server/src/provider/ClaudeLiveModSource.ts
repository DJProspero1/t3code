// Claude loads this source itself; bundling it would rewrite the capability calls its validator reads.
export const claudeLiveModSource = String.raw`
let connection;
let sessionId;
let epoch;
let sequence = 0;
let acknowledged = 0;
let events = [];
let queuedBytes = 0;
let sending = false;
let timer;
let lastContact = 0;
const commands = new Map();

function record(type, payload) {
  if (!connection) return;
  try {
    const event = { sequence: sequence + 1, type, payload };
    const size = JSON.stringify(event).length;
    sequence = event.sequence;
    if (queuedBytes + size > 1024 * 1024) {
      events = [{ sequence: ++sequence, type: 'resync', payload: {} }];
      queuedBytes = 64;
      return;
    }
    events.push(event);
    queuedBytes += size;
  } catch {
    // Observing a provider event must never change whether its original work runs.
  }
}

async function snapshot($) {
  try {
    record('snapshot', {
      model: await $.session.model(),
      agents: await $.agent.list(),
      version: await $.session.version(),
    });
  } catch {
    // Missing telemetry is not evidence that the session or its children stopped.
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
        result = await $.prompt.submit({ text: command.text, asUser: true });
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
  record('request.open', { requestId, kind, payload });
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
    record('request.closed', { requestId });
  }
}

async function flush($) {
  if (!connection || sending || (events.length === 0 && Date.now() - lastContact < 500)) return;
  sending = true;
  const batch = events.slice(0, 256);
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
    queuedBytes = events.reduce((sum, event) => sum + JSON.stringify(event).length, 0);
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

export function register(on) {
  on('session.start', async ($, e, next) => {
    sessionId = await $.session.id();
    const path = $.plugin.root + '/connections.json';
    if (!(await $.fs.exists(path))) return next(e);
    const configured = JSON.parse(await $.fs.read(path))[sessionId];
    if (!configured) return next(e);
    const url = new URL(configured.url);
    if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || url.username || url.password) return next(e);
    if (typeof configured.token !== 'string' || configured.token.length < 32) return next(e);
    const waitCommand = Array.isArray(configured.waitCommand) && configured.waitCommand.length > 0 && configured.waitCommand.every(arg => typeof arg === 'string') ? configured.waitCommand : undefined;
    connection = { url: url.href, token: configured.token, waitCommand };
    epoch = crypto.randomUUID();
    sequence = 0;
    acknowledged = 0;
    events = [];
    queuedBytes = 0;
    await snapshot($);
    if (timer) timer.cancel();
    timer = $.clock.every(50, () => flush($));
    return next(e);
  }).catch(($, e, next) => next(e));

  on('turn.start', async ($, e, next) => {
    record('turn.start', e);
    return next(e);
  });

  on('turn.step', async function* ($, e, next) {
    record('step.start', e);
    const stream = next(e);
    try {
      for await (const chunk of stream) {
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
    const result = await next(e);
    record('message', { ...e, message: result.message, uuid: result.uuid });
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
      record('tool.complete', { tool_use_id: e.tool_use_id, agentId: e.agentId });
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
    if (connection) await snapshot($);
    return next(e);
  });

  on('classic.SubagentStart', async ($, e, next) => {
    record('agent.start', e);
    return next(e);
  });

  on('classic.SubagentStop', async ($, e, next) => {
    record('agent.stop', e);
    if (connection) await snapshot($);
    return next(e);
  });
}
`;

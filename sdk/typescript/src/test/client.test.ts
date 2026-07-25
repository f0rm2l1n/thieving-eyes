import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import {
  ThievingEyesClient,
  ThievingEyesError,
  ThievingEyesProtocolError,
  type EventEnvelope,
  type SubmissionAccepted,
  type SubmissionCreate,
  type SubmissionResult,
  type SubmissionStatus,
} from "../index.js";

const digest = `sha256:${"0".repeat(64)}`;

const submission: SubmissionCreate = {
  input: { parts: [{ type: "text", text: "inspect the immutable change" }] },
  workspace: {
    kind: "local",
    root_id: "local",
    path: "innovation/tunascope",
    revision: "a".repeat(40),
    access: "read_only",
  },
  execution: {
    route_ids: ["opencode_default"],
    locality: "local_only",
    side_effects: "read_only",
  },
  scheduling: { priority: 60 },
};

const accepted: SubmissionAccepted = {
  submission_id: "sub_test",
  state: "queued",
  terminal: false,
  revision: 1,
  request_digest: digest,
  resolved_profile: { id: "local_coding", version: "1", digest },
  resolved_policy: { id: "standard", version: "1", digest },
  status_url: "/v1/submissions/sub_test",
  events_url: "/v1/submissions/sub_test/events",
  result_url: "/v1/submissions/sub_test/result",
  idempotent_replay: false,
};

const runningStatus: SubmissionStatus = {
  submission_id: "sub_test",
  request_digest: digest,
  resolved_profile: accepted.resolved_profile,
  resolved_policy: accepted.resolved_policy,
  state: "running",
  terminal: false,
  revision: 2,
  mode: "task",
  created_at: "2026-07-24T00:00:00Z",
  updated_at: "2026-07-24T00:00:01Z",
  attempts: [],
  latest_event_sequence: 1,
};

const completedResult: SubmissionResult = {
  submission_id: "sub_test",
  state: "completed",
  terminal: true,
  output: { kind: "text", text: "OK", truncated: false },
  artifacts: [],
  attempts: [],
  finished_at: "2026-07-24T00:00:02Z",
};

test("submit sends a stable idempotency key and typed request", async (context) => {
  let receivedKey: string | undefined;
  let receivedBody: unknown;
  const fixture = await unixServer(async (request, response) => {
    receivedKey = header(request, "idempotency-key");
    receivedBody = JSON.parse((await readRequest(request)).toString("utf8"));
    json(response, 201, accepted);
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({ socketPath: fixture.socketPath });
  const result = await client.submit(submission, {
    idempotencyKey: "tunascope:task-1:attempt-1",
  });

  assert.equal(receivedKey, "tunascope:task-1:attempt-1");
  assert.deepEqual(receivedBody, submission);
  assert.deepEqual(result, accepted);
});

test("API errors preserve stable code without exposing arbitrary bodies", async (context) => {
  const fixture = await unixServer(async (_request, response) => {
    json(response, 409, {
      request_id: "req_test",
      error: {
        code: "idempotency_conflict",
        message: "the key is already bound",
        retryable: false,
        scope: "request",
      },
    });
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({ socketPath: fixture.socketPath });
  await assert.rejects(
    client.submit(submission, { idempotencyKey: "duplicate" }),
    (error: unknown) => {
      assert.ok(error instanceof ThievingEyesError);
      assert.equal(error.statusCode, 409);
      assert.equal(error.requestId, "req_test");
      assert.equal(error.detail?.code, "idempotency_conflict");
      return true;
    },
  );
});

test("malformed intermediary errors are not reflected into exception messages", async (context) => {
  const fixture = await unixServer(async (_request, response) => {
    json(response, 502, { diagnostic: "sensitive upstream response" });
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({ socketPath: fixture.socketPath });
  await assert.rejects(client.status("sub_test"), (error: unknown) => {
    assert.ok(error instanceof ThievingEyesError);
    assert.equal(error.statusCode, 502);
    assert.equal(error.detail, undefined);
    assert.equal(error.message, "thieving-eyes request failed with HTTP 502");
    assert.doesNotMatch(error.message, /sensitive/);
    return true;
  });
});

test("watch parses fragmented SSE and skips replayed sequences", async (context) => {
  const replay = sse(event(1, "submission.created", {}));
  const second = event(2, "attempt.state_changed", { to: "running" });
  const secondWire = sse(second, "\r\n");
  const fixture = await unixServer(async (_request, response) => {
    response.writeHead(200, { "Content-Type": "text/event-stream" });
    response.write(": keepalive\r\n\r\n");
    response.write(replay.slice(0, 13));
    response.write(replay.slice(13));
    response.end(secondWire);
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({
    socketPath: fixture.socketPath,
    maxResponseBytes: Math.max(
      Buffer.byteLength(replay),
      Buffer.byteLength(secondWire),
    ),
  });
  const received: EventEnvelope[] = [];
  for await (const value of client.watch("sub_test", {
    afterSequence: 1,
    reconnect: false,
  })) {
    received.push(value);
  }

  assert.deepEqual(received, [second]);
});

test("watch rejects mismatched SSE metadata without reconnecting", async (context) => {
  let connections = 0;
  const value = event(1, "submission.created", {});
  const fixture = await unixServer(async (_request, response) => {
    connections += 1;
    response.writeHead(200, { "Content-Type": "text/event-stream" });
    response.end(
      `id: 2\nevent: ${value.type}\ndata: ${JSON.stringify(value)}\n\n`,
    );
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({ socketPath: fixture.socketPath });
  const next = client.watch("sub_test", {
    reconnect: true,
    reconnectDelayMs: 0,
  }).next();

  await assert.rejects(next, (error: unknown) => {
    assert.ok(error instanceof ThievingEyesProtocolError);
    assert.match(error.message, /SSE id/);
    return true;
  });
  assert.equal(connections, 1);
});

test("successful malformed JSON is reported as a protocol error", async (context) => {
  const fixture = await unixServer(async (_request, response) => {
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end("{");
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({ socketPath: fixture.socketPath });
  await assert.rejects(client.status("sub_test"), (error: unknown) => {
    assert.ok(error instanceof ThievingEyesProtocolError);
    assert.equal(
      error.message,
      "thieving-eyes returned an invalid JSON response",
    );
    return true;
  });
});

test("successful responses require their documented media type", async (context) => {
  const fixture = await unixServer(async (_request, response) => {
    response.writeHead(200, { "Content-Type": "text/plain" });
    response.end(JSON.stringify(runningStatus));
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({ socketPath: fixture.socketPath });
  await assert.rejects(client.status("sub_test"), (error: unknown) => {
    assert.ok(error instanceof ThievingEyesProtocolError);
    assert.match(error.message, /application\/json/);
    return true;
  });
});

test("watch bounds the size of one SSE event", async (context) => {
  const value = event(1, "agent.message", { text: "x".repeat(512) });
  const fixture = await unixServer(async (_request, response) => {
    response.writeHead(200, { "Content-Type": "text/event-stream" });
    response.end(sse(value));
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({
    socketPath: fixture.socketPath,
    maxResponseBytes: 128,
  });
  const next = client.watch("sub_test", { reconnect: true }).next();

  await assert.rejects(next, (error: unknown) => {
    assert.ok(error instanceof ThievingEyesProtocolError);
    assert.match(error.message, /SSE (event|line) exceeded/);
    return true;
  });
});

test("run reconnects, deduplicates events, and returns the terminal result", async (context) => {
  let eventConnections = 0;
  const first = event(1, "attempt.state_changed", { to: "running" });
  const terminal = event(2, "submission.state_changed", {
    from: "running",
    to: "completed",
  });
  const fixture = await unixServer(async (request, response) => {
    if (request.method === "POST" && request.url === "/v1/submissions") {
      await readRequest(request);
      json(response, 201, accepted);
      return;
    }
    if (request.method === "GET" && request.url === "/v1/submissions/sub_test") {
      json(response, 200, runningStatus);
      return;
    }
    if (
      request.method === "GET" &&
      request.url?.startsWith("/v1/submissions/sub_test/events")
    ) {
      eventConnections += 1;
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      if (eventConnections === 1) {
        response.end(sse(first));
      } else {
        response.end(`${sse(first)}${sse(terminal)}`);
      }
      return;
    }
    if (
      request.method === "GET" &&
      request.url === "/v1/submissions/sub_test/result"
    ) {
      json(response, 200, completedResult);
      return;
    }
    json(response, 404, {
      request_id: "req_missing",
      error: {
        code: "not_found",
        message: "missing",
        retryable: false,
        scope: "request",
      },
    });
  });
  context.after(fixture.close);

  const seen: number[] = [];
  const client = new ThievingEyesClient({ socketPath: fixture.socketPath });
  const result = await client.run(submission, {
    idempotencyKey: "tunascope:task-2:attempt-1",
    reconnectDelayMs: 0,
    onEvent: (value) => {
      seen.push(value.sequence);
    },
  });

  assert.equal(eventConnections, 2);
  assert.deepEqual(seen, [1, 2]);
  assert.deepEqual(result, completedResult);
});

test("aborting a watch closes an already-open SSE response", async (context) => {
  let markOpened: (() => void) | undefined;
  const opened = new Promise<void>((resolve) => {
    markOpened = resolve;
  });
  const fixture = await unixServer(async (_request, response) => {
    response.writeHead(200, { "Content-Type": "text/event-stream" });
    response.flushHeaders();
    markOpened?.();
  });
  context.after(fixture.close);

  const client = new ThievingEyesClient({ socketPath: fixture.socketPath });
  const controller = new AbortController();
  const iterator = client.watch("sub_test", {
    reconnect: true,
    signal: controller.signal,
  });
  const next = iterator.next();
  await opened;
  controller.abort(new Error("stop watching"));

  await assert.rejects(next, /stop watching/);
});

function event(
  sequence: number,
  type: string,
  data: Record<string, unknown>,
): EventEnvelope {
  return {
    event_id: `evt_${sequence}`,
    submission_id: "sub_test",
    sequence,
    occurred_at: "2026-07-24T00:00:00Z",
    type,
    data,
  };
}

function sse(value: EventEnvelope, newline = "\n"): string {
  return [
    `id: ${value.sequence}`,
    `event: ${value.type}`,
    `data: ${JSON.stringify(value)}`,
    "",
    "",
  ].join(newline);
}

async function unixServer(
  handler: (
    request: IncomingMessage,
    response: ServerResponse,
  ) => Promise<void>,
): Promise<{ socketPath: string; close: () => Promise<void> }> {
  const directory = await mkdtemp(join(tmpdir(), "thieving-eyes-sdk-"));
  const socketPath = join(directory, "daemon.sock");
  const server = createServer((request, response) => {
    void handler(request, response).catch((error: unknown) => {
      response.destroy(error instanceof Error ? error : new Error(String(error)));
    });
  });
  await listen(server, socketPath);
  return {
    socketPath,
    close: async () => {
      await close(server);
      await rm(directory, { recursive: true, force: true });
    },
  };
}

function listen(server: Server, socketPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

async function readRequest(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

function header(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return Array.isArray(value) ? value[0] : value;
}

function json(response: ServerResponse, status: number, value: unknown): void {
  const body = Buffer.from(JSON.stringify(value), "utf8");
  response.writeHead(status, {
    "Content-Length": body.byteLength,
    "Content-Type": "application/json",
  });
  response.end(body);
}

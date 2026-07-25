import { request as httpRequest, type IncomingMessage } from "node:http";
import { join } from "node:path";
import { StringDecoder } from "node:string_decoder";

import type {
  ApiError,
  CancellationResult,
  CapabilityCatalog,
  ErrorDetail,
  EventEnvelope,
  SubmissionAccepted,
  SubmissionCreate,
  SubmissionPatch,
  SubmissionResult,
  SubmissionStatus,
} from "./schema.js";

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
const DEFAULT_RECONNECT_DELAY_MS = 500;
const TERMINAL_STATES = new Set([
  "completed",
  "failed",
  "cancelled",
  "expired",
  "uncertain",
]);

export interface ThievingEyesClientOptions {
  socketPath?: string;
  requestTimeoutMs?: number;
  maxResponseBytes?: number;
  userAgent?: string;
}

export interface RequestOptions {
  signal?: AbortSignal;
}

export interface SubmitOptions extends RequestOptions {
  idempotencyKey: string;
}

export interface WatchOptions extends RequestOptions {
  afterSequence?: number;
  reconnect?: boolean;
  reconnectDelayMs?: number;
  untilTerminal?: boolean;
}

export interface RunOptions extends SubmitOptions {
  afterSequence?: number;
  onEvent?: (event: EventEnvelope) => void | Promise<void>;
  reconnectDelayMs?: number;
}

interface JsonRequestOptions extends RequestOptions {
  body?: unknown;
  headers?: Readonly<Record<string, string>>;
}

interface SseMessage {
  data: string;
  event?: string;
  id?: string;
}

export class ThievingEyesError extends Error {
  readonly statusCode: number;
  readonly requestId?: string;
  readonly detail?: ErrorDetail;

  constructor(
    message: string,
    options: {
      statusCode: number;
      requestId?: string;
      detail?: ErrorDetail;
      cause?: unknown;
    },
  ) {
    super(message, { cause: options.cause });
    this.name = "ThievingEyesError";
    this.statusCode = options.statusCode;
    this.requestId = options.requestId;
    this.detail = options.detail;
  }
}

export class ThievingEyesProtocolError extends Error {
  constructor(message: string, options: { cause?: unknown } = {}) {
    super(message, { cause: options.cause });
    this.name = "ThievingEyesProtocolError";
  }
}

export function defaultSocketPath(): string {
  const runtimeDirectory = process.env.XDG_RUNTIME_DIR;
  if (runtimeDirectory) {
    return join(runtimeDirectory, "thieving-eyes", "daemon.sock");
  }
  if (typeof process.getuid !== "function") {
    throw new Error("socketPath is required when the process has no Unix uid");
  }
  return `/run/user/${process.getuid()}/thieving-eyes/daemon.sock`;
}

export class ThievingEyesClient {
  readonly socketPath: string;
  readonly requestTimeoutMs: number;
  readonly maxResponseBytes: number;
  readonly userAgent: string;

  constructor(options: ThievingEyesClientOptions = {}) {
    this.socketPath = options.socketPath ?? defaultSocketPath();
    this.requestTimeoutMs = positiveInteger(
      options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      "requestTimeoutMs",
    );
    this.maxResponseBytes = positiveInteger(
      options.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES,
      "maxResponseBytes",
    );
    this.userAgent = options.userAgent ?? "@thieving-eyes/sdk/0.1.0";
  }

  async submit(
    submission: SubmissionCreate,
    options: SubmitOptions,
  ): Promise<SubmissionAccepted> {
    validateIdempotencyKey(options.idempotencyKey);
    return this.requestJson("POST", "/v1/submissions", {
      body: submission,
      headers: { "Idempotency-Key": options.idempotencyKey },
      signal: options.signal,
    });
  }

  async status(
    submissionId: string,
    options: RequestOptions = {},
  ): Promise<SubmissionStatus> {
    return this.requestJson(
      "GET",
      `/v1/submissions/${encodeIdentifier(submissionId)}`,
      options,
    );
  }

  async result(
    submissionId: string,
    options: RequestOptions = {},
  ): Promise<SubmissionResult> {
    return this.requestJson(
      "GET",
      `/v1/submissions/${encodeIdentifier(submissionId)}/result`,
      options,
    );
  }

  async cancel(
    submissionId: string,
    options: RequestOptions = {},
  ): Promise<CancellationResult> {
    return this.requestJson(
      "POST",
      `/v1/submissions/${encodeIdentifier(submissionId)}/cancel`,
      options,
    );
  }

  async patchScheduling(
    submissionId: string,
    revision: number,
    patch: SubmissionPatch,
    options: RequestOptions = {},
  ): Promise<SubmissionStatus> {
    nonNegativeInteger(revision, "revision");
    return this.requestJson(
      "PATCH",
      `/v1/submissions/${encodeIdentifier(submissionId)}`,
      {
        body: patch,
        headers: { "If-Match": `"${revision}"` },
        signal: options.signal,
      },
    );
  }

  async capabilities(
    options: RequestOptions = {},
  ): Promise<CapabilityCatalog> {
    return this.requestJson("GET", "/v1/capabilities", options);
  }

  async *watch(
    submissionId: string,
    options: WatchOptions = {},
  ): AsyncGenerator<EventEnvelope> {
    const encodedId = encodeIdentifier(submissionId);
    let cursor = nonNegativeInteger(options.afterSequence ?? 0, "afterSequence");
    const reconnect = options.reconnect ?? true;
    const reconnectDelayMs = nonNegativeInteger(
      options.reconnectDelayMs ?? DEFAULT_RECONNECT_DELAY_MS,
      "reconnectDelayMs",
    );

    for (;;) {
      throwIfAborted(options.signal);
      const path = `/v1/submissions/${encodedId}/events?after_sequence=${cursor}`;
      let response: IncomingMessage;
      try {
        response = await this.openEventStream(path, options.signal);
        for await (const message of parseSse(
          response,
          this.maxResponseBytes,
        )) {
          const event = parseEvent(message, submissionId);
          if (event.sequence <= cursor) {
            continue;
          }
          cursor = event.sequence;
          yield event;
          if (options.untilTerminal && isTerminalEvent(event)) {
            return;
          }
        }
      } catch (error) {
        if (
          options.signal?.aborted ||
          error instanceof ThievingEyesError ||
          error instanceof ThievingEyesProtocolError ||
          !reconnect
        ) {
          throw error;
        }
      }

      if (!reconnect) {
        return;
      }
      await abortableDelay(reconnectDelayMs, options.signal);
    }
  }

  async run(
    submission: SubmissionCreate,
    options: RunOptions,
  ): Promise<SubmissionResult> {
    const accepted = await this.submit(submission, options);
    const current = await this.status(accepted.submission_id, options);
    if (!current.terminal) {
      for await (const event of this.watch(accepted.submission_id, {
        afterSequence: options.afterSequence,
        reconnect: true,
        reconnectDelayMs: options.reconnectDelayMs,
        signal: options.signal,
        untilTerminal: true,
      })) {
        await options.onEvent?.(event);
      }
    }
    return this.result(accepted.submission_id, options);
  }

  private async requestJson<T>(
    method: string,
    path: string,
    options: JsonRequestOptions = {},
  ): Promise<T> {
    const encodedBody =
      options.body === undefined
        ? undefined
        : Buffer.from(JSON.stringify(options.body), "utf8");
    const response = await this.openRequest(
      method,
      path,
      {
        Accept: "application/json",
        ...(encodedBody
          ? {
              "Content-Length": encodedBody.byteLength.toString(),
              "Content-Type": "application/json",
            }
          : {}),
        ...options.headers,
      },
      options.signal,
      encodedBody,
    );
    const body = await readBody(response, this.maxResponseBytes);
    if (!isSuccess(response.statusCode)) {
      throw decodeApiError(response.statusCode ?? 0, body);
    }
    if (!hasMediaType(response, "application/json")) {
      throw new ThievingEyesProtocolError(
        "thieving-eyes returned a successful response without application/json",
      );
    }
    if (body.length === 0) {
      throw new ThievingEyesProtocolError(
        "thieving-eyes returned an empty JSON response",
      );
    }
    try {
      return JSON.parse(body.toString("utf8")) as T;
    } catch (error) {
      throw new ThievingEyesProtocolError(
        "thieving-eyes returned an invalid JSON response",
        { cause: error },
      );
    }
  }

  private async openEventStream(
    path: string,
    signal?: AbortSignal,
  ): Promise<IncomingMessage> {
    const response = await this.openRequest(
      "GET",
      path,
      { Accept: "text/event-stream" },
      signal,
    );
    if (!isSuccess(response.statusCode)) {
      const body = await readBody(response, this.maxResponseBytes);
      throw decodeApiError(response.statusCode ?? 0, body);
    }
    if (!hasMediaType(response, "text/event-stream")) {
      response.destroy();
      throw new ThievingEyesProtocolError(
        "thieving-eyes returned a successful event response without text/event-stream",
      );
    }
    return response;
  }

  private async openRequest(
    method: string,
    path: string,
    headers: Readonly<Record<string, string>>,
    signal?: AbortSignal,
    body?: Buffer,
  ): Promise<IncomingMessage> {
    throwIfAborted(signal);
    return new Promise<IncomingMessage>((resolve, reject) => {
      const request = httpRequest(
        {
          socketPath: this.socketPath,
          path,
          method,
          headers: {
            "User-Agent": this.userAgent,
            ...headers,
          },
        },
        (response) => {
          request.setTimeout(0);
          response.setTimeout(this.requestTimeoutMs, () => {
            response.destroy(
              new Error(
                `thieving-eyes response was idle for ${this.requestTimeoutMs}ms`,
              ),
            );
          });
          response.once("close", cleanup);
          response.once("end", cleanup);
          resolve(response);
        },
      );
      const abort = () => request.destroy(abortReason(signal));
      const cleanup = () => signal?.removeEventListener("abort", abort);

      signal?.addEventListener("abort", abort, { once: true });
      request.once("error", (error) => {
        cleanup();
        reject(error);
      });
      request.setTimeout(this.requestTimeoutMs, () => {
        request.destroy(
          new Error(
            `thieving-eyes request headers timed out after ${this.requestTimeoutMs}ms`,
          ),
        );
      });
      if (body) {
        request.write(body);
      }
      request.end();
    });
  }
}

function validateIdempotencyKey(value: string): void {
  if (value.length === 0 || value.length > 200) {
    throw new TypeError("idempotencyKey must contain between 1 and 200 characters");
  }
}

function encodeIdentifier(value: string): string {
  if (value.length === 0) {
    throw new TypeError("identifier must not be empty");
  }
  return encodeURIComponent(value);
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
  return value;
}

function isSuccess(statusCode: number | undefined): boolean {
  return statusCode !== undefined && statusCode >= 200 && statusCode < 300;
}

function hasMediaType(response: IncomingMessage, expected: string): boolean {
  const value = response.headers["content-type"];
  if (typeof value !== "string") {
    return false;
  }
  return value.split(";", 1)[0]?.trim().toLowerCase() === expected;
}

async function readBody(
  response: IncomingMessage,
  maxBytes: number,
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let length = 0;
  for await (const value of response) {
    const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
    length += chunk.length;
    if (length > maxBytes) {
      response.destroy();
      throw new Error(`thieving-eyes response exceeded ${maxBytes} bytes`);
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks, length);
}

function decodeApiError(statusCode: number, body: Buffer): ThievingEyesError {
  let decoded: ApiError | undefined;
  try {
    const value: unknown = JSON.parse(body.toString("utf8"));
    if (isApiError(value)) {
      decoded = value;
    }
  } catch {
    // The stable error code is unavailable when an intermediary returns a
    // non-JSON response. Do not include the response body in the exception.
  }
  return new ThievingEyesError(
    decoded?.error.message ?? `thieving-eyes request failed with HTTP ${statusCode}`,
    {
      statusCode,
      requestId: decoded?.request_id,
      detail: decoded?.error,
    },
  );
}

function isApiError(value: unknown): value is ApiError {
  if (!isRecord(value) || typeof value["request_id"] !== "string") {
    return false;
  }
  const detail = value["error"];
  return (
    isRecord(detail) &&
    typeof detail["code"] === "string" &&
    typeof detail["message"] === "string" &&
    typeof detail["retryable"] === "boolean" &&
    typeof detail["scope"] === "string"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

async function* parseSse(
  response: IncomingMessage,
  maxEventBytes: number,
): AsyncGenerator<SseMessage> {
  const decoder = new StringDecoder("utf8");
  let buffer = "";
  let bufferBytes = 0;
  let dataLines: string[] = [];
  let eventType: string | undefined;
  let eventId: string | undefined;
  let eventBytes = 0;

  const consumeLine = (line: string): SseMessage | undefined => {
    if (line.length === 0) {
      if (dataLines.length === 0) {
        eventType = undefined;
        eventId = undefined;
        eventBytes = 0;
        return undefined;
      }
      const message = {
        data: dataLines.join("\n"),
        event: eventType,
        id: eventId,
      };
      dataLines = [];
      eventType = undefined;
      eventId = undefined;
      eventBytes = 0;
      return message;
    }
    if (line.startsWith(":")) {
      return undefined;
    }
    eventBytes += Buffer.byteLength(line, "utf8") + 1;
    if (eventBytes > maxEventBytes) {
      throw new ThievingEyesProtocolError(
        `thieving-eyes SSE event exceeded ${maxEventBytes} bytes`,
      );
    }
    const separator = line.indexOf(":");
    const field = separator === -1 ? line : line.slice(0, separator);
    let value = separator === -1 ? "" : line.slice(separator + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }
    if (field === "data") {
      dataLines.push(value);
    } else if (field === "event") {
      eventType = value;
    } else if (field === "id" && !value.includes("\0")) {
      eventId = value;
    }
    return undefined;
  };

  for await (const chunk of response) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bufferBytes += bytes.byteLength;
    buffer += decoder.write(bytes);
    for (;;) {
      const newline = buffer.indexOf("\n");
      if (newline === -1) {
        break;
      }
      const rawLine = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      bufferBytes = Math.max(
        0,
        bufferBytes - Buffer.byteLength(rawLine, "utf8") - 1,
      );
      let line = rawLine;
      if (line.endsWith("\r")) {
        line = line.slice(0, -1);
      }
      const message = consumeLine(line);
      if (message) {
        yield message;
      }
    }
    if (bufferBytes > maxEventBytes) {
      throw new ThievingEyesProtocolError(
        `thieving-eyes SSE line exceeded ${maxEventBytes} bytes`,
      );
    }
  }
  buffer += decoder.end();
  if (buffer.endsWith("\r")) {
    buffer = buffer.slice(0, -1);
  }
  const final = consumeLine(buffer) ?? consumeLine("");
  if (final) {
    yield final;
  }
}

function parseEvent(
  message: SseMessage,
  submissionId: string,
): EventEnvelope {
  let event: Partial<EventEnvelope>;
  try {
    event = JSON.parse(message.data) as Partial<EventEnvelope>;
  } catch (error) {
    throw new ThievingEyesProtocolError(
      "invalid JSON in thieving-eyes event stream",
      { cause: error },
    );
  }
  if (
    typeof event.event_id !== "string" ||
    event.submission_id !== submissionId ||
    !Number.isSafeInteger(event.sequence) ||
    (event.sequence ?? 0) <= 0 ||
    typeof event.type !== "string" ||
    typeof event.occurred_at !== "string" ||
    event.data === null ||
    typeof event.data !== "object" ||
    Array.isArray(event.data)
  ) {
    throw new ThievingEyesProtocolError(
      "invalid thieving-eyes event envelope",
    );
  }
  if (message.id !== String(event.sequence)) {
    throw new ThievingEyesProtocolError(
      "thieving-eyes SSE id does not match the event sequence",
    );
  }
  if (message.event !== event.type) {
    throw new ThievingEyesProtocolError(
      "thieving-eyes SSE event does not match the event envelope type",
    );
  }
  return event as EventEnvelope;
}

function isTerminalEvent(event: EventEnvelope): boolean {
  return (
    event.type === "submission.state_changed" &&
    TERMINAL_STATES.has(String(event.data["to"]))
  );
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw abortReason(signal);
  }
}

function abortReason(signal?: AbortSignal): Error {
  return signal?.reason instanceof Error
    ? signal.reason
    : new Error("operation aborted");
}

async function abortableDelay(
  milliseconds: number,
  signal?: AbortSignal,
): Promise<void> {
  throwIfAborted(signal);
  if (milliseconds === 0) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, milliseconds);
    const abort = () => {
      clearTimeout(timer);
      cleanup();
      reject(abortReason(signal));
    };
    const cleanup = () => signal?.removeEventListener("abort", abort);
    signal?.addEventListener("abort", abort, { once: true });
  });
}

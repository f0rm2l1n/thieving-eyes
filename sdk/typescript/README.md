# @thieving-eyes/sdk

Node.js TypeScript client for the thieving-eyes Submission API.

The package currently targets trusted local applications and connects over the
daemon Unix domain socket. It has no runtime dependencies. Public protocol
types are generated from `../../api/openapi.yaml`; the UDS transport, durable
idempotency, SSE resume, cancellation, and `run()` helper are handwritten.

## Local development

```text
pnpm install
pnpm run generate
pnpm run check
pnpm run test
pnpm run build
```

Generated `src/schema.ts` is committed. `pnpm run check` fails when it no longer
matches the OpenAPI source of truth.

## Usage

```ts
import {
  ThievingEyesClient,
  type SubmissionCreate,
} from "@thieving-eyes/sdk";

const client = new ThievingEyesClient({
  socketPath: process.env.THIEVING_EYES_SOCKET,
});

const submission: SubmissionCreate = {
  input: {
    parts: [{ type: "text", text: "Inspect the configured workspace." }],
  },
};

const accepted = await client.submit(submission, {
  idempotencyKey: "my-app:job-42:attempt-1",
});

for await (const event of client.watch(accepted.submission_id, {
  afterSequence: 0,
  reconnect: true,
})) {
  await persist(event);
  if (
    event.type === "submission.state_changed" &&
    ["completed", "failed", "cancelled", "expired", "uncertain"].includes(
      String(event.data["to"]),
    )
  ) {
    break;
  }
}

const result = await client.result(accepted.submission_id);
```

See [`docs/client-integration.md`](../../docs/client-integration.md) for the
Tunascope integration contract, recovery rules, workspace constraints, and
current capability limits.

Non-2xx API responses throw `ThievingEyesError`. Invalid successful JSON or
SSE envelopes throw `ThievingEyesProtocolError`; the watcher does not reconnect
on protocol errors. `maxResponseBytes` bounds a JSON body or one SSE event.
`requestTimeoutMs` is a transport inactivity timeout, not a Submission runtime
limit; timing out the client does not cancel server-side execution.

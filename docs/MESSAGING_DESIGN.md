# Agent messaging design choices

This project is a durable addressed inbox. Agent IDs name logical recipients, sessions name registrations, and credentials identify stable subjects. Message delivery, webhook receipt and completed work are separate states.

| Paradigm | How it works | Fit here |
|---|---|---|
| Inbox / actor | Send to a stable recipient that controls processing. | Persistent ownership and direct messages fit this model. Database locks serialize mailbox changes, not the external agent's computation. |
| Publish/subscribe | Fan out an event to independently interested observers. | Arrival webhooks and resource hints are observers. Broadcast deliberately creates one delivery per recipient. |
| Work queue | Claim a job, retain a lease, and finish or retry it. | Callback workers use leases. Agent polling has no job lease and does not provide competing-worker claiming or exactly-once execution. |
| Request/reply | Correlate a response with its originating request. | `in_reply_to` supports result/error messages. Stable idempotency keys support safe retries. |
| Task protocol / A2A | Discover agent skills and exchange task status and artifacts. | A useful future adapter for remote delegation, once explicit task state exists. |

[A2A](https://a2a-protocol.org/latest/topics/key-concepts/) provides Agent Cards, tasks, artifacts, polling, streaming and push updates. Its useful lesson is to represent work explicitly when callers need progress, interruption or a final result. A full adapter should translate task IDs, authorization and artifacts into a shared task layer. This change does not claim A2A wire compatibility.

[NATS JetStream consumers](https://docs.nats.io/learn/jetstream/pull-consumers) illustrate persistent consumer progress and explicit acknowledgement. Durable event positions and callback attempts apply that idea here. [RabbitMQ acknowledgements](https://www.rabbitmq.com/docs/confirms) distinguish publisher acceptance from consumer completion: this server commits acceptance and records receipt separately.

Applications can use this convention today:

1. Send a `task.request` topic with an application task identifier and stable idempotency key.
2. Receive and acknowledge when the recipient has safely retained the request.
3. Send progress or a `task.result`/`task.error` reply with `in_reply_to` and a stable key per update.
4. Deduplicate notifications and make task effects idempotent.

These topics are conventions, not enforced task transitions. Acknowledging before safely retaining work can lose an application's task even if its mailbox is correct.

If job execution becomes a core feature, the next addition should be a coherent task/claim layer: task state, worker leases, retry/dead-letter controls, correlation and artifacts. An A2A adapter can sit above that. Operation metrics and administrative delivery recovery are also useful future additions. The current implementation provides the reliability foundation while keeping inbox receipt semantics clear.

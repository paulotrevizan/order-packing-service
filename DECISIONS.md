# Decisions Log

The decisions behind the original solution, which this repository replicates on a stand-in
domain, with the options considered and the trade-offs accepted. Where this repository
simplifies the original solution, the entry says so under **In this repository**.

| ID    | Decision                                                                           |
|-------|------------------------------------------------------------------------------------|
| D-001 | Store every message in an inbox before processing it                               |
| D-002 | A scheduled reconciler decides completion                                          |
| D-003 | Reconciler concurrency with `FOR UPDATE SKIP LOCKED`                               |
| D-004 | Complete = row numbers exactly `1..N`, duplicates rejected by unique constraints |
| D-005 | The carrier is called at most once, an unknown outcome goes to a person            |
| D-006 | Bounded wait for missing rows (exponential backoff and max attempts)            |
| D-007 | A failed order is marked `FAILED` and alerted                                      |
| D-008 | Invalid records are skipped at once, never retried                                 |
| D-009 | Manual acknowledgement after the inbox commit                                      |
| D-010 | One topic, JSON messages with a `type` attribute                                   |
| D-011 | Packaged by feature, the carrier is the only interface                             |
| D-012 | No business HTTP API, Actuator only                                                |
| D-013 | Scenario tests only against real infrastructure                                    |
| D-014 | UUIDs assigned by the application                                                  |
| D-015 | Inbox retention and replay are out of scope                                        |
| D-016 | Receive-path details                                                               |

---

## D-001 - Store every message in an inbox before processing it

- **Context:** the messages of one order arrive on different partitions, in any order and a 
  rebalance can redeliver a message that was already processed. Processing on arrival with a 5s 
  retry failed on both counts.
- **Options:**
  - A) Inbox table with dedup constraints, then a reconciler decides.
  - B) Kafka Streams: re-key by `orderId` and aggregate the rows in a state store.
- **Decision:** A.
- **Trade-offs:** 
  - pros: one mechanism fixes both problems, the ack is fast and decoupled from processing, dedup is a 
    database constraint, full history for debugging. 
  - cons: one more table, one more write and more transactions per message (~41M rows/month), processing 
  is no longer immediate.
- **Why not B:** it fixes ordering, but not the duplicate call to the external carrier, as exactly-once in 
Kafka Streams covers Kafka to Kafka, not an HTTP side effect, so with several instances and rebalances 
the carrier can still be called twice. It also brings RocksDB store, changelog topics and a new runtime 
to operate.

## D-002 - A scheduled reconciler decides completion

- **Context:** someone has to notice that the last message of an order arrived.
- **Options:** 
  - A) A reconciler polls due work. 
  - B) Each insert checks "am I the last message?".
- **Decision:** A. B can be added later as a latency optimization.
- **Trade-offs:** 
  - pros: one place decides, and it also heals crashes and missed triggers. 
  - cons: latency of up to one polling interval (2s) and constant light database load.
- **Why not B:** two pods inserting the last two messages at the same time can each miss the others 
uncommitted row, so nobody completes the order, a crash right after the insert also leaves no one to finish.

## D-003 - Reconciler concurrency with `FOR UPDATE SKIP LOCKED`

- **Context:** the reconciler runs on every pod (up to 3), two pods must never decide the same order.
- **Options:** 
  - A) `SELECT … FOR UPDATE SKIP LOCKED` on due work, in PostgreSQL. 
  - B) A distributed lock per order in Redis.
- **Decision:** A. The claim only selects `RECEIVED` / `WAITING` records that are due, so an order 
marked as `LABEL_REQUESTED` is never claimed for completion again. It is claimed only once it is past 
the label timeout, to be failed as abandoned.
- **Trade-offs:** 
  - pros: all pods share the work, the lock and the state live in the same database and 
  are committed together, a dead pod locks are released with its own connection, no new infrastructure. 
  - cons: PostgreSQL native SQL, the claim transaction must stay short, without HTTP inside it.
- **Why not B:** one more system to run and keep available, and the lock lives apart from the data, with 
the risk of having stale data during concurrent processing between pods.

## D-004 - Complete = row numbers exactly `1..N`, duplicates rejected by unique constraints

- **Context:** a row can be re-sent with a new `eventId` (for example, a producer retry), so counting 
messages can reach 4 with only 3 real rows. The same can happen to "order completed".
- **Decision:** an order is complete when the distinct `row_number` values are exactly 
`1..expectedRows`. 
Unique constraints `inbox_order_row_unique (order_id, row_number) WHERE type = 'ROW_COMPLETED'` and 
`inbox_order_completed_unique (order_id) WHERE type = 'ORDER_COMPLETED'` reject re-sends at insert time.
- **Trade-offs:** 
  - pros: duplicates can never fake completeness, and a gap in the row numbers (`{1,2,4}`) never 
  completes an order, the rule lives in the database. 
  - cons: a corrected re-send of a row is ignored and would need a versioned update.

## D-005 - The carrier is called at most once, an unknown outcome goes to a person

- **Context:** the carrier is not idempotent and has no lookup endpoint, so we can only send data. 
A second call to external carrier creates a second label and a duplicate shipment.
- **Options:**
  - A) Commit `LABEL_REQUESTED`, call the carrier outside the transaction, then record the outcome 
  (at most once).
  - B) Call inside the database transaction.
  - C) At-least-once retry.
- **Decision:** A, with the outcome classified:
  - no connection (refused, connect timeout) or `429` → the carrier certainly created no label, so
  order completed is marked back to `WAITING`;
  - other `4xx` → `FAILED` (`CARRIER_REJECTED`);
  - read timeout or `5xx` → the label may exist, so order completed is marked as `FAILED` 
  (`LABEL_OUTCOME_UNKNOWN`) and requires a person to check it;
  - a pod that dies mid-call leaves the order marked as `LABEL_REQUESTED`, after the label timeout the 
  reconciler fails it the same way.

  The carrier is never called again automatically for the same order.
- **Trade-offs:** 
  - pros: the external carrier never receives the same order twice, no database locks held during HTTP call.
  - cons: unknown outcomes would require a person, making the operation delayed.
- **Why not B:** a rollback after a successful call loses our record and the retry duplicates the label,
it also holds locks across HTTP calls. 
- **Why not C:** duplicates are exactly the bug.

## D-006 - Bounded wait for missing rows (exponential backoff and max attempts)

- **Context:** a row may arrive late or never (producer bug, upstream loss).
- **Decision:** `next_attempt_at = now + min(2s * 2^(attempts−1), 60s) +-20% jitter`, 
after 10 attempts (~5 min) the order fails, and it is marked as `MISSING_ROWS`. A carrier call that 
was never taken (no connection, `429`) re-schedules the order with the same backoff but never fails it,
as an external carrier outage is not the order completed fault. The outage stays visible through 
the `refused` external carrier call counter and a `WARN` by attempt.
- **Trade-offs:**
  - pros: no infinite wait for a row, quick completion in the common case (as the rows are seconds 
  apart), failures are visible. 
  - cons: the numbers are tuning guesses, a row that arrives after `FAILED` needs a replay.

## D-007 - A failed order is marked `FAILED` and alerted

- **Context:** "failed" must be visible, so every order ends marked as `PROCESSED` or `FAILED`, and a 
failure reaches a person.
- **Decision:** the order is marked as `FAILED` with a reason 
(`MISSING_ROWS`, `CARRIER_REJECTED` or `LABEL_OUTCOME_UNKNOWN`), logged at `ERROR` with the order id 
and the rows received (alert hook) and counted in metric
`order_packing_orders_total{outcome=failed, reason}`.
- **Trade-offs:**
  - pros: the database holds the order, the reason and history in one place, the alert comes from a log 
  line and a counter. 
  - cons: no message stream of failures for other services to consume.
- **In this repository:** the original service also publishes the failed order to a dead-letter topic, 
inside the failing transaction, to be reprocessed by the existing replay path. That path is out of scope 
here, and without it a dead letter queue topic would only repeat what the database already holds.

## D-008 - Invalid records are skipped at once, never retried

- **Context:** a record that cannot be deserialized or fails validation will never succeed, 
retrying the record blocks its partition.
- **Decision:** `ErrorHandlingDeserializer` + `DefaultErrorHandler`, no retries for deserialization 
and validation errors, and the record is logged at `ERROR` with the message topic, partition and offset, 
counted in `order_packing_invalid_records_total` metric, and the offset is committed. Other exceptions 
(for example the database is down) are retried with backoff and never acknowledged, so the record is not 
lost.
- **Trade-offs:** 
  - pros: a bad record never blocks a partition, the position is enough to read it again from Kafka while 
  the topic retains the event. 
  - cons: one bad record by order means the order will never be completed.
- **In this repository:** the original service sends invalid records to a dead letter queue topic,
here they are logged and skipped to keep things thin.

## D-009 - Manual acknowledgement after the inbox commit

- **Context:** acknowledging before persisting can lose a message, persisting before acknowledging can 
store it twice, which the inbox already absorbs.
- **Decision:** `AckMode.MANUAL_IMMEDIATE`, the listener acknowledges after `InboxWriter` returns, 
including for duplicates.
- **Trade-offs:** 
  - pros: no message is lost, duplicates are harmless by design. 
  - cons: a crash between commit and ack causes a re-delivery, but this is expected and cheap.

## D-010 - One topic, JSON messages with a `type` attribute

- **Context:** row and order completed messages share the same producer and topic.
- **Decision:** one record type `PickingEvent` with a `type` attribute (`ROW_COMPLETED` | `ORDER_COMPLETED`) 
and type specific attributes are validated by type, JSON using Spring Kafka JSON deserializer.
- **Trade-offs:** 
  - pros: mirrors the real producer scenario with one listener. 
  - cons: optional fields by type, so validation needs to be explicit.
- **In this repository:** the original service uses Avro with a schema registry, which enforces the 
contract and future evolutions at the producer side. Here the messages are plain JSON validated by the 
consumer, to keep a schema registry out of the stack, tests and the demo. The type attribute and the 
type validation stay the same.

## D-011 - Packaged by feature, the carrier is the only interface

- **Decision:** features `inbox/`, `packing/`, `integration/carrier`, `common/` (plus `demo/`, the local 
demo publisher), each with `messaging/` → `application/` → `domain/` + `persistence/` packages. The external 
carrier `CarrierClient` is the only interface, using a declarative HTTP service client (`@PostExchange`) 
that Spring implements, configured under `spring.http.serviceclient.carrier`. Tests replace the external 
carrier at the HTTP boundary using WireMock.
- **Trade-offs:** 
  - pros: easy to navigate, the receive path and the decide path are visibly separate. 
  - cons: a few small packages for a small service.

## D-012 - No business HTTP API, Actuator only

- **Context:** the service is a consumer, so nothing calls it except probes and metrics scraping.
- **Decision:** Spring Web MVC only to serve `/actuator/health` and `/actuator/metrics`.
- **Trade-offs:** 
  - pros: less code and configuration. 
  - cons: the actuator endpoints are unauthenticated here.
- **In this repository:** the original service authenticates machine-to-machine (M2M). Spring Security is 
left out here for simplicity.

## D-013 - Scenario tests only against real infrastructure

- **Decision:** every test publishes real records to Testcontainers Kafka, with Testcontainers PostgreSQL 18 
and WireMock as the external carrier, and asserts on what can be observed from outside, like database state, 
the external carrier received requests and metrics. Each test class is named after the problem it proves, 
like out-of-order messages, redelivery, invalid message, missing row, carrier failures, shuffled events, 
and concurrent reconcilers. There are no unit tests. 
- **Trade-offs:** 
  - pros: a small suite where every test proves an invariant and doubles as the demo, refactoring internals 
  never breaks a test, as we are always testing the behavior. 
  - cons: tests need Docker and take about a minute, having no unit tests is an accepted gap and was 
  accepted only for this problem solution.

## D-014 - UUIDs assigned by the application

- **Decision:** entities get a random UUID in the constructor, `BaseEntity` implements `Persistable` 
so a `save` of a new entity issues an `INSERT` without a `SELECT`.
- **Trade-offs:** 
  - pros: ids known before insert, without extra round trip. 
  - cons: random UUIDs fragment the primary key index slightly at high volume, acceptable at ~16 msg/s.

## D-015 - Inbox retention and replay are out of scope

- **Context:** ~41M inbox rows per month, the original system already replays failed orders.
- **Decision:** neither is built here, both are considered extension points. A replay must reset the 
existing inbox row, as the record should be marked from `FAILED → WAITING`, attempts as 0, instead of 
re-publishing the message. A re-published message will hit `inbox_event_id_unique` index and is ignored as 
a duplicate.
- **Trade-offs:** 
  - pros: the service stays focused on the two problems that it solves. 
  - cons: the table grows without a purge job until one is added, a failed order requires a person to 
  reset it.

## D-016 - Receive-path details

- **Context:** implementing the listener left four choices open.
- **Decisions:**
  - **Topic ownership:** the service never creates topics, in production they belong to the platform. 
  Only the tests (a `NewTopic` bean) and `docker-compose.yml` (a one-off `kafka-topics` container) create 
  `order-picking-events` (10 partitions).
  - **Listener retry:** errors other than deserialization and validation (for example the database is down) 
  are retried forever with exponential backoff (1s * 2, capped at 30s). Spring default (10 attempts, then 
  give up) would drop valid records during an outage. `commitRecovered` is on, so a skipped invalid record 
  is committed at once with `MANUAL_IMMEDIATE`.
  - **Stored payload:** `inbox_message.payload` is the deserialized `PickingEvent` serialized again 
  (null fields omitted), not the original bytes, so JSON deserializer stays.
  - **Validation limits:** the record is validated against the schema: `orderId` and `sku` not blank and 
  at most 64 characters, item `quantity >= 1`. `rowNumber` and `expectedRows` are limited to 1–4 
  (the business fixed limit). So, a row 5 is a producer bug, rejected and counted at once instead of 
  surfacing minutes later as `MISSING_ROWS`. A record that passes validation can always be inserted.
- **Trade-offs:** 
  - pros: a valid record is never skipped and never blocks the partition with an insert error,
  topics stay under platform control, one deserializer. 
  - cons: a database outage blocks the affected partitions until it recovers, fields outside the contract 
  and the original formatting are not kept in the payload, the 30s cap is a tuning guess.

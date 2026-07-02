# Chaos Engineering Drills

These are the failure-injection drills run against the live stack. Each one maps
to a reliability requirement and to a distributed-systems interview question.
Scripts: [`chaos_kill_review.sh`](../scripts/chaos_kill_review.sh),
[`chaos_degrade_user.sh`](../scripts/chaos_degrade_user.sh).

---

## Drill 1 — Kill a Review Service instance under load

**Hypothesis:** killing one consumer in the group triggers an automatic Kafka
rebalance; the dead instance's partition is reassigned to a survivor and its
in-flight backlog is processed with **no message loss and no duplicate
moderation**.

**Procedure**
1. Stack running with **3 review-service replicas** (3-partition topic → one
   partition each).
2. Start ~40-worker background load.
3. At t≈8s, `docker kill` one review instance (it owned partition 0).
4. Watch the consumer group and the backlog.

**Observations**
- Immediately after the kill, partition 0 still showed the dead member and its
  lag climbed — Kafka waits out the session timeout before evicting a member.
- Within ~10–30s the group **rebalanced to 2 live members**; partition 0 was
  reassigned to a surviving instance:

  ```
  PARTITION  OWNER (after rebalance)     LAG
  0          app@8a8e398d76c0  (live)    0
  2          app@8a8e398d76c0  (live)    0
  1          app@7c453ce1ce7f  (live)    0
  ```
- Total lag drained back to **0**. Final DB check across the whole run:

  ```
  posts = 10,898   moderation_results = 10,894   distinct_results = 10,894
  duplicate post_ids in moderation_results: none
  ```

  (The 4-post gap is in-flight work at the instant of the query.) **No loss, no
  duplicates.**

**Why it holds:** offsets are committed only *after* the moderation result is
durably written (manual commit), so a message the killed instance had fetched
but not committed is simply redelivered to whoever inherits the partition. The
unique index on `moderation_results.post_id` makes that redelivery a no-op — the
consumer sees "already moderated" and skips. This is at-least-once delivery made
effectively-once by an idempotent consumer.

**Interview hook:** *"How do you recover after a service crashes?"* → consumer
group rebalance + manual offset commit + idempotent write.

---

## Drill 2 — Take the User Service down (dependency outage)

**Hypothesis:** with the User Service unreachable, the Review Service **degrades
gracefully** — the circuit breaker opens, reputation lookups fall back to a
default, and posts keep being moderated on content rules alone — rather than the
pipeline stalling.

**Procedure**
1. `docker compose stop user-service`.
2. Post several times (including from the low-reputation "troll" user, who would
   normally be routed to manual review by the reputation gate).
3. Inspect review logs and the resulting decisions.

**Observations**
- gRPC calls failed with `DeadlineExceeded`; each was logged as
  `reputation lookup failed, degrading to default`.
- Decisions were written with `degraded = true` and the reputation gate was
  **skipped** (fail-open), so the posts were `approved` on content rules instead
  of stuck:

  ```
  post_id  status    degraded
  4        approved  1
  5        approved  1
  6        approved  1
  ...
  ```
- The pipeline never stalled; throughput continued. On restart of the User
  Service the breaker returns to closed on the next successful probe and
  reputation routing resumes.

**Design note:** a dependency outage triggers *degradation*, not dead-lettering.
The DLQ is reserved for genuinely un-processable (poison) messages, so an outage
of a non-critical dependency can never dead-letter the whole stream. The trade
-off — during the outage low-reputation users are not routed to manual review —
is logged (`degraded=true`) so it is auditable and reversible.

**Interview hook:** *"What happens when a downstream dependency is down?"* →
circuit breaker + bounded retries + fail-open fallback + audit flag.

---

## Drill 3 — Backpressure / peak-shaving (see LOADTEST.md)

Firing ~1,560 req/s at a 200 req/s-limited endpoint, the limiter shed the excess
(`429`) while Kafka absorbed a ~3,100-message burst and the consumer group
drained it to zero afterward — the producer never blocked on the slower,
gRPC-bound consumers. This is the message-queue "peak shaving" property made
concrete.

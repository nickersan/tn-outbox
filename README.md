# tn-outbox

A transactional outbox: a service adds a message in the same transaction as the change that caused it, and a relay sends it, again and
again until the receiver has it. A message is never added for a change that rolled back and never lost for one that committed. It is
sent **at least once**, so the receiver must take the same message twice without harm.

1. Add `src/main/resources/META-INF/tn-outbox/outbox.sql` to the service's own migrations (PostgreSQL).
2. Provide an `OutboxSender` bean that delivers `(destination, payload)`. Throw `PermanentFailureException` for a message the receiver
   will never take (it is set aside with the reason); any other exception sends it again after a pause that doubles up to a limit.
3. In the transaction of the change: `outbox.add("destination", jsonPayload)`. Outside a transaction it throws.

Several instances can run the relay: a batch is leased to the instance that takes it, so no message is sent from two at once, and one left
by an instance that died is taken again when the lease ends. Sent messages are removed after the retention period.

```yaml
tn:
  outbox:
    enabled: true        # whether to deliver; messages can still be added
    batch-size: 50
    poll-interval: 1s
    lease: 30s
    initial-backoff: 1s
    max-backoff: 5m
    retention: 7d
```

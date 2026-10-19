package com.tn.outbox;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Sends what was added to the {@link Outbox}, again and again until the receiver has it.
 *
 * <p>A batch is taken in a short transaction that also leases it: pushes the time each message is next due forward, so another
 * instance leaves it alone while this one sends it. The sending then happens outside any transaction, so a slow receiver holds no
 * locks. A message sent is marked, one that failed is due again after a pause that doubles with every failure, and one the sender says
 * can never be taken is set aside with the reason. An instance that dies mid-batch leaves its messages to be taken again when the lease ends.
 * Taken together this sends a message at least once, never at the same time from two instances.
 */
@Slf4j
public class OutboxRelay
{
  private static final String SQL_TAKE = """
    UPDATE outbox SET attempts = attempts + 1, next_attempt = :leaseEnds
    WHERE outbox_id IN (
      SELECT outbox_id FROM outbox
      WHERE sent IS NULL AND set_aside IS NULL AND next_attempt <= :now
      ORDER BY outbox_id
      LIMIT :limit
      FOR UPDATE SKIP LOCKED
    )
    RETURNING outbox_id, destination, payload, attempts
    """;
  private static final String SQL_SENT = "UPDATE outbox SET sent = :now WHERE outbox_id = :id";
  private static final String SQL_FAILED = "UPDATE outbox SET next_attempt = :next, last_failure = :failure WHERE outbox_id = :id";
  private static final String SQL_SET_ASIDE = "UPDATE outbox SET set_aside = :reason WHERE outbox_id = :id";
  private static final String SQL_PURGE = "DELETE FROM outbox WHERE sent IS NOT NULL AND sent < :before";

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;
  private final OutboxSender sender;
  private final OutboxProperties properties;
  private final Clock clock;

  public OutboxRelay(DataSource dataSource, PlatformTransactionManager transactionManager, OutboxSender sender, OutboxProperties properties, Clock clock)
  {
    this.jdbc = JdbcClient.create(dataSource);
    this.transactions = new TransactionTemplate(transactionManager);
    this.sender = sender;
    this.properties = properties;
    this.clock = clock;
  }

  /** Takes and sends one batch of the messages that are due; returns how many were taken. */
  public int relay()
  {
    List<Taken> batch = transactions.execute(status ->
    {
      OffsetDateTime now = OffsetDateTime.now(clock);

      return jdbc.sql(SQL_TAKE)
        .param("leaseEnds", now.plus(properties.lease()))
        .param("now", now)
        .param("limit", properties.batchSize())
        .query((rs, rowNum) -> new Taken(rs.getLong("outbox_id"), rs.getString("destination"), rs.getString("payload"), rs.getInt("attempts")))
        .list();
    });

    if (batch == null) return 0;
    for (Taken message : batch) send(message);

    return batch.size();
  }

  /** Removes the messages that were sent longer ago than they are kept for; returns how many. */
  public int purge()
  {
    return jdbc.sql(SQL_PURGE).param("before", OffsetDateTime.now(clock).minus(properties.retention())).update();
  }

  private void send(Taken message)
  {
    try
    {
      sender.send(message.destination, message.payload, message.attempts);
      jdbc.sql(SQL_SENT).param("now", OffsetDateTime.now(clock)).param("id", message.id).update();
    }
    catch (PermanentFailureException e)
    {
      log.error("Outbox message {} for {} set aside: {}", message.id, message.destination, e.getMessage());
      jdbc.sql(SQL_SET_ASIDE).param("reason", e.getMessage()).param("id", message.id).update();
    }
    catch (Exception e)
    {
      log.warn("Outbox message {} for {} not sent (attempt {}): {}", message.id, message.destination, message.attempts, e.toString());
      jdbc.sql(SQL_FAILED)
        .param("next", OffsetDateTime.now(clock).plus(properties.backoff(message.attempts)))
        .param("failure", String.valueOf(e))
        .param("id", message.id)
        .update();
    }
  }

  private record Taken(long id, String destination, String payload, int attempts) {}
}

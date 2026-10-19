package com.tn.outbox;

import java.time.Clock;
import java.time.OffsetDateTime;

import javax.sql.DataSource;

import io.hypersistence.tsid.TSID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Where a service puts a message it must send, in the same transaction as the change that caused it: the message is there if the change
 * committed and is not if it rolled back. {@link OutboxRelay} sends what is added.
 */
public class Outbox
{
  private static final String SQL_ADD =
    "INSERT INTO outbox (outbox_id, destination, payload, created, next_attempt) VALUES (:id, :destination, :payload, :now, :now)";

  private final JdbcClient jdbc;
  private final Clock clock;

  public Outbox(DataSource dataSource, Clock clock)
  {
    this.jdbc = JdbcClient.create(dataSource);
    this.clock = clock;
  }

  /**
   * @param destination what the message is for, up to 100 characters; the {@link OutboxSender} is told it
   * @param payload     the message, already in the form to be sent
   * @return the message's id, which sorts by when it was added
   * @throws IllegalStateException if there is no transaction, since the message would then not be tied to any change
   */
  public long add(String destination, String payload)
  {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) throw new IllegalStateException("An outbox message must be added in the transaction of the change it is for");

    long id = TSID.Factory.getTsid().toLong();
    jdbc.sql(SQL_ADD)
      .param("id", id)
      .param("destination", destination)
      .param("payload", payload)
      .param("now", OffsetDateTime.now(clock))
      .update();

    return id;
  }
}

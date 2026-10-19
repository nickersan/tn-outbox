package com.tn.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

class OutboxTest
{
  @Test
  void shouldRefuseMessageWhenNoTransaction()
  {
    Outbox outbox = new Outbox(mock(DataSource.class), Clock.systemUTC());

    assertThrows(IllegalStateException.class, () -> outbox.add("router", "{}"));
  }

  @Test
  void shouldDoubleBackoffWithEachFailureUpToMaximum()
  {
    OutboxProperties properties = new OutboxProperties(true, 50, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(1), Duration.ofSeconds(60), Duration.ofDays(7));

    assertEquals(Duration.ofSeconds(1), properties.backoff(1));
    assertEquals(Duration.ofSeconds(2), properties.backoff(2));
    assertEquals(Duration.ofSeconds(32), properties.backoff(6));
    assertEquals(Duration.ofSeconds(60), properties.backoff(7));
    assertEquals(Duration.ofSeconds(60), properties.backoff(500));
  }
}

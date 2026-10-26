package com.tn.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Clock;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;

class OutboxRelayTest
{
  private static final OutboxProperties PROPERTIES = new OutboxProperties(true, 50, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(1), Duration.ofMinutes(5), Duration.ofDays(7));

  @Test
  void shouldTakeNothingWhenTransactionGivesNoBatch()
  {
    OutboxSender sender = mock(OutboxSender.class);
    OutboxRelay relay = new OutboxRelay(mock(DataSource.class), mock(CallbackPreferringPlatformTransactionManager.class), sender, PROPERTIES, Clock.systemUTC());

    assertEquals(0, relay.relay());
    verifyNoInteractions(sender);
  }

  @Test
  void shouldKeepReasonAndCauseWhenFailurePermanent()
  {
    Throwable cause = new IllegalArgumentException("bad");
    PermanentFailureException failure = new PermanentFailureException("malformed", cause);

    assertEquals("malformed", failure.getMessage());
    assertSame(cause, failure.getCause());
  }

  @Test
  void shouldKeepReasonWhenFailurePermanent()
  {
    assertEquals("rejected", new PermanentFailureException("rejected").getMessage());
  }
}

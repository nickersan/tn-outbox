package com.tn.outbox;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class OutboxRelayLifecycleTest
{
  private static final Duration SHORT = Duration.ofMillis(10);
  private static final Duration LONG = Duration.ofHours(1);
  private static final int BATCH_SIZE = 5;
  private static final String THREAD_NAME = "outbox-relay";
  private static final long WAIT_MILLIS = 5000;

  private final OutboxRelay relay = mock(OutboxRelay.class);

  private OutboxRelayLifecycle lifecycle;

  @AfterEach
  void stop()
  {
    if (lifecycle != null) lifecycle.stop();
  }

  @Test
  void shouldNotBeRunningUntilStarted()
  {
    lifecycle = lifecycle(SHORT);

    assertFalse(lifecycle.isRunning());
  }

  @Test
  void shouldBeRunningOnceStartedAndNotOnceStopped()
  {
    lifecycle = lifecycle(SHORT);

    lifecycle.start();
    assertTrue(lifecycle.isRunning());

    lifecycle.stop();
    assertFalse(lifecycle.isRunning());
    assertFalse(relayThreadAlive());
  }

  @Test
  void shouldRelayAndPurgeStraightAwayWhenStarted()
  {
    lifecycle = lifecycle(LONG);

    lifecycle.start();

    verify(relay, timeout(WAIT_MILLIS)).relay();
    verify(relay, timeout(WAIT_MILLIS)).purge();
  }

  @Test
  void shouldPollAgainAfterTheIntervalWhenBatchNotFull()
  {
    when(relay.relay()).thenReturn(BATCH_SIZE - 1);
    lifecycle = lifecycle(SHORT);

    lifecycle.start();

    verify(relay, timeout(WAIT_MILLIS).atLeast(3)).relay();
  }

  @Test
  void shouldRelayAgainWithoutWaitingWhenBatchFull()
  {
    when(relay.relay()).thenReturn(BATCH_SIZE, BATCH_SIZE, BATCH_SIZE, 0);
    lifecycle = lifecycle(LONG);

    lifecycle.start();

    verify(relay, timeout(WAIT_MILLIS).times(4)).relay();
  }

  @Test
  void shouldNotPurgeAgainWithinPurgeInterval()
  {
    lifecycle = lifecycle(SHORT);

    lifecycle.start();

    verify(relay, timeout(WAIT_MILLIS).atLeast(5)).relay();
    verify(relay, times(1)).purge();
  }

  @Test
  void shouldPurgeAgainOncePurgeIntervalPassed()
  {
    lifecycle = new OutboxRelayLifecycle(relay, properties(SHORT), Duration.ofMillis(20));

    lifecycle.start();

    verify(relay, timeout(WAIT_MILLIS).atLeast(3)).purge();
  }

  @Test
  void shouldKeepRelayingWhenRelayFails()
  {
    when(relay.relay()).thenThrow(new IllegalStateException("database away")).thenReturn(0);
    lifecycle = lifecycle(SHORT);

    lifecycle.start();

    verify(relay, timeout(WAIT_MILLIS).atLeast(3)).relay();
    assertTrue(lifecycle.isRunning());
  }

  @Test
  void shouldNotPurgeWhenRelayFailsFirst()
  {
    when(relay.relay()).thenThrow(new IllegalStateException("database away"));
    lifecycle = lifecycle(SHORT);

    lifecycle.start();

    verify(relay, timeout(WAIT_MILLIS).atLeast(2)).relay();
    verify(relay, never()).purge();
  }

  @Test
  void shouldStopWhileWaitingToPoll()
  {
    lifecycle = lifecycle(LONG);
    lifecycle.start();
    verify(relay, timeout(WAIT_MILLIS)).purge();

    lifecycle.stop();

    assertFalse(relayThreadAlive());
  }

  @Test
  void shouldStopWhilePausedAfterFailure()
  {
    when(relay.relay()).thenThrow(new IllegalStateException("database away"));
    lifecycle = lifecycle(LONG);
    lifecycle.start();
    verify(relay, timeout(WAIT_MILLIS)).relay();

    lifecycle.stop();

    assertFalse(lifecycle.isRunning());
    assertFalse(relayThreadAlive());
  }

  @Test
  void shouldDoNothingWhenStoppedBeforeStarted()
  {
    lifecycle = lifecycle(SHORT);

    lifecycle.stop();

    assertFalse(lifecycle.isRunning());
    verify(relay, never()).relay();
  }

  @Test
  void shouldKeepInterruptWhenInterruptedWhileStopping()
  {
    lifecycle = lifecycle(LONG);
    lifecycle.start();

    Thread.currentThread().interrupt();
    lifecycle.stop();

    assertTrue(Thread.interrupted());
  }

  private OutboxRelayLifecycle lifecycle(Duration pollInterval)
  {
    return new OutboxRelayLifecycle(relay, properties(pollInterval));
  }

  private static OutboxProperties properties(Duration pollInterval)
  {
    return new OutboxProperties(true, BATCH_SIZE, pollInterval, Duration.ofSeconds(30), Duration.ofSeconds(1), Duration.ofMinutes(5), Duration.ofDays(7));
  }

  private static boolean relayThreadAlive()
  {
    return Thread.getAllStackTraces().keySet().stream().anyMatch(thread -> THREAD_NAME.equals(thread.getName()) && thread.isAlive());
  }
}

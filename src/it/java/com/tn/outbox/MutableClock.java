package com.tn.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** A clock the test moves on. */
class MutableClock extends Clock
{
  private volatile Instant now = Instant.now();

  void advance(Duration duration)
  {
    now = now.plus(duration);
  }

  @Override
  public ZoneId getZone()
  {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone)
  {
    return this;
  }

  @Override
  public Instant instant()
  {
    return now;
  }
}

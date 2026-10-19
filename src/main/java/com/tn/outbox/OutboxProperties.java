package com.tn.outbox;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param enabled        whether to deliver messages at all; messages can still be added
 * @param batchSize      how many messages are taken at once
 * @param pollInterval   how long to wait before looking for messages when the last look found fewer than a batch
 * @param lease          how long a message that has been taken is left alone before another instance may take it, should this one have died
 * @param initialBackoff how long to wait before sending a message again after its first failure; it doubles with each failure
 * @param maxBackoff     the longest to wait between attempts
 * @param retention      how long a message that was sent is kept
 */
@ConfigurationProperties("tn.outbox")
public record OutboxProperties(
  @DefaultValue("true") boolean enabled,
  @DefaultValue("50") int batchSize,
  @DefaultValue("1s") Duration pollInterval,
  @DefaultValue("30s") Duration lease,
  @DefaultValue("1s") Duration initialBackoff,
  @DefaultValue("5m") Duration maxBackoff,
  @DefaultValue("7d") Duration retention
)
{
  /** How long to wait after the given number of attempts have failed: the initial backoff, doubled for each attempt after the first, up to the longest. */
  Duration backoff(int attempts)
  {
    long doublings = Math.min(Math.max(attempts - 1, 0), 30);
    Duration backoff = initialBackoff.multipliedBy(1L << doublings);

    return backoff.compareTo(maxBackoff) > 0 || backoff.isNegative() ? maxBackoff : backoff;
  }
}

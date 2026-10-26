package com.tn.outbox;

import java.time.Duration;

import lombok.extern.slf4j.Slf4j;

import org.springframework.context.SmartLifecycle;

/**
 * Runs {@link OutboxRelay} for the life of the process: straight on while there are full batches to send, otherwise after the poll
 * interval, and removes what has been kept long enough now and then. Stopping waits for the batch in hand.
 */
@Slf4j
public class OutboxRelayLifecycle implements SmartLifecycle
{
  private static final Duration PURGE_EVERY = Duration.ofMinutes(10);
  private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);

  private final OutboxRelay relay;
  private final OutboxProperties properties;
  private final Duration purgeEvery;

  private volatile boolean running;
  private Thread thread;

  public OutboxRelayLifecycle(OutboxRelay relay, OutboxProperties properties)
  {
    this(relay, properties, PURGE_EVERY);
  }

  OutboxRelayLifecycle(OutboxRelay relay, OutboxProperties properties, Duration purgeEvery)
  {
    this.relay = relay;
    this.properties = properties;
    this.purgeEvery = purgeEvery;
  }

  @Override
  public void start()
  {
    running = true;
    thread = Thread.ofPlatform().daemon().name("outbox-relay").start(this::run);
  }

  @Override
  public void stop()
  {
    running = false;
    if (thread == null) return;

    thread.interrupt();
    try
    {
      thread.join(STOP_TIMEOUT.toMillis());
    }
    catch (InterruptedException e)
    {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean isRunning()
  {
    return running;
  }

  private void run()
  {
    long purgeDue = System.nanoTime();

    while (running)
    {
      try
      {
        int taken = relay.relay();

        if (System.nanoTime() - purgeDue >= 0)
        {
          relay.purge();
          purgeDue = System.nanoTime() + purgeEvery.toNanos();
        }

        if (taken < properties.batchSize()) Thread.sleep(properties.pollInterval());
      }
      catch (InterruptedException e)
      {
        Thread.currentThread().interrupt();
        return;
      }
      catch (RuntimeException e)
      {
        // the database being away must not end the relay
        log.warn("Outbox relay failed; trying again", e);
        pause();
      }
    }
  }

  private void pause()
  {
    try
    {
      Thread.sleep(properties.pollInterval());
    }
    catch (InterruptedException e)
    {
      Thread.currentThread().interrupt();
      running = false;
    }
  }
}

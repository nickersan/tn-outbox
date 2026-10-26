package com.tn.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.transaction.PlatformTransactionManager;

class OutboxAutoConfigurationTest
{
  private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
  private static final String SLOW_POLL = "tn.outbox.poll-interval=1h";

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class))
    .withPropertyValues(SLOW_POLL);

  @Test
  void shouldProvideNothingWhenNoDataSource()
  {
    runner.run(context ->
    {
      assertEquals(0, context.getBeanNamesForType(Outbox.class).length);
      assertEquals(0, context.getBeanNamesForType(OutboxRelay.class).length);
      assertEquals(0, context.getBeanNamesForType(OutboxRelayLifecycle.class).length);
    });
  }

  @Test
  void shouldProvideOutboxAndClockWhenDataSource()
  {
    runner.withBean(DataSource.class, () -> mock(DataSource.class)).run(context ->
    {
      assertEquals(1, context.getBeanNamesForType(Outbox.class).length);
      assertEquals(1, context.getBeanNamesForType(Clock.class).length);
      assertEquals(0, context.getBeanNamesForType(OutboxRelay.class).length);
      assertEquals(0, context.getBeanNamesForType(OutboxRelayLifecycle.class).length);
    });
  }

  @Test
  void shouldNotProvideRelayWhenNoSender()
  {
    runner
      .withBean(DataSource.class, () -> mock(DataSource.class))
      .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
      .run(context -> assertEquals(0, context.getBeanNamesForType(OutboxRelay.class).length));
  }

  @Test
  void shouldNotProvideRelayWhenNoTransactionManager()
  {
    runner
      .withBean(DataSource.class, () -> mock(DataSource.class))
      .withBean(OutboxSender.class, () -> mock(OutboxSender.class))
      .run(context -> assertEquals(0, context.getBeanNamesForType(OutboxRelay.class).length));
  }

  @Test
  void shouldProvideRelayAndLifecycleWhenSender()
  {
    withSender(runner).run(context ->
    {
      assertEquals(1, context.getBeanNamesForType(OutboxRelay.class).length);
      assertEquals(1, context.getBeanNamesForType(OutboxRelayLifecycle.class).length);
      assertEquals(true, context.getBean(OutboxRelayLifecycle.class).isRunning());
    });
  }

  @Test
  void shouldNotProvideLifecycleWhenDisabled()
  {
    withSender(runner).withPropertyValues("tn.outbox.enabled=false").run(context ->
    {
      assertEquals(1, context.getBeanNamesForType(OutboxRelay.class).length);
      assertEquals(0, context.getBeanNamesForType(OutboxRelayLifecycle.class).length);
    });
  }

  @Test
  void shouldBindProperties()
  {
    runner.withBean(DataSource.class, () -> mock(DataSource.class)).withPropertyValues("tn.outbox.batch-size=7").run(context ->
      assertEquals(7, context.getBean(OutboxProperties.class).batchSize())
    );
  }

  @Test
  void shouldUseDefaultPropertiesWhenNoneGiven()
  {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class))
      .withBean(DataSource.class, () -> mock(DataSource.class))
      .run(context ->
      {
        assertEquals(50, context.getBean(OutboxProperties.class).batchSize());
        assertEquals(true, context.getBean(OutboxProperties.class).enabled());
      });
  }

  @Test
  void shouldKeepProvidedClock()
  {
    runner.withBean(DataSource.class, () -> mock(DataSource.class)).withBean(Clock.class, () -> CLOCK).run(context ->
    {
      assertEquals(1, context.getBeanNamesForType(Clock.class).length);
      assertSame(CLOCK, context.getBean(Clock.class));
    });
  }

  @Test
  void shouldKeepProvidedOutboxAndRelay()
  {
    Outbox outbox = new Outbox(mock(DataSource.class), CLOCK);
    OutboxRelay relay = mock(OutboxRelay.class);

    runner
      .withBean(DataSource.class, () -> mock(DataSource.class))
      .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
      .withBean(OutboxSender.class, () -> mock(OutboxSender.class))
      .withBean(Outbox.class, () -> outbox)
      .withBean(OutboxRelay.class, () -> relay)
      .run(context ->
      {
        assertSame(outbox, context.getBean(Outbox.class));
        assertSame(relay, context.getBean(OutboxRelay.class));
      });
  }

  private static ApplicationContextRunner withSender(ApplicationContextRunner runner)
  {
    return runner
      .withBean(DataSource.class, () -> mock(DataSource.class))
      .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
      .withBean(OutboxSender.class, () -> mock(OutboxSender.class));
  }
}

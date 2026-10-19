package com.tn.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

class OutboxIntegrationTest
{
  private static final String DESTINATION = "event-routing-service";
  private static final Duration BACKOFF = Duration.ofSeconds(1);

  private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17");

  private static JdbcTemplate jdbc;
  private static DataSource dataSource;
  private static DataSourceTransactionManager transactionManager;

  private final MutableClock clock = new MutableClock();
  private final OutboxProperties properties = new OutboxProperties(true, 50, Duration.ofMillis(10), Duration.ofSeconds(30), BACKOFF, Duration.ofMinutes(5), Duration.ofDays(7));

  @BeforeAll
  static void startDatabase() throws IOException
  {
    postgres.start();

    dataSource = new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    transactionManager = new DataSourceTransactionManager(dataSource);
    jdbc = new JdbcTemplate(dataSource);

    jdbc.execute(new ClassPathResource("META-INF/tn-outbox/outbox.sql").getContentAsString(java.nio.charset.StandardCharsets.UTF_8));
  }

  @BeforeEach
  void emptyOutbox()
  {
    jdbc.execute("TRUNCATE outbox");
  }

  @Test
  void shouldLeaveNoMessageWhenChangeRollsBack()
  {
    Outbox outbox = outbox();

    assertThrows(IllegalStateException.class, () -> new TransactionTemplate(transactionManager).executeWithoutResult(status ->
    {
      outbox.add(DESTINATION, "{\"n\":1}");
      throw new IllegalStateException("the change failed");
    }));

    assertEquals(0, count("TRUE"));
  }

  @Test
  void shouldSendMessageOnceChangeCommits()
  {
    List<String> received = new CopyOnWriteArrayList<>();
    add("{\"n\":1}");

    int taken = relay((destination, payload) -> received.add(destination + " " + payload)).relay();

    assertEquals(1, taken);
    assertEquals(List.of(DESTINATION + " {\"n\":1}"), received);
    assertEquals(1, count("sent IS NOT NULL"));
  }

  @Test
  void shouldSendAfterReceiverWasDown()
  {
    AtomicInteger attempts = new AtomicInteger();
    List<String> received = new CopyOnWriteArrayList<>();
    OutboxRelay relay = relay((destination, payload) ->
    {
      if (attempts.incrementAndGet() <= 2) throw new IOException("the receiver is down");
      received.add(payload);
    });
    add("{\"n\":1}");

    relay.relay();
    assertEquals(0, relay.relay(), "a message that failed waits for its pause");
    clock.advance(BACKOFF.plusMillis(1));
    relay.relay();
    clock.advance(BACKOFF.multipliedBy(2).plusMillis(1));
    relay.relay();

    assertEquals(3, attempts.get());
    assertEquals(List.of("{\"n\":1}"), received);
    assertEquals(1, count("sent IS NOT NULL"));
    assertNotNull(jdbc.queryForObject("SELECT last_failure FROM outbox", String.class));
  }

  @Test
  void shouldTellSenderWhichAttemptItIs()
  {
    List<Integer> attempts = new CopyOnWriteArrayList<>();
    OutboxRelay relay = relay(new OutboxSender()
    {
      @Override
      public void send(String destination, String payload)
      {
        throw new IllegalStateException("the attempt should be asked for");
      }

      @Override
      public void send(String destination, String payload, int attempt) throws IOException
      {
        attempts.add(attempt);
        if (attempt < 3) throw new IOException("down");
      }
    });
    add("{}");

    for (int i = 0; i < 3; i++)
    {
      relay.relay();
      clock.advance(BACKOFF.multipliedBy(4));
    }

    assertEquals(List.of(1, 2, 3), attempts);
  }

  @Test
  void shouldSetMessageAsideWhenReceiverNeverTakesIt()
  {
    AtomicInteger attempts = new AtomicInteger();
    OutboxRelay relay = relay((destination, payload) ->
    {
      attempts.incrementAndGet();
      throw new PermanentFailureException("malformed");
    });
    add("{}");

    relay.relay();
    clock.advance(Duration.ofHours(1));
    relay.relay();

    assertEquals(1, attempts.get());
    assertEquals("malformed", jdbc.queryForObject("SELECT set_aside FROM outbox", String.class));
    assertNull(jdbc.queryForObject("SELECT sent FROM outbox", java.time.OffsetDateTime.class));
  }

  @Test
  void shouldLeaveMessageAloneWhileAnotherInstanceHasIt()
  {
    List<String> received = new CopyOnWriteArrayList<>();
    add("{\"n\":1}");
    // as though another instance took the message, and died before it could send it
    jdbc.update("UPDATE outbox SET attempts = 1, next_attempt = now() + interval '30 seconds'");
    OutboxRelay relay = relay((destination, payload) -> received.add(payload));

    assertEquals(0, relay.relay());
    clock.advance(Duration.ofSeconds(31));
    assertEquals(1, relay.relay());

    assertEquals(1, received.size());
  }

  @Test
  void shouldSendEachMessageOnceWhenInstancesRelayAtTheSameTime() throws Exception
  {
    int messages = 300;
    for (int i = 0; i < messages; i++) add("{\"n\":" + i + "}");

    Map<String, AtomicInteger> sends = new ConcurrentHashMap<>();
    OutboxProperties small = new OutboxProperties(true, 10, Duration.ofMillis(10), Duration.ofSeconds(30), BACKOFF, Duration.ofMinutes(5), Duration.ofDays(7));
    ExecutorService instances = Executors.newFixedThreadPool(4);
    List<Future<?>> done = new ArrayList<>();

    for (int i = 0; i < 4; i++)
    {
      OutboxRelay instance = new OutboxRelay(dataSource, transactionManager, (destination, payload) ->
      {
        sends.computeIfAbsent(payload, p -> new AtomicInteger()).incrementAndGet();
        Thread.sleep(1);
      }, small, clock);
      done.add(instances.submit(() ->
      {
        while (instance.relay() > 0) { /* until none are due */ }
      }));
    }
    for (Future<?> future : done) future.get(60, TimeUnit.SECONDS);
    instances.shutdown();

    assertEquals(messages, sends.size());
    sends.forEach((payload, count) -> assertEquals(1, count.get(), payload));
    assertEquals(messages, count("sent IS NOT NULL"));
  }

  @Test
  void shouldRemoveSentMessagesOnlyAfterTheyHaveBeenKeptLongEnough()
  {
    OutboxRelay relay = relay((destination, payload) -> {});
    add("{\"n\":1}");
    relay.relay();
    add("{\"n\":2}");

    assertEquals(0, relay.purge());
    clock.advance(Duration.ofDays(8));

    assertEquals(1, relay.purge());
    assertEquals(1, count("TRUE"));
    assertEquals(1, count("sent IS NULL"));
  }

  private Outbox outbox()
  {
    return new Outbox(dataSource, clock);
  }

  private void add(String payload)
  {
    new TransactionTemplate(transactionManager).executeWithoutResult(status -> outbox().add(DESTINATION, payload));
  }

  private OutboxRelay relay(OutboxSender sender)
  {
    return new OutboxRelay(dataSource, transactionManager, sender, properties, clock);
  }

  private int count(String where)
  {
    return jdbc.queryForObject("SELECT count(*) FROM outbox WHERE " + where, Integer.class);
  }
}

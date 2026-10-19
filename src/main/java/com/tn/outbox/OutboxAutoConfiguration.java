package com.tn.outbox;

import java.time.Clock;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Gives a service with a data source the {@link Outbox} to add messages to, and, once it provides an {@link OutboxSender}, the relay that
 * sends them.
 */
@AutoConfiguration(afterName = {
  "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
  "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration",
  "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
})
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration
{
  @Bean
  @ConditionalOnMissingBean
  Clock outboxClock()
  {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean
  Outbox outbox(DataSource dataSource, Clock clock)
  {
    return new Outbox(dataSource, clock);
  }

  @Bean
  @ConditionalOnBean({OutboxSender.class, PlatformTransactionManager.class})
  @ConditionalOnMissingBean
  OutboxRelay outboxRelay(DataSource dataSource, PlatformTransactionManager transactionManager, OutboxSender sender, OutboxProperties properties, Clock clock)
  {
    return new OutboxRelay(dataSource, transactionManager, sender, properties, clock);
  }

  @Bean
  @ConditionalOnBean(OutboxRelay.class)
  @ConditionalOnProperty(prefix = "tn.outbox", name = "enabled", matchIfMissing = true)
  OutboxRelayLifecycle outboxRelayLifecycle(OutboxRelay relay, OutboxProperties properties)
  {
    return new OutboxRelayLifecycle(relay, properties);
  }
}

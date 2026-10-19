package com.tn.outbox;

/**
 * Delivers a message to where it is going: the service provides one. A message is sent at least once, so the receiver must take the
 * same message again without harm.
 */
public interface OutboxSender
{
  /**
   * @param destination what the message was added for
   * @param payload     the message as it was added
   * @throws PermanentFailureException if the receiver will never take this message, however often it is sent, so it is set aside
   * @throws Exception                 for any other failure, in which case the message is sent again after a pause
   */
  void send(String destination, String payload) throws Exception;

  /**
   * As {@link #send(String, String)}, told which attempt this is (1 for the first), for a sender that gives up after a number of them.
   * Override this one to use the attempt; by default it is ignored.
   */
  default void send(String destination, String payload, int attempt) throws Exception
  {
    send(destination, payload);
  }
}

package com.tn.outbox;

/** The receiver will never take the message: it is set aside with this reason, and not sent again. */
public class PermanentFailureException extends RuntimeException
{
  public PermanentFailureException(String reason)
  {
    super(reason);
  }

  public PermanentFailureException(String reason, Throwable cause)
  {
    super(reason, cause);
  }
}

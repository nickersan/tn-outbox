-- The table the outbox keeps its messages in. Add it to the service's own migrations.
CREATE TABLE outbox
(
  outbox_id    BIGINT       NOT NULL PRIMARY KEY,
  destination  VARCHAR(100) NOT NULL,
  payload      TEXT         NOT NULL,
  created      TIMESTAMPTZ  NOT NULL,
  attempts     INT          NOT NULL DEFAULT 0,
  next_attempt TIMESTAMPTZ  NOT NULL,
  sent         TIMESTAMPTZ,
  set_aside    TEXT,
  last_failure TEXT
);

CREATE INDEX outbox_pending ON outbox (next_attempt) WHERE sent IS NULL AND set_aside IS NULL;

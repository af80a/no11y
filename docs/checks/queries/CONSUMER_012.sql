CREATE OR REPLACE MACRO audit.check_consumer_012(epoch_start, epoch_end) AS TABLE
  SELECT
    'CONSUMER_012' AS code,
    ai.name || ' / ' || si.name || ' / ' || c.name AS entity,
    c.pk AS entity_pk,
    c.priority_policy AS current_policy,
    c.epoch
  FROM hx.consumers c
  INNER JOIN hx.stream_ident si ON c.stream_pk = si.pk
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
  WHERE c.epoch BETWEEN epoch_start AND epoch_end
    AND c.is_leader = true
    AND json_extract_string(c.metadata, '$."io.nats.monitor.enabled"') = 'true'
    AND json_extract_string(c.metadata, '$."io.nats.monitor.pinned"') = 'true'
    AND c.priority_policy != 'pinned_client';

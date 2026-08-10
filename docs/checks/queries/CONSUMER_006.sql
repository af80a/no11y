CREATE OR REPLACE MACRO audit.check_consumer_006(epoch_start, epoch_end) AS TABLE
  SELECT
    'CONSUMER_006' AS code,
    ai.name || ' / ' || si.name || ' / ' || c.name AS entity,
    c.pk AS entity_pk,
    c.num_ack_pending::BIGINT AS current_val,
    CAST(json_extract_string(c.metadata, '$."io.nats.monitor.outstanding-ack-critical"') AS BIGINT) AS threshold,
    c.epoch
  FROM hx.consumers c
  INNER JOIN hx.stream_ident si ON c.stream_pk = si.pk
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
  WHERE c.epoch BETWEEN epoch_start AND epoch_end
    AND c.is_leader = true
    AND json_extract_string(c.metadata, '$."io.nats.monitor.enabled"') = 'true'
    AND json_extract_string(c.metadata, '$."io.nats.monitor.outstanding-ack-critical"') IS NOT NULL
    AND CAST(json_extract_string(c.metadata, '$."io.nats.monitor.outstanding-ack-critical"') AS BIGINT) > 0
    AND c.num_ack_pending >= CAST(json_extract_string(c.metadata, '$."io.nats.monitor.outstanding-ack-critical"') AS BIGINT);

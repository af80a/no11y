CREATE OR REPLACE MACRO audit.check_consumer_010(epoch_start, epoch_end) AS TABLE
  -- Parse the duration threshold once per row (see CONSUMER_009 for rationale).
  WITH evaluated AS (
    SELECT
      c.pk AS pk,
      c.stream_pk AS stream_pk,
      c.name AS name,
      epoch_ns(c.epoch) - epoch_ns(c.ack_floor_last) AS elapsed_ns,
      audit.parse_duration_ns(json_extract_string(c.metadata, '$."io.nats.monitor.last-ack-critical"')) AS threshold_ns,
      c.epoch AS epoch
    FROM hx.consumers c
    WHERE c.epoch BETWEEN epoch_start AND epoch_end
      AND c.is_leader = true
      AND json_extract_string(c.metadata, '$."io.nats.monitor.enabled"') = 'true'
      AND json_extract_string(c.metadata, '$."io.nats.monitor.last-ack-critical"') IS NOT NULL
      AND c.ack_floor_last IS NOT NULL
  )
  SELECT
    'CONSUMER_010' AS code,
    ai.name || ' / ' || si.name || ' / ' || e.name AS entity,
    e.pk AS entity_pk,
    e.elapsed_ns AS elapsed_ns,
    e.threshold_ns AS threshold_ns,
    e.epoch
  FROM evaluated e
  INNER JOIN hx.stream_ident si ON e.stream_pk = si.pk
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
  WHERE e.threshold_ns > 0
    AND e.elapsed_ns >= e.threshold_ns;

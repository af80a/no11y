CREATE OR REPLACE MACRO audit.check_jetstream_017(epoch_start, epoch_end) AS TABLE
  SELECT
    'JETSTREAM_017' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    s.mirror_lag::BIGINT AS current_val,
    CAST(json_extract_string(s.metadata, '$."io.nats.monitor.lag-critical"') AS BIGINT) AS threshold,
    s.epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.is_leader = true
    AND s.is_mirror = true
    AND json_extract_string(s.metadata, '$."io.nats.monitor.enabled"') = 'true'
    AND json_extract_string(s.metadata, '$."io.nats.monitor.lag-critical"') IS NOT NULL
    AND s.mirror_lag > CAST(json_extract_string(s.metadata, '$."io.nats.monitor.lag-critical"') AS BIGINT);

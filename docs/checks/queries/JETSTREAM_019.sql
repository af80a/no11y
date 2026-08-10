CREATE OR REPLACE MACRO audit.check_jetstream_019(epoch_start, epoch_end) AS TABLE
  SELECT
    'JETSTREAM_019' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    s.num_sources::BIGINT AS current_val,
    CAST(json_extract_string(s.metadata, '$."io.nats.monitor.min-sources"') AS BIGINT) AS threshold,
    s.epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.is_leader = true
    AND json_extract_string(s.metadata, '$."io.nats.monitor.enabled"') = 'true'
    AND json_extract_string(s.metadata, '$."io.nats.monitor.min-sources"') IS NOT NULL
    AND s.num_sources < CAST(json_extract_string(s.metadata, '$."io.nats.monitor.min-sources"') AS BIGINT);

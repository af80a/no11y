CREATE OR REPLACE MACRO audit.check_jetstream_022(epoch_start, epoch_end) AS TABLE
  SELECT
    'JETSTREAM_022' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    srv.name AS replica_server,
    s.lag::BIGINT AS current_val,
    CAST(json_extract_string(s.metadata, '$."io.nats.monitor.peer-lag-critical"') AS BIGINT) AS threshold,
    s.epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  INNER JOIN hx.server_ident srv ON s.peer_server_pk = srv.pk
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.peer_server_pk != 0 -- Exclude self-reported rows; leader-reported peer rows carry the authoritative lag.
    AND s.is_leader = false
    AND json_extract_string(s.metadata, '$."io.nats.monitor.enabled"') = 'true'
    AND json_extract_string(s.metadata, '$."io.nats.monitor.peer-lag-critical"') IS NOT NULL
    AND s.lag > CAST(json_extract_string(s.metadata, '$."io.nats.monitor.peer-lag-critical"') AS BIGINT);

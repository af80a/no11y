CREATE OR REPLACE MACRO audit.check_jetstream_023(epoch_start, epoch_end) AS TABLE
  SELECT
    'JETSTREAM_023' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    srv.name AS replica_server,
    s.active AS current_ns,
    audit.parse_duration_ns(json_extract_string(s.metadata, '$."io.nats.monitor.peer-seen-critical"')) AS threshold_ns,
    s.epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  INNER JOIN hx.server_ident srv ON s.peer_server_pk = srv.pk
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.peer_server_pk != 0 -- Exclude self-reported rows; leader-reported peer rows carry the authoritative active time.
    AND s.is_leader = false
    AND json_extract_string(s.metadata, '$."io.nats.monitor.enabled"') = 'true'
    -- Gate on the parsed value: an unparseable duration must not silently pass the raw IS NOT NULL check.
    AND audit.parse_duration_ns(json_extract_string(s.metadata, '$."io.nats.monitor.peer-seen-critical"')) IS NOT NULL
    AND s.active > audit.parse_duration_ns(json_extract_string(s.metadata, '$."io.nats.monitor.peer-seen-critical"'));

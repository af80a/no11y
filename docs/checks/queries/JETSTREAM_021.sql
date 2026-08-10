CREATE OR REPLACE MACRO audit.check_jetstream_021(epoch_start, epoch_end) AS TABLE
  WITH peer_counts AS (
    SELECT
      stream_pk,
      epoch,
      COUNT(DISTINCT COALESCE(NULLIF(peer_server_pk, 0), server_pk)) AS actual_peers
    FROM hx.stream_replica_stats
    WHERE epoch BETWEEN epoch_start AND epoch_end
    GROUP BY stream_pk, epoch
  )
  SELECT
    'JETSTREAM_021' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    pc.actual_peers::BIGINT AS current_val,
    CAST(json_extract_string(s.metadata, '$."io.nats.monitor.peer-expect"') AS BIGINT) AS threshold,
    s.epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  INNER JOIN peer_counts pc ON s.pk = pc.stream_pk AND s.epoch = pc.epoch
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.is_leader = true
    AND json_extract_string(s.metadata, '$."io.nats.monitor.enabled"') = 'true'
    AND json_extract_string(s.metadata, '$."io.nats.monitor.peer-expect"') IS NOT NULL
    AND pc.actual_peers != CAST(json_extract_string(s.metadata, '$."io.nats.monitor.peer-expect"') AS BIGINT);

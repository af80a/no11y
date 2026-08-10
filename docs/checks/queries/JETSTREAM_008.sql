CREATE OR REPLACE MACRO audit.check_jetstream_008(epoch_start, epoch_end) AS TABLE
  WITH per_stream AS (
    SELECT
      s.pk,
      s.epoch,
      s.num_replicas,
      s.is_kv,
      s.is_object,
      COUNT(DISTINCT s.peer_server_pk) FILTER (WHERE s.is_offline = true) AS offline_count
    FROM hx.streams s
    WHERE s.epoch BETWEEN epoch_start AND epoch_end
      AND s.num_replicas > 1
    GROUP BY s.pk, s.epoch, s.num_replicas, s.is_kv, s.is_object
  )
  SELECT
    'JETSTREAM_008' AS code,
    ai.name || ' / ' || si.name AS entity,
    p.pk AS entity_pk,
    CASE WHEN p.is_kv THEN 'kvstore' WHEN p.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    p.offline_count,
    p.num_replicas,
    p.num_replicas // 2 + 1 AS quorum_needed,
    p.epoch
  FROM per_stream p
  INNER JOIN hx.stream_ident si ON si.pk = p.pk
  INNER JOIN hx.account_ident ai ON ai.pk = si.account_pk
  WHERE p.offline_count * 2 > p.num_replicas;

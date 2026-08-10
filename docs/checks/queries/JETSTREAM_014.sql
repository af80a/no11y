CREATE OR REPLACE MACRO audit.check_jetstream_014(epoch_start, epoch_end, p_divergence_percent := 5.0, p_min_divergence := 1000) AS TABLE
  WITH per_stream AS (
    SELECT
      s.pk,
      s.epoch,
      s.account_pk,
      s.name,
      s.is_kv,
      s.is_object,
      MIN(s.msgs) AS min_msgs,
      MAX(s.msgs) AS max_msgs,
      COUNT(*) AS replica_count
    FROM hx.streams s
    WHERE s.epoch BETWEEN epoch_start AND epoch_end
      AND s.num_replicas > 1
      AND s.peer_server_pk = 0 -- Only self-reported rows carry real msgs data.
      AND s.is_current = true -- Catching-up replicas legitimately trail; only divergence among current replicas is an anomaly.
    GROUP BY s.pk, s.epoch, s.account_pk, s.name, s.is_kv, s.is_object
    HAVING COUNT(*) > 1
  )
  SELECT
    'JETSTREAM_014' AS code,
    ai.name || ' / ' || p.name AS entity,
    p.pk AS entity_pk,
    CASE WHEN p.is_kv THEN 'kvstore' WHEN p.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    p.min_msgs,
    p.max_msgs,
    round((p.max_msgs - p.min_msgs) * 100.0 / p.max_msgs, 1) AS divergence_pct,
    p.epoch
  FROM per_stream p
  INNER JOIN hx.account_ident ai ON p.account_pk = ai.pk
  WHERE p.max_msgs > 0
    AND (p.max_msgs - p.min_msgs) > p_min_divergence
    AND (p.max_msgs - p.min_msgs) * 100.0 / p.max_msgs > p_divergence_percent;

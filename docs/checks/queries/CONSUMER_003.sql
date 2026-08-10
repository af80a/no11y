CREATE OR REPLACE MACRO audit.check_consumer_003(epoch_start, epoch_end) AS TABLE
  WITH per_consumer AS (
    SELECT
      c.pk,
      c.epoch,
      c.stream_pk,
      c.num_replicas,
      COUNT(DISTINCT c.peer_server_pk) FILTER (WHERE c.is_offline = true) AS offline_count
    FROM hx.consumers c
    WHERE c.epoch BETWEEN epoch_start AND epoch_end
      AND c.num_replicas > 1
    GROUP BY c.pk, c.epoch, c.stream_pk, c.num_replicas
  )
  SELECT
    'CONSUMER_003' AS code,
    ai.name || ' / ' || si.name || ' / ' || ci.name AS entity,
    p.pk AS entity_pk,
    p.offline_count AS offline_count,
    p.num_replicas AS num_replicas,
    -- Integer division: '/' on integers is float division in DuckDB, and the
    -- firing-detail path scans this column positionally into an int64.
    p.num_replicas // 2 + 1 AS quorum_needed,
    p.epoch
  FROM per_consumer p
  INNER JOIN hx.consumer_ident ci ON ci.pk = p.pk
  INNER JOIN hx.stream_ident si ON si.pk = p.stream_pk
  INNER JOIN hx.account_ident ai ON ai.pk = si.account_pk
  WHERE p.offline_count * 2 > p.num_replicas;

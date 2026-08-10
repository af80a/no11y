CREATE OR REPLACE MACRO audit.check_opt_balance_001(epoch_ts, range_start) AS TABLE
  WITH leader_counts AS (
    SELECT server_pk, count(*) AS leaders
    FROM (
      SELECT server_pk
      FROM hx.stream_replica_stats
      WHERE epoch = epoch_ts
        AND is_leader = true
      UNION ALL
      SELECT server_pk
      FROM hx.consumer_replica_stats
      WHERE epoch = epoch_ts
        AND is_leader = true
    )
    GROUP BY server_pk
  ),
  with_cluster AS (
    SELECT
      lc.server_pk,
      si.name AS server_name,
      si.cluster,
      lc.leaders
    FROM leader_counts lc
    INNER JOIN hx.server_ident si ON lc.server_pk = si.pk
    WHERE si.cluster IS NOT NULL AND si.cluster <> ''
  ),
  cluster_avg AS (
    SELECT
      cluster,
      avg(leaders) AS avg_leaders,
      count(*) AS server_count
    FROM with_cluster
    GROUP BY cluster
  )
  SELECT
    'OPT_BALANCE_001' AS code,
    CASE WHEN wc.cluster IS NOT NULL AND wc.cluster != '' THEN wc.cluster || ' / ' || wc.server_name ELSE wc.server_name END AS entity,
    wc.server_pk AS entity_pk,
    wc.leaders AS value,
    round(ca.avg_leaders, 1) AS avg_value,
    wc.cluster AS cluster_name,
    epoch_ts AS epoch
  FROM with_cluster wc
  INNER JOIN cluster_avg ca ON wc.cluster = ca.cluster
  WHERE ca.server_count >= 3
    AND wc.leaders > 1.5 * ca.avg_leaders;

CREATE OR REPLACE MACRO audit.check_opt_balance_004(epoch_ts, range_start) AS TABLE
  WITH replica_counts AS (
    SELECT server_pk, count(*) AS replicas
    FROM hx.stream_replica_stats
    WHERE epoch = epoch_ts
    GROUP BY server_pk
  ),
  with_cluster AS (
    SELECT
      rc.server_pk,
      si.name AS server_name,
      si.cluster,
      rc.replicas
    FROM replica_counts rc
    INNER JOIN hx.server_ident si ON rc.server_pk = si.pk
    WHERE si.cluster IS NOT NULL AND si.cluster <> ''
  ),
  cluster_avg AS (
    SELECT
      cluster,
      avg(replicas) AS avg_replicas,
      count(*) AS server_count
    FROM with_cluster
    GROUP BY cluster
  )
  SELECT
    'OPT_BALANCE_004' AS code,
    CASE WHEN wc.cluster IS NOT NULL AND wc.cluster != '' THEN wc.cluster || ' / ' || wc.server_name ELSE wc.server_name END AS entity,
    wc.server_pk AS entity_pk,
    wc.replicas AS value,
    round(ca.avg_replicas, 1) AS avg_value,
    wc.cluster AS cluster_name,
    epoch_ts AS epoch
  FROM with_cluster wc
  INNER JOIN cluster_avg ca ON wc.cluster = ca.cluster
  WHERE ca.server_count >= 3
    AND wc.replicas >= 10
    AND wc.replicas > 1.5 * ca.avg_replicas;

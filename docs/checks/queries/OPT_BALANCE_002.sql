CREATE OR REPLACE MACRO audit.check_opt_balance_002(epoch_ts, range_start) AS TABLE
  WITH server_conns AS (
    SELECT
      ss.server_pk,
      si.name AS server_name,
      si.cluster,
      ss.connections
    FROM hx.server_stats ss
    INNER JOIN hx.server_ident si ON ss.server_pk = si.pk
    WHERE ss.epoch = epoch_ts
      AND si.cluster IS NOT NULL AND si.cluster <> ''
  ),
  cluster_avg AS (
    SELECT
      cluster,
      avg(connections) AS avg_connections,
      count(*) AS server_count
    FROM server_conns
    GROUP BY cluster
  )
  SELECT
    'OPT_BALANCE_002' AS code,
    CASE WHEN sc.cluster IS NOT NULL AND sc.cluster != '' THEN sc.cluster || ' / ' || sc.server_name ELSE sc.server_name END AS entity,
    sc.server_pk AS entity_pk,
    sc.connections AS value,
    round(ca.avg_connections, 1) AS avg_value,
    sc.cluster AS cluster_name,
    epoch_ts AS epoch
  FROM server_conns sc
  INNER JOIN cluster_avg ca ON sc.cluster = ca.cluster
  WHERE ca.server_count >= 2
    AND sc.connections >= 100
    AND sc.connections > 2.0 * ca.avg_connections;

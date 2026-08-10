CREATE OR REPLACE MACRO audit.check_cluster_008(epoch_start, epoch_end) AS TABLE
  WITH server_gw_sets AS (
    SELECT
      gs.epoch,
      gi.server_pk,
      string_agg(DISTINCT NULLIF(lower(trim(rs.cluster)), ''), ',' ORDER BY NULLIF(lower(trim(rs.cluster)), '')) AS gw_set
    FROM hx.gateway_stats gs
    INNER JOIN hx.gateway_ident gi ON gi.pk = gs.gateway_pk
    INNER JOIN hx.server_ident rs ON gi.remote_server_pk = rs.pk
    WHERE gs.epoch BETWEEN epoch_start AND epoch_end
    GROUP BY gs.epoch, gi.server_pk
  ),
  server_clusters AS (
    SELECT s.epoch, s.server_pk, si.cluster, s.gw_set
    FROM server_gw_sets s
    INNER JOIN hx.server_ident si ON s.server_pk = si.pk
    WHERE si.cluster IS NOT NULL AND si.cluster <> ''
  ),
  cluster_sizes AS (
    SELECT epoch, cluster, count(*) AS server_count
    FROM server_clusters
    GROUP BY epoch, cluster
  ),
  set_counts AS (
    SELECT epoch, cluster, gw_set, count(*) AS cnt
    FROM server_clusters
    GROUP BY epoch, cluster, gw_set
  ),
  majority AS (
    SELECT ranked.epoch, ranked.cluster, ranked.gw_set AS majority_set
    FROM (
      SELECT epoch, cluster, gw_set, cnt,
        ROW_NUMBER() OVER (PARTITION BY epoch, cluster ORDER BY cnt DESC, gw_set) AS rn
      FROM set_counts
    ) ranked
    INNER JOIN cluster_sizes cs ON ranked.cluster = cs.cluster AND ranked.epoch = cs.epoch
    WHERE ranked.rn = 1
      AND cs.server_count >= 3
      AND ranked.cnt * 2 > cs.server_count
  )
  SELECT
    'CLUSTER_008' AS code,
    CASE WHEN si.cluster IS NOT NULL AND si.cluster != '' THEN si.cluster || ' / ' || si.name ELSE si.name END AS entity,
    si.pk AS entity_pk,
    sc.gw_set AS server_gateways,
    m.majority_set AS majority_gateways,
    sc.epoch
  FROM server_clusters sc
  INNER JOIN majority m ON sc.cluster = m.cluster AND sc.epoch = m.epoch
  INNER JOIN hx.server_ident si ON sc.server_pk = si.pk
  WHERE sc.gw_set != m.majority_set;

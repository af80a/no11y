CREATE OR REPLACE MACRO audit.check_cluster_005(epoch_start, epoch_end) AS TABLE
  WITH servers_dedup AS (
    -- one row per (pk, epoch); normalize cluster name
    SELECT i.pk, i.name, s.epoch, s.routes,
           NULLIF(trim(i.cluster), '') AS cluster
    FROM hx.server_stats s
    INNER JOIN hx.server_ident i ON i.pk = s.server_pk
    WHERE s.epoch BETWEEN epoch_start AND epoch_end
  ),
  cluster_sizes AS (
    SELECT epoch, cluster, count(DISTINCT pk) AS member_count
    FROM servers_dedup
    WHERE cluster IS NOT NULL
    GROUP BY epoch, cluster
  )
  SELECT
    'CLUSTER_005' AS code,
    s.name AS entity,
    s.pk AS entity_pk,
    s.routes,
    cs.member_count - 1 AS expected,
    cs.member_count,
    s.epoch
  FROM servers_dedup s
  INNER JOIN cluster_sizes cs ON s.cluster = cs.cluster AND s.epoch = cs.epoch
  WHERE cs.member_count > 1
    AND s.routes < cs.member_count - 1;

CREATE OR REPLACE MACRO audit.check_opt_sys_017(epoch_ts, range_start, p_min_leafs := 20) AS TABLE
  WITH leaf_agg AS (
    SELECT
      l.server_pk,
      COUNT(*) AS leaf_count,
      COUNT(*) FILTER (WHERE l.compression = 's2_auto') AS auto_count
    FROM hx.leafs l
    WHERE l.epoch = epoch_ts
    GROUP BY l.server_pk
  )
  SELECT
    'OPT_SYS_017' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    la.server_pk AS entity_pk,
    la.leaf_count::BIGINT AS leaf_count,
    la.auto_count::BIGINT AS auto_count,
    epoch_ts AS epoch
  FROM leaf_agg la
  INNER JOIN hx.server_ident srv ON la.server_pk = srv.pk
  WHERE la.leaf_count > p_min_leafs
    AND la.auto_count > 0;

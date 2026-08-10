CREATE OR REPLACE MACRO audit.check_opt_balance_008(epoch_ts, range_start, p_saturation_pct := 90.0, p_skew_pp := 30.0) AS TABLE
  WITH server_pct AS (
    SELECT
      ss.server_pk,
      si.name AS server_name,
      si.cluster,
      CASE WHEN ss.js_reserved_storage > 0
        THEN ss.js_storage * 100.0 / ss.js_reserved_storage
        ELSE 0 END AS pct
    FROM hx.server_stats ss
    INNER JOIN hx.server_ident si ON ss.server_pk = si.pk
    WHERE ss.epoch = epoch_ts
      AND si.cluster IS NOT NULL AND si.cluster <> ''
      AND ss.js_reserved_storage > 0
  ),
  cluster_range AS (
    SELECT
      cluster,
      MIN(pct) AS min_pct,
      MAX(pct) AS max_pct,
      COUNT(*) AS server_count
    FROM server_pct
    GROUP BY cluster
    HAVING COUNT(*) >= 2
  )
  SELECT
    'OPT_BALANCE_008' AS code,
    CASE WHEN sp.cluster IS NOT NULL AND sp.cluster != '' THEN sp.cluster || ' / ' || sp.server_name ELSE sp.server_name END AS entity,
    sp.server_pk AS entity_pk,
    round(sp.pct, 1) AS pct,
    round(cr.min_pct, 1) AS cluster_min_pct,
    round(cr.max_pct, 1) AS cluster_max_pct,
    sp.cluster AS cluster_name,
    epoch_ts AS epoch
  FROM server_pct sp
  INNER JOIN cluster_range cr ON sp.cluster = cr.cluster
  WHERE sp.pct >= p_saturation_pct
    AND (cr.max_pct - cr.min_pct) > p_skew_pp;

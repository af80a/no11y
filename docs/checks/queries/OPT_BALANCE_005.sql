CREATE OR REPLACE MACRO audit.check_opt_balance_005(epoch_ts, range_start) AS TABLE
  WITH server_js AS (
    SELECT
      ss.server_pk,
      si.name AS server_name,
      si.cluster,
      ss.js_storage
    FROM hx.server_stats ss
    INNER JOIN hx.server_ident si ON ss.server_pk = si.pk
    WHERE ss.epoch = epoch_ts
      AND si.cluster IS NOT NULL AND si.cluster <> ''
      AND ss.js_storage > 0
  ),
  cluster_avg AS (
    SELECT
      cluster,
      avg(js_storage) AS avg_storage,
      count(*) AS server_count
    FROM server_js
    GROUP BY cluster
  )
  SELECT
    'OPT_BALANCE_005' AS code,
    CASE WHEN sj.cluster IS NOT NULL AND sj.cluster != '' THEN sj.cluster || ' / ' || sj.server_name ELSE sj.server_name END AS entity,
    sj.server_pk AS entity_pk,
    round(sj.js_storage / 1073741824.0, 1) AS value,
    round(ca.avg_storage / 1073741824.0, 1) AS avg_value,
    sj.cluster AS cluster_name,
    epoch_ts AS epoch
  FROM server_js sj
  INNER JOIN cluster_avg ca ON sj.cluster = ca.cluster
  WHERE ca.server_count >= 2
    AND sj.js_storage >= 1073741824
    AND sj.js_storage > 2.0 * ca.avg_storage;

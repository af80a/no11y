CREATE OR REPLACE MACRO audit.check_opt_cost_005(epoch_ts, range_start, p_min_utilization_percent := 20.0) AS TABLE
  SELECT
    'OPT_COST_005' AS code,
    CASE WHEN s.cluster IS NOT NULL AND s.cluster != '' THEN s.cluster || ' / ' || s.name ELSE s.name END AS entity,
    s.pk AS entity_pk,
    round(s.js_storage * 100.0 / s.js_reserved_storage, 1) AS pct,
    round(s.js_reserved_storage / 1048576.0, 1) AS reserved_mib,
    epoch_ts AS epoch
  FROM hx.servers s
  WHERE s.epoch = epoch_ts
    AND s.js_reserved_storage > 0
    AND s.js_storage * 100.0 / s.js_reserved_storage < p_min_utilization_percent;

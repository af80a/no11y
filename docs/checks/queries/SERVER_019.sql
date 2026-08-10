CREATE OR REPLACE MACRO audit.check_server_019(epoch_start, epoch_end, p_warn_percent := 90.0, p_crit_percent := 95.0) AS TABLE
  SELECT
    'SERVER_019' AS code,
    CASE WHEN js_storage * 100.0 / js_max_store >= p_crit_percent THEN 2 ELSE 1 END AS severity,
    CASE WHEN cluster IS NOT NULL AND cluster != '' THEN cluster || ' / ' || name ELSE name END AS entity,
    pk AS entity_pk,
    round(js_storage * 100.0 / js_max_store, 1) AS pct,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND js_max_store > 0
    AND js_storage * 100.0 / js_max_store >= p_warn_percent;

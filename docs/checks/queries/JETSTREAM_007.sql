CREATE OR REPLACE MACRO audit.check_jetstream_007(epoch_start, epoch_end, p_crit_percent := 95.0) AS TABLE
  SELECT
    'JETSTREAM_007' AS code,
    CASE WHEN cluster IS NOT NULL AND cluster != '' THEN cluster || ' / ' || name ELSE name END AS entity,
    pk AS entity_pk,
    round(js_memory * 100.0 / js_reserved_memory, 1) AS pct,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND js_reserved_memory > 0
    AND js_memory * 100.0 / js_reserved_memory >= p_crit_percent;

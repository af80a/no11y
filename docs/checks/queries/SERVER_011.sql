CREATE OR REPLACE MACRO audit.check_server_011(epoch_start, epoch_end, p_warn_percent := 80.0) AS TABLE
  SELECT
    'SERVER_011' AS code,
    CASE WHEN cluster IS NOT NULL AND cluster != '' THEN cluster || ' / ' || name ELSE name END AS entity,
    pk AS entity_pk,
    connections AS current_val,
    max_connections AS max_val,
    round(connections * 100.0 / max_connections, 1) AS pct,
    'connections' AS unit,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND max_connections > 0
    AND connections * 100.0 / max_connections >= p_warn_percent;

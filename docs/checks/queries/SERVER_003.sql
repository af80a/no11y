CREATE OR REPLACE MACRO audit.check_server_003(epoch_start, epoch_end, p_cpu_percent := 90.0) AS TABLE
  SELECT
    'SERVER_003' AS code,
    CASE WHEN cluster IS NOT NULL AND cluster != '' THEN cluster || ' / ' || name ELSE name END AS entity,
    pk AS entity_pk,
    round(cpu, 1) AS cpu_percent,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND cores > 0
    AND cpu >= p_cpu_percent;

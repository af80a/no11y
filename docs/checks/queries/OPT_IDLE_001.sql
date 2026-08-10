CREATE OR REPLACE MACRO audit.check_opt_idle_001(epoch_ts, range_start, p_max_cpu_percent := 5.0, p_max_connections := 10) AS TABLE
  WITH server_agg AS (
    SELECT
      s.server_pk,
      MAX(s.cpu / i.cores) AS max_cpu,
      MAX(s.connections) AS max_conns,
      COUNT(*) AS num_epochs
    FROM hx.server_stats s
    INNER JOIN hx.server_ident i ON i.pk = s.server_pk
    WHERE s.epoch BETWEEN range_start AND epoch_ts
      AND i.cores > 0
    GROUP BY s.server_pk
    -- Liveness guard: a restart issues a new server pk and the old pk stops
    -- reporting. Require a sample at the current epoch so dead boot-generation
    -- pks aren't flagged as underutilized.
    -- MAX() wraps the constant macro param so the binder accepts it in HAVING.
    HAVING MAX(s.epoch) = MAX(epoch_ts)
  )
  SELECT
    'OPT_IDLE_001' AS code,
    CASE WHEN i.cluster IS NOT NULL AND i.cluster != '' THEN i.cluster || ' / ' || i.name ELSE i.name END AS entity,
    i.pk AS entity_pk,
    round(a.max_cpu, 1) AS max_cpu,
    a.max_conns AS max_conns,
    epoch_ts AS epoch
  FROM server_agg a
  INNER JOIN hx.server_ident i ON i.pk = a.server_pk
  WHERE a.max_cpu < p_max_cpu_percent
    AND a.max_conns < p_max_connections
    AND a.num_epochs >= 5;

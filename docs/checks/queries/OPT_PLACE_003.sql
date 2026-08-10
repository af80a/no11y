CREATE OR REPLACE MACRO audit.check_opt_place_003(epoch_ts, range_start, p_warn_percent := 30.0, p_min_total_bytes := 1048576) AS TABLE
  WITH window_stats AS (
    SELECT
      account_pk,
      server_pk,
      epoch,
      gateway_bytes_sent + gateway_bytes_recv AS gateway_bytes,
      bytes_sent + bytes_recv AS total_bytes
    FROM hx.account_stats
    WHERE epoch BETWEEN range_start AND epoch_ts
  ),
  per_server AS (
    SELECT
      account_pk,
      server_pk,
      greatest(arg_max(gateway_bytes, epoch) - arg_min(gateway_bytes, epoch), 0) AS gateway_delta,
      greatest(arg_max(total_bytes, epoch) - arg_min(total_bytes, epoch), 0) AS total_delta
    FROM window_stats
    GROUP BY account_pk, server_pk
  ),
  agg AS (
    SELECT
      account_pk,
      sum(least(gateway_delta, total_delta)) AS gateway_bytes,
      sum(total_delta) AS total_bytes
    FROM per_server
    GROUP BY account_pk
  )
  SELECT
    'OPT_PLACE_003' AS code,
    ai.name AS entity,
    ai.pk AS entity_pk,
    round(a.gateway_bytes * 100.0 / a.total_bytes, 1) AS gateway_pct,
    a.gateway_bytes AS gateway_bytes,
    a.total_bytes AS total_bytes,
    epoch_ts AS epoch
  FROM agg a
  INNER JOIN hx.account_ident ai ON a.account_pk = ai.pk
  WHERE a.total_bytes >= p_min_total_bytes
    AND a.gateway_bytes * 100.0 / a.total_bytes >= p_warn_percent;

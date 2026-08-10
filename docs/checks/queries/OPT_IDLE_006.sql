CREATE OR REPLACE MACRO audit.check_opt_idle_006(epoch_ts, range_start) AS TABLE
  WITH active_user_pks AS (
    SELECT DISTINCT ci.user_pk
    FROM hx.conn_ident ci
    INNER JOIN hx.conn_stats cs ON cs.conn_pk = ci.pk
    WHERE cs.epoch = epoch_ts
      AND cs.stop_time = '0001-01-01T00:00:00Z'
      AND ci.user_pk IS NOT NULL
  ),
  latest_opts AS (
    SELECT DISTINCT ON (account_pk) account_pk, is_system
    FROM hx.account_opts
    WHERE epoch <= epoch_ts
    ORDER BY account_pk, epoch DESC
  )
  SELECT
    'OPT_IDLE_006' AS code,
    ai.name || ' / ' || u.name AS entity,
    u.pk AS entity_pk,
    epoch_ts AS epoch
  FROM hx.user_ident u
  INNER JOIN hx.account_ident ai ON ai.pk = u.account_pk
  INNER JOIN latest_opts o ON o.account_pk = u.account_pk
  LEFT JOIN active_user_pks a ON a.user_pk = u.pk
  WHERE o.is_system = false
    AND a.user_pk IS NULL;

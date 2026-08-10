CREATE OR REPLACE MACRO audit.check_user_001(epoch_start, epoch_end) AS TABLE
  WITH active_user_conns AS (
    SELECT ci.user_pk, COUNT(*) AS conn_count
    FROM hx.conn_ident ci
    INNER JOIN hx.conn_stats cs ON cs.conn_pk = ci.pk
    WHERE cs.epoch BETWEEN epoch_start AND epoch_end
      AND cs.stop_time = '0001-01-01T00:00:00Z'
      AND ci.user_pk IS NOT NULL
    GROUP BY ci.user_pk
  )
  SELECT
    'USER_001' AS code,
    ai.name || ' / ' || u.name AS entity,
    u.pk AS entity_pk,
    ac.conn_count AS conn_count,
    u.epoch
  FROM hx.users u
  INNER JOIN hx.account_ident ai ON u.account_pk = ai.pk
  INNER JOIN active_user_conns ac ON ac.user_pk = u.pk
  WHERE u.epoch BETWEEN epoch_start AND epoch_end
    AND u.bearer = true;

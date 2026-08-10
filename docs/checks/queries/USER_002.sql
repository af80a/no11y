CREATE OR REPLACE MACRO audit.check_user_002(epoch_start, epoch_end, p_max_connections := 100) AS TABLE
  WITH user_conn_counts AS (
    SELECT ci.user_pk, COUNT(*) AS conn_count
    FROM hx.conn_ident ci
    INNER JOIN hx.conn_stats cs ON cs.conn_pk = ci.pk
    WHERE cs.epoch BETWEEN epoch_start AND epoch_end
      AND cs.stop_time = '0001-01-01T00:00:00Z'
      AND ci.user_pk IS NOT NULL
    GROUP BY ci.user_pk
  )
  SELECT
    'USER_002' AS code,
    ai.name || ' / ' || u.name AS entity,
    u.pk AS entity_pk,
    uc.conn_count AS conn_count,
    u.epoch
  FROM hx.users u
  INNER JOIN hx.account_ident ai ON u.account_pk = ai.pk
  INNER JOIN user_conn_counts uc ON uc.user_pk = u.pk
  WHERE u.epoch BETWEEN epoch_start AND epoch_end
    AND uc.conn_count > p_max_connections;

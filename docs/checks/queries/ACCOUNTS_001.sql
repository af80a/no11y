CREATE OR REPLACE MACRO audit.check_accounts_001(epoch_start, epoch_end, p_warn_percent := 90.0) AS TABLE
  WITH worst AS (
    SELECT
      a.epoch,
      a.name,
      a.pk,
      a.server_pk,
      a.conns,
      a.max_conn
    FROM hx.accounts a
    WHERE a.epoch BETWEEN epoch_start AND epoch_end
      AND a.max_conn > 0
      AND a.max_conn != -1
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY a.epoch, a.name
      ORDER BY a.conns * 100.0 / a.max_conn DESC
    ) = 1
  )
  SELECT
    'ACCOUNTS_001' AS code,
    w.name AS entity,
    w.pk AS entity_pk,
    w.conns::BIGINT AS current_val,
    w.max_conn AS max_val,
    round(w.conns * 100.0 / w.max_conn, 1) AS pct,
    'connections' AS unit,
    si.name AS server_name,
    w.epoch
  FROM worst w
  INNER JOIN hx.server_ident si ON w.server_pk = si.pk
  WHERE w.conns * 100.0 / w.max_conn >= p_warn_percent;

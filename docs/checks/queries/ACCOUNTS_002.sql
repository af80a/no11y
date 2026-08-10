CREATE OR REPLACE MACRO audit.check_accounts_002(epoch_start, epoch_end) AS TABLE
  WITH prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.account_stats WHERE epoch < epoch_start
  ),
  range_epochs AS (
    SELECT epoch FROM prev_epoch WHERE epoch IS NOT NULL
    UNION
    SELECT DISTINCT epoch FROM hx.account_stats WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  epoch_pairs AS (
    SELECT epoch, LAG(epoch) OVER (ORDER BY epoch) AS prev_epoch FROM range_epochs
  ),
  deltas AS (
    SELECT ep.epoch, c.account_pk, GREATEST(c.slow_consumers - p.slow_consumers, 0) AS delta
    FROM epoch_pairs ep
    INNER JOIN hx.account_stats c ON c.epoch = ep.epoch
    INNER JOIN hx.account_stats p ON c.account_pk = p.account_pk AND c.server_pk = p.server_pk AND p.epoch = ep.prev_epoch
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  ),
  agg AS (
    SELECT epoch, account_pk, SUM(delta) AS total_slow
    FROM deltas
    GROUP BY epoch, account_pk
  )
  SELECT
    'ACCOUNTS_002' AS code,
    i.name AS entity,
    i.pk AS entity_pk,
    a.total_slow,
    a.epoch
  FROM agg a
  INNER JOIN hx.account_ident i ON a.account_pk = i.pk
  WHERE a.total_slow > 0;

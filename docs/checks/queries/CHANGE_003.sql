CREATE OR REPLACE MACRO audit.check_change_003(epoch_start, epoch_end) AS TABLE
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
  -- Distinct accounts present at each epoch.
  current_accounts AS (
    SELECT DISTINCT ep.epoch, s.account_pk
    FROM epoch_pairs ep
    INNER JOIN hx.account_stats s ON s.epoch = ep.epoch
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  ),
  prev_accounts AS (
    SELECT DISTINCT ep.epoch, s.account_pk
    FROM epoch_pairs ep
    INNER JOIN hx.account_stats s ON s.epoch = ep.prev_epoch
    WHERE ep.prev_epoch IS NOT NULL
  )
  -- Accounts added (present now, absent before).
  SELECT
    'CHANGE_003' AS code,
    i.name AS entity,
    c.account_pk AS entity_pk,
    'added' AS change_type,
    c.epoch
  FROM current_accounts c
  LEFT JOIN prev_accounts p ON c.account_pk = p.account_pk AND c.epoch = p.epoch
  INNER JOIN hx.account_ident i ON i.pk = c.account_pk
  WHERE p.account_pk IS NULL
  UNION ALL
  -- Accounts removed (present before, absent now).
  SELECT
    'CHANGE_003' AS code,
    i.name AS entity,
    p.account_pk AS entity_pk,
    'removed' AS change_type,
    p.epoch
  FROM prev_accounts p
  LEFT JOIN current_accounts c ON p.account_pk = c.account_pk AND p.epoch = c.epoch
  INNER JOIN hx.account_ident i ON i.pk = p.account_pk
  WHERE c.account_pk IS NULL;

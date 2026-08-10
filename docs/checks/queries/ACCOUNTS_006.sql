CREATE OR REPLACE MACRO audit.check_accounts_006(epoch_start, epoch_end, p_warn_percent := 90.0) AS TABLE
  WITH sub_counts AS (
    SELECT epoch, account_pk, SUM(subs) AS total_subs
    FROM hx.account_stats
    WHERE epoch BETWEEN epoch_start AND epoch_end
    GROUP BY epoch, account_pk
  ),
  limits AS (
    SELECT
      epoch,
      a.name,
      max(a.pk) AS pk,
      max(a.pk) AS account_pk,
      max(a.max_subs) AS max_subs
    FROM hx.accounts a
    WHERE a.epoch BETWEEN epoch_start AND epoch_end
    GROUP BY epoch, a.name
  )
  SELECT
    'ACCOUNTS_006' AS code,
    l.name AS entity,
    l.pk AS entity_pk,
    COALESCE(sc.total_subs, 0)::BIGINT AS current_val,
    l.max_subs AS max_val,
    round(COALESCE(sc.total_subs, 0) * 100.0 / l.max_subs, 1) AS pct,
    'subscriptions' AS unit,
    l.epoch
  FROM limits l
  LEFT JOIN sub_counts sc ON l.account_pk = sc.account_pk AND l.epoch = sc.epoch
  WHERE l.max_subs > 0
    AND l.max_subs != -1
    AND COALESCE(sc.total_subs, 0) * 100.0 / l.max_subs >= p_warn_percent;

CREATE OR REPLACE MACRO audit.check_opt_balance_003(epoch_ts, range_start) AS TABLE
  WITH server_subs AS (
    SELECT
      ss.server_pk,
      si.name AS server_name,
      si.cluster,
      ss.subscriptions
    FROM hx.server_stats ss
    INNER JOIN hx.server_ident si ON ss.server_pk = si.pk
    WHERE ss.epoch = epoch_ts
      AND si.cluster IS NOT NULL AND si.cluster <> ''
  ),
  cluster_avg AS (
    SELECT
      cluster,
      avg(subscriptions) AS avg_subscriptions,
      count(*) AS server_count
    FROM server_subs
    GROUP BY cluster
  )
  SELECT
    'OPT_BALANCE_003' AS code,
    CASE WHEN ss.cluster IS NOT NULL AND ss.cluster != '' THEN ss.cluster || ' / ' || ss.server_name ELSE ss.server_name END AS entity,
    ss.server_pk AS entity_pk,
    ss.subscriptions AS value,
    round(ca.avg_subscriptions, 1) AS avg_value,
    ss.cluster AS cluster_name,
    epoch_ts AS epoch
  FROM server_subs ss
  INNER JOIN cluster_avg ca ON ss.cluster = ca.cluster
  WHERE ca.server_count >= 2
    AND ss.subscriptions >= 100
    AND ss.subscriptions > 2.0 * ca.avg_subscriptions;

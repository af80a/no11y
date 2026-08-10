CREATE OR REPLACE MACRO audit.check_opt_place_004(epoch_ts, range_start) AS TABLE
  SELECT
    'OPT_PLACE_004' AS code,
    gas.account || ' -> ' || rsrv.cluster AS entity,
    min(gi.server_pk) AS entity_pk,
    gas.account,
    rsrv.cluster AS remote_cluster,
    max(gas.no_interest_count) AS no_interest_count,
    max(gas.interest_only_threshold) AS interest_only_threshold,
    max(gas.total_subscriptions) AS total_subscriptions,
    count(DISTINCT gi.server_pk) AS local_server_count,
    epoch_ts AS epoch
  FROM hx.gateway_account_stats gas
  INNER JOIN hx.gateway_ident gi ON gas.gateway_pk = gi.pk
  INNER JOIN hx.server_ident rsrv ON gi.remote_server_pk = rsrv.pk
  WHERE gas.epoch = epoch_ts
    AND gas.interest_mode = 'Optimistic'
  GROUP BY gas.account, rsrv.cluster;

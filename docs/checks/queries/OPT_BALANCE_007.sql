CREATE OR REPLACE MACRO audit.check_opt_balance_007(epoch_ts, range_start, p_consumer_leader_pct := 50.0) AS TABLE
  WITH stream_leaders AS (
    SELECT pk, server_pk, account_pk, name, is_kv, is_object
    FROM hx.streams
    WHERE epoch = epoch_ts
      AND is_leader = true
  ),
  consumer_leader_counts AS (
    SELECT
      c.stream_pk,
      c.server_pk,
      COUNT(*) AS leaders_on_server
    FROM hx.consumers c
    WHERE c.epoch = epoch_ts
      AND c.is_leader = true
      AND c.num_replicas > 1
    GROUP BY c.stream_pk, c.server_pk
  ),
  consumer_totals AS (
    SELECT stream_pk, SUM(leaders_on_server) AS total_leaders
    FROM consumer_leader_counts
    GROUP BY stream_pk
    HAVING SUM(leaders_on_server) >= 3
  )
  SELECT
    'OPT_BALANCE_007' AS code,
    ai.name || ' / ' || sl.name AS entity,
    sl.pk AS entity_pk,
    CASE WHEN sl.is_kv THEN 'kvstore' WHEN sl.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    clc.leaders_on_server::BIGINT AS consumer_leaders_on_server,
    ct.total_leaders::BIGINT AS total_consumer_leaders,
    round(clc.leaders_on_server * 100.0 / ct.total_leaders, 1) AS pct,
    epoch_ts AS epoch
  FROM stream_leaders sl
  INNER JOIN consumer_leader_counts clc ON sl.pk = clc.stream_pk AND sl.server_pk = clc.server_pk
  INNER JOIN consumer_totals ct ON sl.pk = ct.stream_pk
  INNER JOIN hx.account_ident ai ON sl.account_pk = ai.pk
  WHERE clc.leaders_on_server * 100.0 / ct.total_leaders > p_consumer_leader_pct;

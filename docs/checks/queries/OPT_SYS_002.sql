CREATE OR REPLACE MACRO audit.check_opt_sys_002(epoch_ts, range_start, p_redelivery_percent := 10.0) AS TABLE
  WITH delivered_window AS (
    SELECT
      consumer_pk,
      arg_max(delivered_consumer_seq, epoch) - arg_min(delivered_consumer_seq, epoch) AS delivered_delta
    FROM hx.consumer_replica_stats
    WHERE epoch >= range_start
      AND epoch <= epoch_ts
      AND is_leader = true
    GROUP BY consumer_pk
  )
  SELECT
    'OPT_SYS_002' AS code,
    ai.name || ' / ' || si.name || ' / ' || c.name AS entity,
    c.pk AS entity_pk,
    round(c.num_redelivered * 100.0 / dw.delivered_delta, 1) AS redelivery_pct,
    c.num_redelivered,
    dw.delivered_delta AS delivered,
    epoch_ts AS epoch
  FROM hx.consumers c
  INNER JOIN delivered_window dw ON c.pk = dw.consumer_pk
  INNER JOIN hx.stream_ident si ON c.stream_pk = si.pk
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
  WHERE c.epoch = epoch_ts
    AND c.is_leader = true
    -- A minimum in-window delivery volume keeps a 1-message window from
    -- reading as a 100% redelivery rate.
    AND dw.delivered_delta >= 100
    AND c.num_redelivered * 100.0 / dw.delivered_delta > p_redelivery_percent;

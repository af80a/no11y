CREATE OR REPLACE MACRO audit.check_opt_sys_015(epoch_ts, range_start, p_warn_multiplier := 2.0, p_crit_multiplier := 5.0, p_warn_abs := 100000, p_crit_abs := 1000000) AS TABLE
  WITH candidates AS (
    SELECT
      c.pk,
      c.name,
      c.stream_pk,
      (c.delivered_stream_seq - c.ack_floor_stream_seq) AS gap,
      c.max_ack_pending
    FROM hx.consumers c
    WHERE c.epoch = epoch_ts
      AND c.is_leader = true
      AND c.delivered_stream_seq > c.ack_floor_stream_seq
      AND ((c.max_ack_pending > 0 AND c.max_ack_pending != -1
            AND (c.delivered_stream_seq - c.ack_floor_stream_seq) > p_warn_multiplier * c.max_ack_pending)
        OR (c.delivered_stream_seq - c.ack_floor_stream_seq) > p_warn_abs)
  )
  SELECT
    'OPT_SYS_015' AS code,
    CASE
      WHEN cd.gap > p_crit_abs THEN 1
      WHEN cd.max_ack_pending > 0 AND cd.max_ack_pending != -1
        AND cd.gap > p_crit_multiplier * cd.max_ack_pending THEN 1
      ELSE 0
    END AS severity,
    ai.name || ' / ' || si.name || ' / ' || cd.name AS entity,
    cd.pk AS entity_pk,
    cd.gap::BIGINT AS gap,
    cd.max_ack_pending::BIGINT AS max_ack_pending,
    CASE WHEN cd.max_ack_pending > 0 AND cd.max_ack_pending != -1
      THEN round(cd.gap * 1.0 / cd.max_ack_pending, 1)
      ELSE NULL END AS gap_ratio,
    epoch_ts AS epoch
  FROM candidates cd
  INNER JOIN hx.stream_ident si ON cd.stream_pk = si.pk
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk;

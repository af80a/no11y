CREATE OR REPLACE MACRO audit.check_opt_sys_003(epoch_ts, range_start, p_warn_percent := 80.0) AS TABLE
  SELECT
    'OPT_SYS_003' AS code,
    ai.name || ' / ' || si.name || ' / ' || c.name AS entity,
    c.pk AS entity_pk,
    c.num_ack_pending AS current_val,
    c.max_ack_pending AS max_val,
    round(c.num_ack_pending * 100.0 / c.max_ack_pending, 1) AS pct,
    'ack pending' AS unit,
    epoch_ts AS epoch
  FROM hx.consumers c
  INNER JOIN hx.stream_ident si ON c.stream_pk = si.pk
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
  WHERE c.epoch = epoch_ts
    AND c.is_leader = true
    AND c.max_ack_pending > 0
    AND c.max_ack_pending != -1
    AND c.num_ack_pending * 100.0 / c.max_ack_pending >= p_warn_percent;

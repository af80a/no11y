CREATE OR REPLACE MACRO audit.check_opt_sys_004(epoch_ts, range_start) AS TABLE
  SELECT
    'OPT_SYS_004' AS code,
    ai.name || ' / ' || si.name || ' / ' || c.name AS entity,
    c.pk AS entity_pk,
    c.deliver_subject,
    epoch_ts AS epoch
  FROM hx.consumers c
  INNER JOIN hx.stream_ident si ON c.stream_pk = si.pk
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
  WHERE c.epoch = epoch_ts
    AND c.is_leader = true
    AND c.deliver_subject != ''
    AND c.push_bound = false;

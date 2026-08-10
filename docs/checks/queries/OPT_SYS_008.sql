CREATE OR REPLACE MACRO audit.check_opt_sys_008(epoch_ts, range_start) AS TABLE
  WITH active_accounts AS (
    SELECT DISTINCT account_pk
    FROM hx.account_stats
    WHERE epoch = epoch_ts
  )
  SELECT
    'OPT_SYS_008' AS code,
    ai.name AS entity,
    ai.pk AS entity_pk,
    epoch_ts AS epoch
  FROM active_accounts aa
  INNER JOIN hx.account_ident ai ON aa.account_pk = ai.pk
  INNER JOIN hx.account_opts ao ON ai.pk = ao.account_pk
    AND ao.epoch = (SELECT MAX(epoch) FROM hx.account_opts
                    WHERE account_pk = ai.pk AND epoch <= epoch_ts)
  WHERE ao.jetstream_enabled = true
    AND ao.js_mem_storage = -1
    AND ao.js_disk_storage = -1
    AND ao.is_system = false;

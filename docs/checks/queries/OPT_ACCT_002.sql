CREATE OR REPLACE MACRO audit.check_opt_acct_002(epoch_ts, range_start, p_max_size_kib := 64.0) AS TABLE
  SELECT
    'OPT_ACCT_002' AS code,
    ai.name AS entity,
    ai.pk AS entity_pk,
    round(length(ao.claims) / 1024.0, 1) AS claims_size_kib,
    epoch_ts AS epoch
  FROM hx.account_opts ao
  INNER JOIN hx.account_ident ai ON ao.account_pk = ai.pk
  WHERE ao.epoch = (SELECT MAX(epoch) FROM hx.account_opts WHERE account_pk = ao.account_pk AND epoch <= epoch_ts)
    AND ao.claims IS NOT NULL
    AND ao.claims != ''
    AND length(ao.claims) > p_max_size_kib * 1024;

CREATE OR REPLACE MACRO audit.check_opt_acct_001(epoch_ts, range_start, p_warn_percent := 85.0) AS TABLE
  WITH stream_reservations AS (
    SELECT
      si.account_pk,
      SUM(CASE WHEN so.max_bytes > 0 AND so.max_bytes != -1
          THEN so.max_bytes * so.num_replicas ELSE 0 END) AS reserved_bytes
    FROM hx.stream_opts so
    INNER JOIN hx.stream_ident si ON so.stream_pk = si.pk
    INNER JOIN hx.stream_replica_stats srs ON srs.stream_pk = si.pk AND srs.epoch = epoch_ts
    WHERE so.epoch = (SELECT MAX(epoch) FROM hx.stream_opts
                      WHERE stream_pk = so.stream_pk AND epoch <= epoch_ts)
      AND srs.is_leader = true
    GROUP BY si.account_pk
  )
  SELECT
    'OPT_ACCT_001' AS code,
    ai.name AS entity,
    ai.pk AS entity_pk,
    sr.reserved_bytes::BIGINT AS reserved_bytes,
    ao.js_disk_storage::BIGINT AS quota_bytes,
    round(sr.reserved_bytes * 100.0 / ao.js_disk_storage, 1) AS pct,
    epoch_ts AS epoch
  FROM stream_reservations sr
  INNER JOIN hx.account_ident ai ON sr.account_pk = ai.pk
  INNER JOIN hx.account_opts ao ON ao.account_pk = ai.pk
    AND ao.epoch = (SELECT MAX(epoch) FROM hx.account_opts
                    WHERE account_pk = ai.pk AND epoch <= epoch_ts)
  WHERE ao.js_disk_storage > 0
    AND ao.js_disk_storage != -1
    AND sr.reserved_bytes * 100.0 / ao.js_disk_storage > p_warn_percent;

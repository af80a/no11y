CREATE OR REPLACE MACRO audit.check_jetstream_024(epoch_start, epoch_end) AS TABLE
  WITH thresholds AS (
    SELECT
      s.pk,
      s.epoch,
      ai.name AS account_name,
      s.name AS stream_name,
      s.is_kv,
      s.is_object,
      s.msgs,
      CAST(json_extract_string(s.metadata, '$."io.nats.monitor.msgs-warn"') AS BIGINT) AS warn_val,
      CAST(json_extract_string(s.metadata, '$."io.nats.monitor.msgs-critical"') AS BIGINT) AS crit_val
    FROM hx.streams s
    INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
    WHERE s.epoch BETWEEN epoch_start AND epoch_end
      AND s.is_leader = true
      AND json_extract_string(s.metadata, '$."io.nats.monitor.enabled"') = 'true'
      AND (json_extract_string(s.metadata, '$."io.nats.monitor.msgs-warn"') IS NOT NULL
           OR json_extract_string(s.metadata, '$."io.nats.monitor.msgs-critical"') IS NOT NULL)
  )
  SELECT
    'JETSTREAM_024' AS code,
    CASE
      WHEN crit_val IS NOT NULL AND warn_val IS NOT NULL AND crit_val > warn_val AND msgs > crit_val THEN 2
      WHEN crit_val IS NOT NULL AND warn_val IS NOT NULL AND crit_val < warn_val AND msgs < crit_val THEN 2
      WHEN crit_val IS NOT NULL AND warn_val IS NULL AND msgs > crit_val THEN 2
      ELSE 1
    END AS severity,
    account_name || ' / ' || stream_name AS entity,
    pk AS entity_pk,
    CASE WHEN is_kv THEN 'kvstore' WHEN is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    msgs::BIGINT AS current_val,
    warn_val AS warn_threshold,
    crit_val AS crit_threshold,
    epoch
  FROM thresholds
  WHERE
    (crit_val IS NOT NULL AND warn_val IS NOT NULL AND crit_val > warn_val AND (msgs > warn_val OR msgs > crit_val))
    OR (crit_val IS NOT NULL AND warn_val IS NOT NULL AND crit_val < warn_val AND (msgs < warn_val OR msgs < crit_val))
    OR (warn_val IS NOT NULL AND crit_val IS NULL AND msgs > warn_val)
    OR (crit_val IS NOT NULL AND warn_val IS NULL AND msgs > crit_val);

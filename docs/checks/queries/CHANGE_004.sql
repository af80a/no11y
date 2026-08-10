CREATE OR REPLACE MACRO audit.check_change_004(epoch_start, epoch_end) AS TABLE
  WITH prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.stream_opts WHERE epoch < epoch_start
  ),
  range_epochs AS (
    SELECT epoch FROM prev_epoch WHERE epoch IS NOT NULL
    UNION
    SELECT DISTINCT epoch FROM hx.stream_opts WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  epoch_pairs AS (
    SELECT epoch, LAG(epoch) OVER (ORDER BY epoch) AS prev_epoch FROM range_epochs
  ),
  current_opts AS (
    SELECT ep.epoch, o.stream_pk, o.num_replicas, o.retention_policy,
           o.max_msgs, o.max_bytes, o.max_age, o.max_consumers,
           o.is_kv, o.is_object
    FROM epoch_pairs ep
    INNER JOIN hx.stream_opts o ON o.epoch = ep.epoch
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  ),
  prev_opts AS (
    SELECT ep.epoch, o.stream_pk, o.num_replicas, o.retention_policy,
           o.max_msgs, o.max_bytes, o.max_age, o.max_consumers
    FROM epoch_pairs ep
    INNER JOIN hx.stream_opts o ON o.epoch = ep.prev_epoch
    WHERE ep.prev_epoch IS NOT NULL
  ),
  changes AS (
    SELECT
      c.epoch,
      c.stream_pk,
      c.is_kv,
      c.is_object,
      -- Build a comma-separated list of changed fields.
      list_filter([
        CASE WHEN c.num_replicas != p.num_replicas
          THEN 'num_replicas: ' || CAST(p.num_replicas AS VARCHAR) || ' â ' || CAST(c.num_replicas AS VARCHAR) END,
        CASE WHEN c.retention_policy != p.retention_policy
          THEN 'retention_policy: ' || p.retention_policy || ' â ' || c.retention_policy END,
        CASE WHEN c.max_msgs != p.max_msgs
          THEN 'max_msgs: ' || CAST(p.max_msgs AS VARCHAR) || ' â ' || CAST(c.max_msgs AS VARCHAR) END,
        CASE WHEN c.max_bytes != p.max_bytes
          THEN 'max_bytes: ' || CAST(p.max_bytes AS VARCHAR) || ' â ' || CAST(c.max_bytes AS VARCHAR) END,
        CASE WHEN c.max_age != p.max_age
          THEN 'max_age: ' || CAST(p.max_age AS VARCHAR) || ' â ' || CAST(c.max_age AS VARCHAR) END,
        CASE WHEN c.max_consumers != p.max_consumers
          THEN 'max_consumers: ' || CAST(p.max_consumers AS VARCHAR) || ' â ' || CAST(c.max_consumers AS VARCHAR) END
      ], x -> x IS NOT NULL) AS changed_fields
    FROM current_opts c
    INNER JOIN prev_opts p ON c.stream_pk = p.stream_pk AND c.epoch = p.epoch
    WHERE c.num_replicas != p.num_replicas
       OR c.retention_policy != p.retention_policy
       OR c.max_msgs != p.max_msgs
       OR c.max_bytes != p.max_bytes
       OR c.max_age != p.max_age
       OR c.max_consumers != p.max_consumers
  )
  SELECT
    'CHANGE_004' AS code,
    ai.name || ' / ' || si.name AS entity,
    ch.stream_pk AS entity_pk,
    CASE WHEN ch.is_kv THEN 'kvstore' WHEN ch.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    array_to_string(changed_fields, ', ') AS changed_fields,
    ch.epoch
  FROM changes ch
  INNER JOIN hx.stream_ident si ON si.pk = ch.stream_pk
  INNER JOIN hx.account_ident ai ON ai.pk = si.account_pk;

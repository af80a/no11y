CREATE OR REPLACE MACRO audit.check_jetstream_006(epoch_start, epoch_end, p_max_delta := 5000) AS TABLE
  WITH prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.consumer_replica_stats WHERE epoch < epoch_start
  ),
  range_epochs AS (
    SELECT epoch FROM prev_epoch WHERE epoch IS NOT NULL
    UNION
    SELECT DISTINCT epoch FROM hx.consumer_replica_stats WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  epoch_pairs AS (
    SELECT epoch, LAG(epoch) OVER (ORDER BY epoch) AS prev_epoch FROM range_epochs
  ),
  counts AS (
    SELECT ep.epoch,
           (SELECT count(*) FROM hx.consumer_replica_stats WHERE epoch = ep.epoch AND is_leader = true) AS current_count,
           (SELECT count(*) FROM hx.consumer_replica_stats WHERE epoch = ep.prev_epoch AND is_leader = true) AS prev_count
    FROM epoch_pairs ep
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  )
  SELECT
    'JETSTREAM_006' AS code,
    'system' AS entity,
    0 AS entity_pk,
    prev_count,
    current_count,
    epoch
  FROM counts
  WHERE abs(current_count - prev_count) > p_max_delta;

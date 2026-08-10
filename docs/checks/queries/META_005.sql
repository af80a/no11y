CREATE OR REPLACE MACRO audit.check_meta_005(epoch_start, epoch_end, p_max_assets := 5000) AS TABLE
  WITH range_epochs AS (
    SELECT DISTINCT epoch FROM hx.stream_replica_stats WHERE epoch BETWEEN epoch_start AND epoch_end
    UNION
    SELECT DISTINCT epoch FROM hx.consumer_replica_stats WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  asset_count AS (
    SELECT re.epoch,
      (SELECT COUNT(*) FROM hx.stream_replica_stats WHERE epoch = re.epoch) +
      (SELECT COUNT(*) FROM hx.consumer_replica_stats WHERE epoch = re.epoch) AS total_replicas
    FROM range_epochs re
  )
  SELECT
    'META_005' AS code,
    'meta cluster' AS entity,
    0 AS entity_pk,
    total_replicas,
    epoch
  FROM asset_count
  WHERE total_replicas > p_max_assets;

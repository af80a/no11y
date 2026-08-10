CREATE OR REPLACE MACRO audit.check_cluster_003(epoch_start, epoch_end, p_warn_count := 1000) AS TABLE
  SELECT
    'CLUSTER_003' AS code,
    name AS entity,
    pk AS entity_pk,
    ha_assets,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND ha_assets >= p_warn_count;

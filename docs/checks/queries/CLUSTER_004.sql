CREATE OR REPLACE MACRO audit.check_cluster_004(epoch_start, epoch_end) AS TABLE
  SELECT
    'CLUSTER_004' AS code,
    name AS entity,
    pk AS entity_pk,
    cluster AS cluster_name,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND cluster IS NOT NULL
    AND regexp_matches(cluster, '\s');

CREATE OR REPLACE MACRO audit.check_cluster_001(epoch_start, epoch_end, p_multiplier := 1.5) AS TABLE
  WITH cluster_avg AS (
    SELECT epoch, cluster, avg(memory) AS avg_mem
    FROM hx.servers
    WHERE epoch BETWEEN epoch_start AND epoch_end
      AND cluster IS NOT NULL
    GROUP BY epoch, cluster
  )
  SELECT
    'CLUSTER_001' AS code,
    s.name AS entity,
    s.pk AS entity_pk,
    round(s.memory * 1.0 / ca.avg_mem, 2) AS memory_ratio,
    s.cluster AS cluster_name,
    s.epoch
  FROM hx.servers s
  INNER JOIN cluster_avg ca ON s.cluster = ca.cluster AND s.epoch = ca.epoch
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND ca.avg_mem > 0
    AND s.memory * 1.0 / ca.avg_mem > p_multiplier;

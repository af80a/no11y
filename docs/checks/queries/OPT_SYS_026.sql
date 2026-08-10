CREATE OR REPLACE MACRO audit.check_opt_sys_026(epoch_ts, range_start) AS TABLE
  WITH stream_expected AS (
    SELECT raft_group, num_replicas
    FROM (
      SELECT so.raft_group, so.num_replicas,
        ROW_NUMBER() OVER (PARTITION BY so.raft_group ORDER BY so.epoch DESC) AS rn
      FROM hx.stream_opts so
      WHERE so.raft_group != '' AND so.epoch <= epoch_ts
    ) WHERE rn = 1
  ),
  latest_consumer_per_group AS (
    SELECT consumer_pk, raft_group, num_replicas
    FROM (
      SELECT co.consumer_pk, co.raft_group, co.num_replicas,
        ROW_NUMBER() OVER (PARTITION BY co.raft_group ORDER BY co.epoch DESC) AS rn
      FROM hx.consumer_opts co
      WHERE co.raft_group != '' AND co.epoch <= epoch_ts
    ) WHERE rn = 1
  ),
  consumer_expected AS (
    SELECT
      co.raft_group,
      CASE WHEN co.num_replicas = 0
           THEN (SELECT so.num_replicas FROM hx.stream_opts so
                 WHERE so.stream_pk = ci.stream_pk AND so.epoch <= epoch_ts
                 ORDER BY so.epoch DESC LIMIT 1)
           ELSE co.num_replicas
      END AS num_replicas
    FROM latest_consumer_per_group co
    INNER JOIN hx.consumer_ident ci ON ci.pk = co.consumer_pk
  ),
  expected AS (
    SELECT raft_group, num_replicas FROM stream_expected
    UNION ALL
    SELECT raft_group, num_replicas FROM consumer_expected
  )
  SELECT
    'OPT_SYS_026' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END || ' / ' || rg.group_name AS entity,
    rg.server_pk AS entity_pk,
    rg.group_name,
    rg.size::BIGINT AS observed_size,
    e.num_replicas::BIGINT AS expected_replicas,
    epoch_ts AS epoch
  FROM hx.raft_groups rg
  INNER JOIN hx.server_ident srv ON rg.server_pk = srv.pk
  INNER JOIN expected e ON rg.group_name = e.raft_group
  WHERE rg.epoch = epoch_ts
    AND rg.size > e.num_replicas
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rg.group_name, rg.epoch
    ORDER BY (srv.name = rg.leader) DESC, rg.size DESC, rg.server_pk
  ) = 1;

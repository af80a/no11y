CREATE OR REPLACE MACRO audit.check_meta_003(epoch_start, epoch_end, p_max_changes := 1, p_window_minutes := 10) AS TABLE
  WITH range_epochs AS (
    SELECT DISTINCT epoch FROM hx.meta_cluster_stats WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  -- One authoritative leader per epoch in the lookback window: the
  -- most-reported leader PK at that epoch, ties broken deterministically.
  epoch_leader AS (
    SELECT eval_epoch, m_epoch, leader_pk
    FROM (
      SELECT re.epoch AS eval_epoch,
             m.epoch  AS m_epoch,
             COALESCE(NULLIF(m.peer_server_pk, 0), m.server_pk) AS leader_pk,
             ROW_NUMBER() OVER (
               PARTITION BY re.epoch, m.epoch
               ORDER BY COUNT(*) DESC, COALESCE(NULLIF(m.peer_server_pk, 0), m.server_pk) ASC
             ) AS rn
      FROM range_epochs re
      INNER JOIN hx.meta_cluster_stats m
        ON m.epoch >  re.epoch - (p_window_minutes * INTERVAL '1 minute')
       AND m.epoch <= re.epoch
       AND m.is_leader = true
      GROUP BY re.epoch, m.epoch, COALESCE(NULLIF(m.peer_server_pk, 0), m.server_pk)
    ) WHERE rn = 1
  ),
  -- Count transitions across consecutive epochs within each eval window.
  transitions AS (
    SELECT eval_epoch,
           leader_pk,
           LAG(leader_pk) OVER (PARTITION BY eval_epoch ORDER BY m_epoch) AS prev_leader_pk
    FROM epoch_leader
  ),
  leader_changes AS (
    SELECT eval_epoch,
           COUNT(*) FILTER (WHERE prev_leader_pk IS NOT NULL AND leader_pk <> prev_leader_pk) AS changes
    FROM transitions
    GROUP BY eval_epoch
  )
  SELECT
    'META_003' AS code,
    'meta cluster' AS entity,
    0 AS entity_pk,
    changes,
    eval_epoch AS epoch
  FROM leader_changes
  WHERE changes > p_max_changes;

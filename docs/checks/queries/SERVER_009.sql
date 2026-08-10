CREATE OR REPLACE MACRO audit.check_server_009(epoch_start, epoch_end, p_max_restarts := 2) AS TABLE
  WITH range_epochs AS (
    SELECT DISTINCT epoch FROM hx.server_stats WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  -- Boot generations per name: only pks with stats inside the window count,
  -- which bounds the metric to the window (a crash loop that ended more than
  -- an hour ago no longer fires).
  restart_counts AS (
    SELECT re.epoch, i.name, COUNT(DISTINCT i.start_time) AS restart_count
    FROM range_epochs re
    INNER JOIN hx.server_stats s ON s.epoch >= re.epoch - INTERVAL '1 hour' AND s.epoch <= re.epoch
    INNER JOIN hx.server_ident i ON s.server_pk = i.pk
    GROUP BY re.epoch, i.name
  ),
  -- The current pk (and cluster) for the name at the evaluated epoch.
  current_idents AS (
    SELECT DISTINCT re.epoch, i.pk, i.name, i.cluster
    FROM range_epochs re
    INNER JOIN hx.server_stats s ON s.epoch = re.epoch
    INNER JOIN hx.server_ident i ON s.server_pk = i.pk
  )
  SELECT
    'SERVER_009' AS code,
    CASE WHEN ci.cluster IS NOT NULL AND ci.cluster != '' THEN ci.cluster || ' / ' || ci.name ELSE ci.name END AS entity,
    ci.pk AS entity_pk,
    rc.restart_count,
    rc.epoch
  FROM restart_counts rc
  INNER JOIN current_idents ci ON rc.name = ci.name AND rc.epoch = ci.epoch
  WHERE rc.restart_count > p_max_restarts;

CREATE OR REPLACE MACRO audit.check_service_002(epoch_start, epoch_end) AS TABLE
  WITH prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.sub_stats WHERE epoch < epoch_start
  ),
  range_epochs AS (
    SELECT epoch FROM prev_epoch WHERE epoch IS NOT NULL
    UNION
    SELECT DISTINCT epoch FROM hx.sub_stats WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  epoch_pairs AS (
    SELECT epoch, LAG(epoch) OVER (ORDER BY epoch) AS prev_epoch FROM range_epochs
  ),
  prev_services AS (
    SELECT ep.epoch, si.account_pk, si.service_name
    FROM epoch_pairs ep
    INNER JOIN hx.services si ON si.epoch = ep.prev_epoch
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
    GROUP BY ep.epoch, si.account_pk, si.service_name
  ),
  curr_services AS (
    SELECT DISTINCT epoch, account_pk, service_name
    FROM hx.services
    WHERE epoch BETWEEN epoch_start AND epoch_end
  )
  SELECT
    'SERVICE_002' AS code,
    ai.name || ' / ' || p.service_name AS entity,
    p.account_pk AS entity_pk,
    p.service_name,
    p.epoch
  FROM prev_services p
  LEFT JOIN curr_services c ON p.epoch = c.epoch AND p.account_pk = c.account_pk AND p.service_name = c.service_name
  INNER JOIN hx.account_ident ai ON p.account_pk = ai.pk
  WHERE c.service_name IS NULL;

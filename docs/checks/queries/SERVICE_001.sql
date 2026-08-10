CREATE OR REPLACE MACRO audit.check_service_001(epoch_start, epoch_end) AS TABLE
  WITH instances AS (
    SELECT
      si.epoch,
      si.account_pk,
      si.service_name,
      ai.name AS account_name,
      si.lang,
      si.version,
      COUNT(*) AS cnt
    FROM hx.services si
    INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
    WHERE si.epoch BETWEEN epoch_start AND epoch_end
    GROUP BY si.epoch, si.account_pk, si.service_name, ai.name, si.lang, si.version
  ),
  services_with_variants AS (
    SELECT epoch, account_pk, service_name, account_name
    FROM instances
    GROUP BY epoch, account_pk, service_name, account_name
    HAVING COUNT(*) > 1
  )
  SELECT
    'SERVICE_001' AS code,
    s.account_name || ' / ' || s.service_name AS entity,
    s.account_pk AS entity_pk,
    string_agg(i.lang || '/' || i.version || ' (' || i.cnt || ')', ', ' ORDER BY i.cnt DESC) AS versions,
    s.service_name,
    s.epoch
  FROM services_with_variants s
  INNER JOIN instances i ON s.account_pk = i.account_pk AND s.service_name = i.service_name AND s.epoch = i.epoch
  GROUP BY s.epoch, s.account_pk, s.service_name, s.account_name;

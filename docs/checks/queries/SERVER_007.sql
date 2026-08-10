CREATE OR REPLACE MACRO audit.check_server_007(epoch_start, epoch_end) AS TABLE
  SELECT
    'SERVER_007' AS code,
    CASE WHEN cluster IS NOT NULL AND cluster != '' THEN cluster || ' / ' || name ELSE name END AS entity,
    pk AS entity_pk,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND auth_required = false;

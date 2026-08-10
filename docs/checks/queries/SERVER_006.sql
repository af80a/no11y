CREATE OR REPLACE MACRO audit.check_server_006(epoch_start, epoch_end) AS TABLE
  SELECT
    'SERVER_006' AS code,
    CASE WHEN cluster IS NOT NULL AND cluster != '' THEN cluster || ' / ' || name ELSE name END AS entity,
    pk AS entity_pk,
    js_domain,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND js_domain != ''
    AND regexp_matches(js_domain, '\s');

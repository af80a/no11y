CREATE OR REPLACE MACRO audit.check_accounts_004(epoch_start, epoch_end) AS TABLE
  WITH
    exports AS (
      SELECT e.*, ai.name AS account_name
      FROM hx.account_export_opts e
      JOIN hx.account_ident ai ON ai.pk = e.account_pk
      WHERE e.epoch = (
        SELECT MAX(epoch) FROM hx.account_export_opts
        WHERE account_pk = e.account_pk AND epoch <= epoch_end
      )
      AND e.epoch <= epoch_end
    ),
    all_imports AS (
      SELECT ai.*
      FROM hx.account_import_opts ai
      WHERE ai.epoch = (
        SELECT MAX(epoch) FROM hx.account_import_opts
        WHERE account_pk = ai.account_pk AND epoch <= epoch_end
      )
      AND ai.epoch <= epoch_end
    ),
    non_system AS (
      SELECT ao.account_pk
      FROM hx.account_opts ao
      WHERE ao.is_system = false
        AND ao.epoch = (
          SELECT MAX(epoch) FROM hx.account_opts
          WHERE account_pk = ao.account_pk AND epoch <= epoch_end
        )
    )
  SELECT
    'ACCOUNTS_004' AS code,
    e.account_name AS entity,
    e.account_pk AS entity_pk,
    e.subject,
    e.type,
    epoch_end AS epoch
  FROM exports e
  WHERE e.account_pk IN (SELECT account_pk FROM non_system)
    AND NOT EXISTS (
      SELECT 1 FROM all_imports i
      WHERE i.account = e.account_name
        AND audit.subjects_overlap(e.subject, i.subject)
        AND i.type = e.type
    );

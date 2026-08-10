CREATE OR REPLACE MACRO audit.check_accounts_003(epoch_start, epoch_end) AS TABLE
  WITH
    invalid_imports AS (
      SELECT ai.*, imp_acct.name AS importer_name,
        (SELECT pk FROM hx.account_ident WHERE name = ai.account LIMIT 1) AS export_account_pk,
        (SELECT token_req FROM hx.account_export_opts e
         WHERE e.account_pk = (SELECT pk FROM hx.account_ident WHERE name = ai.account LIMIT 1)
           AND audit.subjects_overlap(e.subject, ai.subject)
           AND e.type = ai.type
           AND e.epoch = (SELECT MAX(epoch) FROM hx.account_export_opts WHERE account_pk = e.account_pk AND epoch <= epoch_end)
         LIMIT 1
        ) AS export_token_req,
        (SELECT e.subject FROM hx.account_export_opts e
         WHERE e.account_pk = (SELECT pk FROM hx.account_ident WHERE name = ai.account LIMIT 1)
           AND audit.subjects_overlap(e.subject, ai.subject)
           AND e.type = ai.type
           AND e.epoch = (SELECT MAX(epoch) FROM hx.account_export_opts WHERE account_pk = e.account_pk AND epoch <= epoch_end)
         LIMIT 1
        ) AS export_subject,
        (SELECT signing_keys FROM hx.account_opts
         WHERE account_pk = (SELECT pk FROM hx.account_ident WHERE name = ai.account LIMIT 1)
           AND epoch = (SELECT MAX(epoch) FROM hx.account_opts WHERE account_pk = (SELECT pk FROM hx.account_ident WHERE name = ai.account LIMIT 1) AND epoch <= epoch_end)
         LIMIT 1
        ) AS export_signing_keys
      FROM hx.account_import_opts ai
      JOIN hx.account_ident imp_acct ON imp_acct.pk = ai.account_pk
      WHERE ai.source = 'jwt'
        AND ai.invalid = true
        AND ai.epoch = (
          SELECT MAX(epoch) FROM hx.account_import_opts
          WHERE account_pk = ai.account_pk AND epoch <= epoch_end
        )
        AND ai.epoch <= epoch_end
    ),
    diagnosed AS (
      SELECT
        i.*,
        CASE
          WHEN i.export_account_pk IS NULL THEN 'Source account not found'
          WHEN i.export_subject IS NULL THEN 'Source export not found'
          WHEN i.export_token_req AND NOT i.has_token THEN 'Missing activation token'
          WHEN i.has_token AND i.token_expires > 0
            AND to_timestamp(i.token_expires) < epoch_end
            THEN 'Expired activation token'
          WHEN i.has_token AND i.export_signing_keys IS NOT NULL
            AND NOT list_contains(i.export_signing_keys, i.token_issuer)
            AND i.token_issuer != i.account
            THEN 'Activation token signed by rotated signing key'
          ELSE 'Import activation failed'
        END AS reason
      FROM invalid_imports i
    )
  SELECT
    'ACCOUNTS_003' AS code,
    importer_name AS entity,
    account_pk AS entity_pk,
    subject,
    COALESCE(export_account_pk, 0) AS export_account_pk,
    account AS export_account_name,
    reason,
    epoch_end AS epoch
  FROM diagnosed;

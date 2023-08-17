SELECT quote_policy.product_name AS product_name,
    quote_peril_characteristics.locator AS quote_peril_characteristics_locator,
    quote_policy.locator AS quote_locator,
    quote_policy.policy_locator AS policy_locator,
    quote_policy.payment_schedule_name AS payment_schedule_name,
    exposure.locator AS exposure_locator,
    exposure.name AS exposure_name,
    peril.locator AS peril_locator,
    peril.name AS peril_name,
    quote_peril_characteristics.premium AS quoted_premium,
    quote_peril_characteristics.premium AS quoted_technical_premium,
    quote_peril_characteristics.start_timestamp AS coverage_start,
    quote_peril_characteristics.end_timestamp AS coverage_end,
    quote_policy.issued_timestamp AS quote_issued,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM quote_policy_characteristics_fields pchf
      WHERE is_group = false AND pchf.quote_policy_characteristics_locator = quote_policy_characteristics.locator) AS policy_fields,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM quote_exposure_characteristics_fields echf
      WHERE is_group = false AND echf.quote_exposure_characteristics_locator = quote_exposure_characteristics.locator) AS exposure_fields,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM quote_peril_characteristics_fields pchf
      WHERE is_group = false AND pchf.quote_peril_characteristics_locator = quote_peril_characteristics.locator) AS peril_fields
FROM quote_peril_characteristics
JOIN peril ON quote_peril_characteristics.quote_peril_locator = peril.locator
JOIN quote_exposure_characteristics ON quote_peril_characteristics.quote_policy_locator = quote_exposure_characteristics.quote_policy_locator
JOIN quote_policy_characteristics ON quote_peril_characteristics.quote_policy_locator = quote_policy_characteristics.quote_policy_locator
JOIN exposure ON quote_exposure_characteristics.quote_exposure_locator = exposure.locator
JOIN quote_policy ON quote_peril_characteristics.quote_policy_locator = quote_policy.locator
WHERE quote_peril_characteristics.start_timestamp <= {end_timestamp}
    AND quote_peril_characteristics.end_timestamp >= {start_timestamp}
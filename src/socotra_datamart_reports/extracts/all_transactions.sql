SELECT policy.product_name AS product_name,
    policy.locator AS policy_locator,
    policy.payment_schedule_name AS payment_schedule_name,
    exposure.locator AS exposure_locator,
    exposure.name AS exposure_name,
    peril.locator AS peril_locator,
    peril.name AS peril_name,
    FROM_UNIXTIME(peril_characteristics.issued_timestamp / 1000, '%Y-%m-%d') AS issued_date,
    peril_characteristics.premium AS written_premium,
    FROM_UNIXTIME(peril_characteristics.start_timestamp / 1000, '%Y-%m-%d') AS coverage_start,
    FROM_UNIXTIME(peril_characteristics.end_timestamp / 1000, '%Y-%m-%d') AS coverage_end,
    pm.name AS modification_name,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM policy_characteristics_fields pchf
      WHERE is_group = false AND pchf.policy_characteristics_locator = peril_characteristics.policy_characteristics_locator) AS policy_fields,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM exposure_characteristics_fields echf
      WHERE is_group = false AND echf.exposure_characteristics_locator = peril_characteristics.exposure_characteristics_locator) AS exposure_fields,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM peril_characteristics_fields pchf
      WHERE is_group = false AND pchf.peril_characteristics_locator = peril_characteristics.locator) AS peril_fields
FROM peril_characteristics
JOIN peril ON peril_characteristics.peril_locator = peril.locator
JOIN policy_modification pm ON peril_characteristics.policy_modification_locator = pm.locator
JOIN exposure_characteristics ON peril_characteristics.exposure_characteristics_locator = exposure_characteristics.locator
JOIN policy_characteristics ON peril_characteristics.policy_characteristics_locator = policy_characteristics.locator
JOIN exposure ON exposure_characteristics.exposure_locator = exposure.locator
JOIN policy ON peril_characteristics.policy_locator = policy.locator
WHERE peril_characteristics.start_timestamp <= {end_timestamp}
    AND peril_characteristics.end_timestamp >= {start_timestamp}
    AND peril_characteristics.replaced_timestamp IS NULL;
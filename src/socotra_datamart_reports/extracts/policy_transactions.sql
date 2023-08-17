SELECT policy.locator AS policy_locator,
COALESCE(peril_characteristics.locator, pc2.locator) AS peril_characteristics_locator,
exposure.locator AS exposure_locator,
peril.locator AS peril_locator,
policy.product_name AS product_name,
exposure.name AS exposure_name,
peril.name AS peril_name,
pm.issued_timestamp AS issued,
pc2.premium AS replacement_of_premium,
peril_characteristics.premium AS premium_change,
CASE
    WHEN pm.type IN ("create", "reinstate", "renew") THEN peril_characteristics.premium
    WHEN pm.type IN ("cancel") THEN peril_characteristics.premium - pc2.premium
    WHEN pm.type IN ("endorsement") AND peril_characteristics.replacement_of_locator IS NULL THEN peril_characteristics.premium
    ELSE 0
END premium_delta,
peril_characteristics.start_timestamp AS coverage_start,
peril_characteristics.end_timestamp  AS coverage_end,
pm.name AS mod_name,
pm.locator AS mod_locator,
pm.type as mod_type,
exposure_characteristics.locator AS exposure_characteristics_locator,
policy_characteristics.locator AS policy_characteristics_locator,
peril_characteristics.replacement_of_locator AS replaced_locator,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM policy_characteristics_fields pchf
      WHERE is_group = false AND pchf.policy_characteristics_locator = peril_characteristics.policy_characteristics_locator) AS policy_fields,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM exposure_characteristics_fields echf
      WHERE is_group = false AND echf.exposure_characteristics_locator = peril_characteristics.exposure_characteristics_locator) AS exposure_fields,
    (SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''),'field_name', field_name,'id', id, 'field_value', field_value)) FROM peril_characteristics_fields pchf
      WHERE is_group = false AND pchf.peril_characteristics_locator = peril_characteristics.locator) AS peril_fields
FROM peril_characteristics
JOIN peril ON peril_characteristics.peril_locator = peril.locator
JOIN exposure_characteristics ON peril_characteristics.exposure_characteristics_locator = exposure_characteristics.locator
JOIN policy_characteristics ON peril_characteristics.policy_characteristics_locator = policy_characteristics.locator
JOIN exposure ON exposure_characteristics.exposure_locator = exposure.locator
JOIN policy ON peril_characteristics.policy_locator = policy.locator
JOIN policy_modification pm ON peril_characteristics.policy_modification_locator = pm.locator
LEFT OUTER JOIN peril_characteristics pc2 ON peril_characteristics.replacement_of_locator=pc2.locator
WHERE
    pm.issued_timestamp >= {start_timestamp} AND pm.issued_timestamp < {end_timestamp}
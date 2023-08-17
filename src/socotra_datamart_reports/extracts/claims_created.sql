SELECT 
	locator AS claim_locator,
	policy_locator,
	created_timestamp AS created,
	product_name,
	(SELECT JSON_ARRAYAGG(JSON_OBJECT('parent_name', coalesce(parent_name,''), 'parent_locator', coalesce(parent_locator, ''), 'field_name', field_name, 'id', id, 'field_value', field_value)) FROM claim_fields clmf WHERE clmf.claim_locator = claim.locator) AS claim_fields
FROM claim
WHERE claim.created_timestamp >= {start_timestamp}
{product_statement}
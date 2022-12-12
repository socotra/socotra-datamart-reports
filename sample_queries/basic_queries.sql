/**
 * Count of policyholders
 */
SELECT COUNT(*) FROM policyholder;

/**
 * Count of policies
 */
SELECT COUNT(*) FROM policy;

/**
 * Select all in-effect policies with monthly payment schedules
 */
SELECT *
FROM policy
WHERE `issued_timestamp` IS NOT NULL
    AND `cancellation_timestamp` IS NULL
    AND `payment_schedule_name` LIKE 'monthly'
    AND `policy_start_timestamp` < NOW()
    AND `policy_end_timestamp` > NOW();

/**
 * Select all policies issued with written premium within an interval
 */
SET @start_timestamp = unix_timestamp('2022-01-01') * 1000;
SET @end_timestamp = unix_timestamp('2022-07-01') * 1000;
SET @as_of_timestamp = unix_timestamp('2022-07-01') * 1000;

SELECT
pol.locator,
exp.name AS exp_name, exp.locator AS exp_locator,
per.name AS per_name, per.locator AS per_locator,
from_unixtime(MIN(per_c.start_timestamp/1000, '%Y-%m-%d')) AS effective_date,
from_unixtime(MAX(per_c.end_timestamp/1000, '%Y-%m-%d')) AS expiry_date,
SUM(per_c.premium) AS premium
FROM policy pol
JOIN exposure exp ON pol.locator = exp.policy_locator
JOIN peril per ON exp.locator = per.exposure_locator
JOIN peril_characteristics per_c ON per.locator = per_c.peril_locator
-- Policy is issued within the range provided
WHERE pol.issued_timestamp between @start_timestamp AND @end_timestamp
-- Keep all transactions created after the "as of" date before that date
AND per_c.created_timestamp <= @as_of_timestamp
-- Keep all transactions which have not been replaced before the "as of" date
AND (
    per_c.replaced_timestamp > @as_of_timestamp
    OR
    per_c.replaced_timestamp is null
)
GROUP BY exp_locator, exp_name, per_locator
ORDER BY pol.locator;

/**
 * Summarize specific set of field values with corresponding characteristics and policy locators, across all policies
 */
SELECT
pc.policy_locator,
pc.start_timestamp,
pc.end_timestamp,
pcf.policy_characteristics_locator,
pcf.parent_locator,
-- Basic template for each field: MAX for aggregation requirement and null filter
MAX(CASE WHEN pcf.field_name = "driver_firstname" THEN pcf.field_value END) "driver_firstname",
MAX(CASE WHEN pcf.field_name = "driver_lastname" THEN pcf.field_value END) "driver_lastname",
MAX(CASE WHEN pcf.field_name = "driver_designation" THEN pcf.field_value END) "driver_designation"
FROM policy_characteristics_fields pcf
JOIN policy_characteristics pc ON pcf.policy_characteristics_locator = pc.locator
WHERE parent_name = "drivers"
GROUP BY pcf.policy_characteristics_locator, pcf.parent_locator
ORDER BY pcf.policy_characteristics_locator, pcf.parent_locator ASC;

/**
 * Categorized count of all modifications on a policy
 */
SET @policy_locator = 'locator-value'

SELECT pm.type as modification_type, COUNT(pm.type) as count
FROM policy_modification pm
WHERE pm.policy_locator = @policy_locator
GROUP BY pm.type;

/**
 * Fetch grace periods for a given policy
 */
SET @policy_locator = 'locator-value';

SELECT * FROM grace_period WHERE policy_locator = @policy_locator;

/**
 * Summarize financial transactions on invoices for a policy
 */
SET @policy_locator = 'locator-value';

SELECT tx.type, tx.amount,
tx.peril_characteristics_locator, tx.invoice_locator,
invoice.due_timestamp, invoice.settlement_status, invoice.settlement_type
FROM financial_transaction tx
JOIN invoice ON tx.invoice_locator = invoice.locator
WHERE tx.policy_locator = @policy_locator;


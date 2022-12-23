def get_on_risk_query(product_name: str, as_of_timestamp: int):
    return f"""
        SELECT policy.product_name AS product_name,
               policy.locator AS policy_id,
               policy.payment_schedule_name AS payment_schedule_name,
               exposure.locator AS exposure_locator,
               exposure.name AS exposure_name,
               peril.locator AS peril_locator,
               peril.name AS peril_name,
               FROM_UNIXTIME(peril_characteristics.issued_timestamp / 1000, '%Y%m%d') AS issued_date,
               peril_characteristics.premium AS premium,
               FROM_UNIXTIME(peril_characteristics.start_timestamp / 1000, '%Y%m%d') AS coverage_start,
               FROM_UNIXTIME(peril_characteristics.end_timestamp / 1000, '%Y%m%d') AS coverage_end,
               policy_characteristics.locator AS policy_characteristics_locator,
               exposure_characteristics.locator AS exposure_characteristics_locator,
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
        WHERE
            policy.product_name = "{product_name}"
            AND peril_characteristics.start_timestamp <= {as_of_timestamp}
            AND peril_characteristics.end_timestamp > {as_of_timestamp}
            AND peril_characteristics.replaced_timestamp IS NULL
            AND exposure_characteristics.start_timestamp <= {as_of_timestamp}
            AND exposure_characteristics.end_timestamp > {as_of_timestamp}
            AND exposure_characteristics.replaced_timestamp IS NULL
            AND policy_characteristics.start_timestamp <= {as_of_timestamp}
            AND policy_characteristics.end_timestamp > {as_of_timestamp}
            AND policy_characteristics.replaced_timestamp IS NULL
        ORDER BY policy_id;
    """


def get_all_policies_query(product_name: str, start_timestamp: int, end_timestamp: int):
    return f"""
        SELECT policy.product_name AS product_name,
           policy.locator AS policy_id,
           policy.payment_schedule_name AS payment_schedule_name,
           exposure.locator AS exposure_locator,
           exposure.name AS exposure_name,
           peril.locator AS peril_locator,
           peril.name AS peril_name,
           FROM_UNIXTIME(peril_characteristics.issued_timestamp / 1000, '%Y%m%d') AS issued_date,
           peril_characteristics.premium AS premium,
           FROM_UNIXTIME(peril_characteristics.start_timestamp / 1000, '%Y%m%d') AS coverage_start,
           FROM_UNIXTIME(peril_characteristics.end_timestamp / 1000, '%Y%m%d') AS coverage_end,
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
        WHERE policy.product_name = "{product_name}"
            AND peril_characteristics.start_timestamp <= {end_timestamp}
            AND peril_characteristics.end_timestamp >= {start_timestamp}
            AND peril_characteristics.replaced_timestamp IS NULL;
    """


def get_transactions_query(product_name: str, start_timestamp: int, end_timestamp: int):
    return f"""
       SELECT policy.locator AS policy_locator,
       exposure.locator AS exposure_locator,
       peril.locator AS peril_locator,
       policy.product_name AS product_name,
       exposure.name AS exposure_name,
       peril.name AS peril_name,
       FROM_UNIXTIME(pm.issued_timestamp / 1000, '%Y%m%d') AS issued_date,
       pc2.premium AS replacement_of_premium,
       peril_characteristics.premium AS premium,
       CASE
           WHEN pm.type IN ("create", "reinstate", "renew") THEN peril_characteristics.premium
           WHEN pm.type IN ("cancel") THEN peril_characteristics.premium - pc2.premium
           WHEN pm.type IN ("endorsement") AND peril_characteristics.replacement_of_locator IS NULL THEN peril_characteristics.premium
           ELSE 0
       END premium_delta,
       FROM_UNIXTIME(peril_characteristics.start_timestamp / 1000, '%Y%m%d') AS coverage_start,
       FROM_UNIXTIME(peril_characteristics.end_timestamp / 1000, '%Y%m%d')  AS coverage_end,
       pm.name AS mod_name,
       pm.locator AS mod_locator,
       pm.type as mod_type,
       peril_characteristics.locator AS peril_char_locator,
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
            policy.product_name = "{product_name}" AND
            pm.issued_timestamp >= {start_timestamp} AND pm.issued_timestamp < {end_timestamp}
    """


def get_policies_query(start_timestamp: int, end_timestamp: int):
    return f"""
        SELECT p.locator AS policy_locator,
           p.product_name AS product_name
        FROM policy p
        WHERE p.issued_timestamp >= {start_timestamp} AND p.issued_timestamp < {end_timestamp}
    """


def get_coverage_query(start_timestamp: int, end_timestamp: int):
    return f"""
        SELECT policy.locator AS policy_locator,
           exposure.locator AS exposure_locator,
           peril.locator AS peril_locator,
           policy.product_name AS product_name,
           exposure.name AS exposure_name,
           peril.name AS peril_name,
           peril_characteristics.premium AS premium,
           peril_characteristics.start_timestamp AS coverage_start_timestamp,
           peril_characteristics.end_timestamp AS coverage_end_timestamp,
           peril_characteristics.locator AS peril_char_locator,
           exposure_characteristics.locator AS exposure_characteristics_locator,
           policy_characteristics.locator AS policy_characteristics_locator
        FROM peril_characteristics
        JOIN peril ON peril_characteristics.peril_locator = peril.locator
        JOIN exposure_characteristics ON peril_characteristics.exposure_characteristics_locator = exposure_characteristics.locator
        JOIN policy_characteristics ON peril_characteristics.policy_characteristics_locator = policy_characteristics.locator
        JOIN exposure ON exposure_characteristics.exposure_locator = exposure.locator
        JOIN policy ON peril_characteristics.policy_locator = policy.locator
        WHERE
            {start_timestamp} <= peril_characteristics.end_timestamp AND {end_timestamp} >= peril_characteristics.start_timestamp
            AND peril_characteristics.replaced_timestamp IS NULL
    """


def get_financial_transactions_query(start_timestamp: int, end_timestamp: int):
    return f"""
        SELECT
              ft.id,
              ft.amount,
              policy.currency,
              ft.posted_timestamp,
              ft.type,
              policy.locator AS policy_locator,
              policy.product_name AS product_name,
              ft.peril_characteristics_locator,
              ft.peril_name,
              ft.tax_name,
              ft.fee_name,
              ft.commission_recipient
        FROM financial_transaction ft JOIN policy ON policy.locator = ft.policy_locator
        LEFT JOIN peril_characteristics pc ON pc.locator = ft.peril_characteristics_locator
        LEFT JOIN peril ON peril.locator = pc.peril_locator
        WHERE ft.posted_timestamp >= {start_timestamp} AND ft.posted_timestamp < {end_timestamp}
    """

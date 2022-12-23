import socotra_datamart_reports.lib.get_flattened_fields as gff

def test_distinguishes_entries_belonging_to_group_field():
    non_group_entry = {
        'parent_locator': '',
        'field_name': 'location',
        'field_value': 'somewhere',
        'peril_characteristics_locator': '12312-3123-1231231',
        'parent_name': '',
        'datamart_created_timestamp': 1659326400000,
        'datamart_updated_timestamp': 1659326400000,
        'is_group': False,
        'id': 123456
    }

    group_entry = {
        'parent_locator': '2313-21312-12311',
        'field_name': 'first_name',
        'field_value': 'somebody',
        'peril_characteristics_locator': '12312-3123-1231231',
        'parent_name': 'drivers',
        'datamart_created_timestamp': 1659326400000,
        'datamart_updated_timestamp': 1659326400000,
        'is_group': False,
        'id': 123456
    }

    assert gff.entry_belongs_to_group_field(non_group_entry) == False
    assert gff.entry_belongs_to_group_field(group_entry) == True

def test_get_flattened_fields_empty_list_returns_empty_list():
    assert gff.get_flattened_fields('Prefix', []) == []

def test_get_flattened_fields_returns_expected_result():
    sample_collection = [[
        {
            'parent_locator': '',
            'field_name': 'location',
            'field_value': 'somewhere',
            'peril_characteristics_locator': '12312-3123-1231231',
            'parent_name': '',
            'datamart_created_timestamp': 1659326400000,
            'datamart_updated_timestamp': 1659326400000,
            'is_group': False,
            'id': 123456
        },
        {
            'parent_locator': '2313-21312-12311',
            'field_name': 'first_name',
            'field_value': 'some first name',
            'peril_characteristics_locator': '12312-3123-1231231',
            'parent_name': 'drivers',
            'datamart_created_timestamp': 1659326400000,
            'datamart_updated_timestamp': 1659326400000,
            'is_group': False,
            'id': 123456
        },
        {
            'parent_locator': '2313-21312-12311',
            'field_name': 'last_name',
            'field_value': 'some last name',
            'peril_characteristics_locator': '12312-3123-1231231',
            'parent_name': 'drivers',
            'datamart_created_timestamp': 1659326400000,
            'datamart_updated_timestamp': 1659326400000,
            'is_group': False,
            'id': 123456
        }
    ]]

    result = gff.get_flattened_fields('Policy', sample_collection)

    assert sorted(list(result[0].keys())) == [
        'Policy drivers first_name 1',
        'Policy drivers last_name 1',
        'Policy location',
    ]

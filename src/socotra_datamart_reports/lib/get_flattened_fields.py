import itertools
import json


def entry_belongs_to_group_field(entry):
    return entry['parent_locator'] != ''


def get_flattened_fields(prefix: str, collection):
    """
    Returns ordered lists of dicts of flattened fields for a collection
    of aggregated field data
    :param prefix: optional string to precede final column name for the field
    :param collection: a list of rows of JSON-aggregated field values
    :return: list of lists of flattened values
    """
    all_single_count = {}
    all_group_count = {}

    all_single_entry_maps = []
    all_group_entry_maps = []

    all_single_entry_maps_final_named = []
    all_group_entry_maps_final_named = []

    # Part I: Survey fields across all rows
    for entry_collection in collection:
        single_fields = [entry for entry in entry_collection
                         if not entry_belongs_to_group_field(entry)]
        group_fields = [entry for entry in entry_collection
                        if entry_belongs_to_group_field(entry)]

        single_entry_map = {}
        for field in single_fields:
            field_name = field['field_name']
            if single_entry_map.get(field_name) is None:
                single_entry_map[field_name] = set()
            single_entry_map[field_name].add(
                (field['id'], field['field_value']))

        all_single_entry_maps.append(single_entry_map)

        for k, v in single_entry_map.items():
            all_single_count[k] = max(all_single_count.get(k, 0), len(v))

        # Each individual entry in the group_entry_map will be of the form
        # { parent_name:
        #    { parent_locator:
        #      {
        #         field_name: [ (id, field_value ), ... ]
        #      }, ...
        #    }, ...
        # }
        #
        # In this way, we account for the possibility that a given field is
        # repeatable within the field group itself.
        group_entry_map = {}
        for field in group_fields:
            parent_field_name = field['parent_name']
            parent_field_locator = field['parent_locator']
            field_name = field['field_name']
            field_value = field['field_value']
            field_id = field['id']
            group_entry_map_key = group_entry_map.get(parent_field_name)
            if group_entry_map_key is None:
                group_entry_map[parent_field_name] = {}
            if group_entry_map.get(parent_field_name).get(
                    parent_field_locator) is None:
                group_entry_map[parent_field_name][parent_field_locator] = {}
            if group_entry_map[parent_field_name][parent_field_locator].get(
                    field_name) is None:
                group_entry_map[parent_field_name][parent_field_locator][field_name] = []
            group_entry_map[parent_field_name][parent_field_locator][field_name].append((field_id, field_value))

        all_group_entry_maps.append(group_entry_map)

        for k, v in group_entry_map.items():
            all_group_count[k] = max(all_group_count.get(k, 0), len(v.keys()))

    # Part II: Flatten field values for each row
    for field_map in all_single_entry_maps:
        entry_collection_final_value_map = {}
        for field_name, value_pairs in field_map.items():  # each value a tuple of (id, fieldValue)
            max_count = all_single_count[field_name]
            if max_count == 1:
                # if max count is 1, then we know that this can have only one entry
                # and that the final name needs no number after the name
                final_field_value_name = f'{prefix} {field_name}'
                entry_collection_final_value_map[final_field_value_name] = list(value_pairs)[0][1]
            else:
                # If max count > 1, we need to cycle over each value, and qualify each one with
                # a number after the name. We sort by `id` first to help ensure that ordering remains consistent
                # for the given aggregation level.
                sorted_value_pairs = sorted(value_pairs, key=lambda value_pair: value_pair[0])
                for idx, v in enumerate(sorted_value_pairs):
                    final_field_value_name = f'{prefix} {field_name} {idx + 1}'
                    entry_collection_final_value_map[final_field_value_name] = v[1]

        all_single_entry_maps_final_named.append(entry_collection_final_value_map)

    # group fields
    for entry_field_group_map in all_group_entry_maps:
        entry_collection_final_group_value_map = {}
        for parent_field_name, parent_field_entries in entry_field_group_map.items():
            max_count = all_group_count[parent_field_name]
            if max_count == 1:
                # If max count is 1, then we know that there can be only one entry in the dictionary
                child_items = list(parent_field_entries.values())[0]
                for child_item in child_items.items():
                    child_field_name = child_item[0]
                    child_field_values = child_item[1]
                    for idx, child_field_value in enumerate(child_field_values):
                        final_child_field_name = f'{prefix} {parent_field_name} {child_field_name} {idx + 1}'
                        entry_collection_final_group_value_map[final_child_field_name] = child_field_value[1]
            else:
                for parent_idx, parent_field_entry in enumerate(sorted(list(parent_field_entries.items()),
                                                                       key=lambda i: i[0])):
                    child_items = parent_field_entry[1]
                    for child_item in child_items.items():
                        child_field_name = child_item[0]
                        child_field_values = child_item[1]
                        for idx, child_field_value in enumerate(child_field_values):
                            final_child_field_name = \
                                f'{prefix} {parent_field_name} {parent_idx + 1} {child_field_name} {idx + 1}'
                            entry_collection_final_group_value_map[final_child_field_name] = child_field_value[1]
        all_group_entry_maps_final_named.append(entry_collection_final_group_value_map)

    all_final_results = []
    for result_pair in zip(all_single_entry_maps_final_named, all_group_entry_maps_final_named):
        all_final_results.append(result_pair[0] | result_pair[1])
    return all_final_results


def get_flattened_results(results):
    """
    Takes row results with JSON in policy_fields, exposure_fields, peril_fields,
    returning the rows augmented with columns representing all flattened values
    :param results:
    :return:
    """
    # get results for policy_fields, exposure_fields, peril_fields
    all_fields = {'policy': [], 'exposure': [], 'peril': []}
    for row in results:
        if row.get('policy_fields') is not None:
            all_fields['policy'].append(json.loads(row['policy_fields']))
        if row.get('exposure_fields') is not None:
            all_fields['exposure'].append(json.loads(row['exposure_fields']))
        if row.get('peril_fields') is not None:
            all_fields['peril'].append(json.loads(row['peril_fields']))

    flattened_exposure_fields = get_flattened_fields('Exposure', all_fields['exposure'])
    flattened_policy_fields = get_flattened_fields('Policy', all_fields['policy'])
    flattened_peril_fields = get_flattened_fields('Peril', all_fields['peril'])

    final_results = []
    for result_groups in itertools.zip_longest(
            results, flattened_policy_fields, flattened_exposure_fields, flattened_peril_fields, fillvalue={}):
        final_results.append(result_groups[0] | result_groups[1] | result_groups[2] | result_groups[3])

    return final_results

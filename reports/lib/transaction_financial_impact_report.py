import csv
import mysql.connector
import reports.lib.common.queries
import reports.lib.common.get_flattened_fields


class TransactionFinancialImpactReport:
    def __init__(self, creds):
        self.creds = creds

    def _is_endorsement_mod(self, mod_peril_char):
        """
        Determines whether a given peril mod represents an endorsement.
        Needed until `type` in policy_modification is always just `endorsement`
        and not also (possibly) the configured name of the endorsement.

        Assumes configs will not configure endorsements with
         any of the non-endorsement type names.
        :param mod_peril_char:
        :return: boolean
        """
        mod_type = mod_peril_char.get('mod_type')
        return mod_type not in ['create', 'cancel', 'lapse', 'renew', 'reinstate', 'withdraw', 'withdrawal']

    def _fetch_all_results_for_query(self, query):
        connection_port = self.creds.get('port')
        if connection_port is None:
            connection_port = 3306
        cnx = mysql.connector.connect(user=self.creds.get('user'), password=self.creds.get('password'),
                                      host=self.creds.get('host'),
                                      database=self.creds.get('database'),
                                      port=connection_port)
        cursor = cnx.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        cnx.close()
        return results

    def fetch_mod_peril_chars(self, product_name: str, start_timestamp: int, end_timestamp: int):
        """
        Retrieve all peril characteristics created by transactions committed in a given time period.
        :param product_name: name of the product
        :param start_timestamp: Unix epoch milliseconds
        :param end_timestamp: Unix epoch milliseconds
        :return: dictionary of results
        """
        return self._fetch_all_results_for_query(
            reports.lib.common.queries.get_transactions_query(product_name, start_timestamp, end_timestamp))

    def get_transactions_report(self, product_name: str, start_timestamp: int, end_timestamp: int):
        """
        Retrieve transactions report with delta values given in an intuitive way.
           1. New business: delta is the premium for the peril characteristic
           2. Reinstatement: delta is the premium for the peril characteristic
           3. Renewal: delta is the pc premium
           4. Cancellation: delta is the difference of (peril characteristic premium, replaced peril char premium)
           5. Endorsement:
                a. non-premium-bearing endorsement: delta is 0
                b. added coverage: delta is peril char premium
                c. removed coverage: delta is difference of (peril char premium, replaced char premium)
                d. updated overage: delta on pre-split is 0; post-split is
                    difference of ((sum of pre- and post-split), replaced char premium)
        :param product_name: product name
        :param start_timestamp: Unix epoch milliseconds
        :param end_timestamp: Unix epoch milliseconds
        :return: list of dicts, where each dict represents a row of data
        """
        mod_peril_chars = self.fetch_mod_peril_chars(product_name, start_timestamp, end_timestamp)

        # handle update and removal endorsements:
        #   if no other char with replacement_of, then is a removal and
        #         delta = peril char prem - replacement_of prem
        #   if other char has matching replacement_of, then is an update and
        #         delta pre-split = 0
        #         delta post-split = peril char prem - replacement_of prem

        # build a map of key-vals where
        #    key == replaced peril char locator
        #    value == array of { coverageStart, locator, premium } of replacing char(s)
        replaced_map = {}
        for mod_peril_char in mod_peril_chars:
            replaced_locator = mod_peril_char.get('replaced_locator')
            if not (self._is_endorsement_mod(mod_peril_char)
                    and (replaced_locator is not None)):
                continue

            entry = {'coverage_start': mod_peril_char.get('coverage_start'),
                     'premium': mod_peril_char.get('premium'),
                     'locator': mod_peril_char.get('peril_char_locator')}

            if replaced_map.get(replaced_locator) is None:
                replaced_map[replaced_locator] = [entry]
            else:
                replaced_map[replaced_locator].append(entry)

        # now adjust entries in modPerilChars
        for mod_peril_char in mod_peril_chars:
            replaced_locator = mod_peril_char.get('replaced_locator')
            if not (self._is_endorsement_mod(mod_peril_char)
                    and (replaced_locator is not None)):
                continue

            replaced_map_entries = replaced_map.get(replaced_locator)
            replaced_map_len = len(replaced_map_entries)
            if replaced_map_len == 1:  # one replacing char indicates removal or non-rated
                try:
                    mod_peril_char['premium_delta'] = mod_peril_char.get('premium') - mod_peril_char.get(
                        'replacement_of_premium')
                except TypeError:
                    print('missing expected premium values for peril characteristic %s or replacement' %
                          mod_peril_char.get('peril_char_locator'))
                    mod_peril_char['premium_delta'] = 0
            elif replaced_map_len == 2:  # two replacing chars indicate update
                coverage_starts = [v.get('coverage_start') for v in replaced_map_entries]
                coverage_starts.sort()
                if mod_peril_char.get('coverage_start') == coverage_starts[0]:
                    mod_peril_char['premium_delta'] = 0
                else:
                    mod_peril_char['premium_delta'] = sum(
                        [v.get('premium') for v in replaced_map_entries]) - mod_peril_char.get('replacement_of_premium')
            else:
                print('unexpected replace count for replaced %s; leaving vals unadjusted' % replaced_locator)

        return mod_peril_chars

    def write_transaction_financial_impact_report(self, product_name, start_timestamp, end_timestamp, report_file_path):
        results = reports.lib.common.get_flattened_fields.get_flattened_results(
            self.get_transactions_report(product_name, start_timestamp, end_timestamp))

        # since we're flattening all fields, we will omit the columns with raw field data
        fields_to_omit = ['policy_fields', 'exposure_fields', 'peril_fields']
        fields_to_write = [f for f in results[0].keys() if f not in fields_to_omit]

        with open(report_file_path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fields_to_write, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)

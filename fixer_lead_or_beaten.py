from FixerPerformancesGeneric import FixerPerformancesGeneric
import math
import numpy as np
from prettytable import PrettyTable


class FixerLeadOrBeaten(FixerPerformancesGeneric):
    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None, **kwargs):

        self.verbose = False

        # Set up state variables for current race

        self.new_data = new_data
        self.existing_data = existing_data
        self.current_race_id = race_id
        self.current_race_id_sql = None         # Passed in via kwargs
        self.current_race_id_sql_horse = None   # Passed in via kwargs

        self.current_consolidated_race_conditions = None
        self.current_source_race_conditions = None

        for key, arg in kwargs.items():
            setattr(self, key, arg)

        self.set_race_conditions()

        self.best_value = self.find_best_value()

        if self.verbose:
            self.print_race_info()
            if self.best_value:
                print(f'\nRecommended value to use: {self.best_value}')
                response = input(f'Use recommended value ({self.best_value})? Y/n ').lower()
                if response != 'n' and self.best_value != self.existing_data:
                    self.update_value(self.column_name, self.best_value)
                    print(f'Value updated.')
            else:
                print(f'\nCould not resolved discrepancy--no change recommendation')
            input('Press enter to continue')
        else:
            if self.best_value and self.best_value != self.existing_data:
                self.update_value(self.column_name, self.best_value)
            elif self.best_value is None and abs(self.new_data - self.existing_data) > 0.11:
                # Not reviewing very minor discrepancies at this point...
                # self.print_race_info()
                # input('Press enter to continue')
                pass

        return self.discrepancy_resolved()

    def discrepancy_resolved(self):
        if self.best_value is None:
            return False
        else:
            return True

    def print_discrepancy_table(self):

        def not_empty(value):
            if value is None or np.isnan(value):
                return False
            else:
                return True

        lead_or_beaten_fields = ['lead_or_beaten_0', 'lead_or_beaten_330', 'lead_or_beaten_440', 'lead_or_beaten_660',
                                 'lead_or_beaten_880', 'lead_or_beaten_990', 'lead_or_beaten_1100',
                                 'lead_or_beaten_1210', 'lead_or_beaten_1320', 'lead_or_beaten_1430',
                                 'lead_or_beaten_1540', 'lead_or_beaten_1610', 'lead_or_beaten_1650',
                                 'lead_or_beaten_1760', 'lead_or_beaten_1830', 'lead_or_beaten_1870',
                                 'lead_or_beaten_1980']

        consolidated_lead_or_beaten = self.consolidated_row_data[lead_or_beaten_fields]
        source_lead_or_beaten = self.source_row_data[lead_or_beaten_fields]
        lead_beaten_data = zip(lead_or_beaten_fields, consolidated_lead_or_beaten, source_lead_or_beaten)

        # Print info on current data under examination
        table = PrettyTable(['current field', 'consolidated data', 'new data'])
        table.add_row([self.column_name, self.existing_data, self.new_data])
        print(f'{table}')

        # Print all the horse's race info for context
        table = PrettyTable(['distance', 'consolidated data', 'new data'])
        for field, consolidated_value, source_value in lead_beaten_data:
            if not_empty(consolidated_value) or not_empty(source_value):
                table.add_row([field, consolidated_value, source_value])
        print(f'\n{table}')

    def find_best_value(self):
        """ Tries to find a best value for the lead_or_beaten mismatch.

            Generally prefers more precise values over less precise values and non-round values over round ones. If
            the two values being considered are different by more than one, we assume there's a data error, and
            get_best_value() will not provide a resolution for the discrepancy.

        """
        # If the values differ by more than one, there's an issue.
        if abs(self.existing_data - self.new_data > 1):
            return None

        # If one value is more precise than the other, use that one.
        new_data_precision = self.get_precision(self.new_data)
        existing_data_precision = self.get_precision(self.existing_data)
        if new_data_precision > existing_data_precision:
            return self.new_data
        elif existing_data_precision > new_data_precision:
            return self.existing_data

        # If one value is round and the other is non-round, use the non-round value
        new_data_round = self.is_round(self.new_data)
        existing_data_round = self.is_round(self.existing_data)
        if new_data_round and not existing_data_round:
            return self.existing_data
        elif existing_data_round and not new_data_round:
            return self.new_data

        # If they are the same precision and both are round or both are nonround, we'll assume there is a data
        # error and no resolution is found.
        return None

    def get_precision(self, num):
        max_digits = 14
        int_part = int(abs(num))
        magnitude = 1 if int_part == 0 else int(math.log10(int_part) + 1)
        fractional_part = abs(num) - int_part
        multiplier = 10 ** (max_digits - magnitude)
        fractional_digits = multiplier + int(multiplier * fractional_part + 0.5)
        while fractional_digits % 10 == 0:
            fractional_digits /= 10
        return int(math.log10(fractional_digits))

    def is_round(self, num):
        if (num % 1) % 0.5 == 0:
            return True
        else:
            return False


from FixerPerformancesGeneric import FixerPerformancesGeneric
import math
import numpy as np
from prettytable import PrettyTable


class FixerLeadOrBeaten(FixerPerformancesGeneric):
    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None, **kwargs):

        self.verbose = True

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

        if self.verbose:
            self.print_race_info()
            input('Press enter to continue')
        else:
            pass

        return self.discrepancy_resolved()

    def discrepancy_resolved(self):
        return False

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
        pass

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

    def find_highest_precision(self):
        # Returns the data item with the highest prevision, e.g., 1.43 is more precise than 1.
        # In the event of a tie in the precision, the existing data will be returned.
        # todo do something better when the precision matches but the values do not

        precision_new_data = self.get_precision(self.new_data)
        precision_existing_data = self.get_precision(self.existing_data)

        return self.new_data if precision_new_data > precision_existing_data else self.existing_data

    def fix_lead_or_beaten(self):
        # If the current data is zero, use the new data if it isn't also zero
        if self.existing_data == 0 and self.new_data != 0:
            self.consolidated_db.update_race_values([self.column_name], [self.new_data], self.current_race_id_sql_horse)
            if self.verbose: print(
                f'\nUsing new data for {self.column_name}. New data: {self.new_data}. Consolidated data: {self.existing_data}')

        # If they have the same precision but don't match, pick the one that's least "round" or keep existing data
        # if they are within 1 of each other
        elif (self.get_precision(self.new_data) == self.get_precision(self.existing_data) and self.new_data != self.existing_data):
            # If new data is round and existing data isn't, keep the existing data
            if (self.new_data % 1) % 0.5 == 0 and (self.existing_data % 1) % 0.5 != 0:
                if self.verbose: print(
                    f'Using least round data: {self.existing_data}. New data: {self.new_data}. Consolidated data: {self.existing_data}')
                self.unfixed_data[self.column_name].append(self.current_race_id)
            # if existing data is round and new data isn't, use the new data if it's within one; otherwise
            # keep the existing data and note the discrepancy in the log
            elif (self.new_data % 1) % 0.5 != 0 and (self.existing_data % 1) % 0.5 == 0:
                if abs(self.new_data - self.existing_data) < 1:
                    if self.verbose: print(
                        f'Using least round data: {self.new_data}. New data: {self.new_data}. Consolidated data: {self.existing_data}')
                    self.consolidated_db.update_race_values([self.column_name], [self.new_data],
                                                            self.get_current_race_id(as_sql=True))
                else:
                    if self.verbose: print(f'Data too far apart. New data: {self.new_data}. Consolidated data: {self.existing_data}')
                    self.unfixed_data[self.column_name].append(self.current_race_id)

        # If they're within 1 of eachoter (i.e., (abs(x- y) <= 1), pick the one with the highest precision.
        # Need to address when they are within 1 of each other and have the same precision
        elif abs(self.new_data - self.existing_data) < 1:
            best_value = self.find_highest_precision()
            self.consolidated_db.update_race_values([self.column_name], [best_value], self.get_current_race_id(as_sql=True))
            if self.verbose: print(
                f'\nUsing most precise value: {best_value} for {self.column_name}. New data: {self.new_data}. Consolidated data: {self.existing_data}')

        else:
            if self.verbose: print('Couldn\'t fix lead/beaten discrepancy:')
            self.unfixed_data[self.column_name].append(self.current_race_id)
        # todo Maybe add special handling if one of the values is zero and they aren't within 1 of each other
from fixer_generic import Fixer
from prettytable import PrettyTable
import re


class FixerDistance(Fixer):

    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None, **kwargs):

        # Set up state variables for current race
        self.new_data = new_data
        self.existing_data = existing_data
        self.current_race_id = race_id
        self.current_race_id_sql = None

        self.current_consolidated_race_conditions = None
        self.current_source_race_conditions = None

        for key, arg in kwargs.items():
            setattr(self, key, arg)

        self.set_race_conditions()

        try:
            otc = self.has_off_turf_condition(self.current_consolidated_race_conditions)
            otc = self.has_off_turf_condition(self.current_source_race_conditions)
        except:
            pass


        # Print out race information
        if full_info:
            print('Full info received')
            self.new_row_data, self.consolidated_row_data = full_info
            self.print_races_info()



    def print_races_info(self):
        # Fields to examine:
        review_fields = ['distance', 'off_turf_dist_change']
        source_fields = [self.source_table_structure[field] for field in review_fields]
        consolidated_fields = [field for field in review_fields]
        # Data from source and consolidated tables

        def get_value(value_dict, field):
            if field:
                return value_dict[field]
            else:
                return 'None'

        # Print out table with the review data
        table = PrettyTable(['field', 'New data', 'Existing data'])
        table.add_row([self.column_name, self.new_data, self.existing_data])
        print(table)
        del table

        self.off_turf_dist_change = self.consolidated_db.get_value('off_turf_dist_change',
                                                                   self.current_race_id_sql)

        if self.off_turf_dist_change:
            print(f'Off turk distance change flag: {self.off_turf_dist_change}')
        else:
            print(f'No off turf distance flag found')


        # Print race conditions

        print(f'Source race conditions: {self.current_source_race_conditions}\n')
        print(f'Consolidated race conditions: {self.current_consolidated_race_conditions}')

        self.print_entry_info(pause=True)

    def set_race_conditions(self):
        race_condition_fields = ['race_conditions_text_1', 'race_conditions_text_2', 'race_conditions_text_3',
                                 'race_conditions_text_4', 'race_conditions_text_5', 'race_conditions_text_6']
        source_race_condition_fields = [self.source_table_structure[key] for key in race_condition_fields]
        if not any(source_race_condition_fields):
            source_race_conditions = 'No race conditions'
        else:
            source_race_conditions = self.source_db.get_values(source_race_condition_fields, self.current_race_id_sql)
            for item in source_race_condition_fields:
                print(self.new_row_data[item])
                source_race_conditions += self.new_row_data[item] if self.new_row_data else ''

        consolidated_race_conditions = ''
        for item in self.consolidated_db.get_values(race_condition_fields, self.current_race_id_sql):
            if item:
                consolidated_race_conditions += item

        self.current_source_race_conditions = source_race_conditions
        self.current_consolidated_race_conditions = consolidated_race_conditions

    def has_off_turf_condition(self, race_conditions):
        off_turf_condition = re.search(r'\(if.+?\)', race_conditions.lower())
        if off_turf_condition:
            return True
        else:
            return False





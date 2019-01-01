from prettytable import PrettyTable
import re


class Fixer:

    def __init__(self, column_name, source_data_handler, consolidated_db_handler, verbose=False):
        self.column_name = column_name
        self.source_db = source_data_handler
        self.consolidated_db = consolidated_db_handler
        self.verbose = verbose

        self.new_data = None
        self.existing_data = None
        self.current_race_id = None
        self.new_row_data = None
        self.consolidated_row_data = None

        self.source_table_structure = self.source_db.get_table_structure()

        self.current_source_race_conditions = None
        self.current_consolidated_race_conditions = None

    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None):
        """ Primary function of class. Attempts to fix a discrepancy in the data for its assigned column.

            Needs implementation in the concrete, column-specific class
        """
        self.new_data = new_data
        self.existing_data = existing_data
        self.current_race_id = race_id
        if full_info:
            self.new_row_data, self.consolidated_row_data = full_info
        pass

    def update_value(self, field, value):
        self.consolidated_db.update_race_values([field], [value], self.current_race_id_sql)

    def set_race_conditions(self):
        race_condition_fields = ['race_conditions_text_1', 'race_conditions_text_2', 'race_conditions_text_3',
                                 'race_conditions_text_4', 'race_conditions_text_5', 'race_conditions_text_6']
        source_race_condition_fields = [self.source_table_structure[key] for key in race_condition_fields if self.source_table_structure[key]]
        if not any(source_race_condition_fields):
            source_race_conditions = None
        else:
            source_race_conditions = self.source_db.get_values(source_race_condition_fields, self.current_race_id_sql)
            text = ''
            for item in source_race_conditions:
                text += item if item else ''
            source_race_conditions = text

        consolidated_race_conditions = ''
        for item in self.consolidated_db.get_values(race_condition_fields, self.current_race_id_sql):
            if item:
                consolidated_race_conditions += item

        self.current_source_race_conditions = source_race_conditions
        self.current_consolidated_race_conditions = consolidated_race_conditions

    def print_race_info(self):
        print(f'\n\nCurrent race: {self.current_race_id}')
        print(f'Current source table: {self.source_db.table}')
        self.print_discrepancy_table()
        print(f'\nConsolidated race conditions: \n{self.current_consolidated_race_conditions}')
        print(f'\nSource race conditions:\n{self.current_source_race_conditions}')

    def print_discrepancy_table(self):
        table = PrettyTable(['field', 'New data', 'Existing data'])
        table.add_row([self.column_name, self.new_data, self.existing_data])
        print(f'{table}')

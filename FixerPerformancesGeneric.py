from fixer_generic import Fixer
from prettytable import PrettyTable


class FixerPerformancesGeneric(Fixer):

    def __init__(self, column_name, source_data_handler, consolidated_db_handler, consolidated_races_db, verbose=False):
        super().__init__(column_name, source_data_handler, consolidated_db_handler, verbose=verbose)

        self.consolidated_races_db = consolidated_races_db
        self.current_race_id_sql = None             # Set by fix_discrepancy()
        self.current_race_id_sql_horse = None       # Set by fix_discrepancy()
        self.source_row_data = None                 # Set by fix_discrepancy()
        self.consolidated_row_data = None           # Set by fix_discrepancy()

    def set_race_conditions(self):
        race_condition_fields = ['race_conditions_text_1', 'race_conditions_text_2', 'race_conditions_text_3',
                                 'race_conditions_text_4', 'race_conditions_text_5', 'race_conditions_text_6']
        consolidated_race_conditions = ''
        for item in self.consolidated_races_db.get_values(race_condition_fields, self.current_race_id_sql):
            if item:
                consolidated_race_conditions += item
        self.current_consolidated_race_conditions = consolidated_race_conditions

    def print_race_info(self, print_conditions=False):
        print(f'\n\nCurrent race: {self.current_race_id}')
        print(f'Current source table: {self.source_db.table}')
        self.print_discrepancy_table()
        if print_conditions:
            print(f'\nConsolidated race conditions: \n{self.current_consolidated_race_conditions}')
            print(f'\nSource race conditions:\n{self.current_source_race_conditions}')

    def print_discrepancy_table(self):
        table = PrettyTable(['field', 'New data', 'Existing data'])
        table.add_row([self.column_name, self.new_data, self.existing_data])
        print(f'{table}')

    def update_value(self, field, value):
        self.consolidated_db.update_race_values([field], [value], self.current_race_id_sql_horse)

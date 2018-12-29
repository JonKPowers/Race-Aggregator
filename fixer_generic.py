class Fixer:

    def __init__(self, column_name, source_data_handler, consolidated_db_handler):
        self.column_name = column_name
        self.source_db = source_data_handler
        self.consolidated_db = consolidated_db_handler

        self.new_data = None
        self.existing_data = None
        self.current_race_id = None
        self.new_row_data = None
        self.consolidated_row_data = None

        self.source_table_structure = self.source_db.get_table_structure()

    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None):
        """ Primary function of class. Attempts to fix a discrepancy in the data for its assigned column.

            Needs implementation in the concrete, column-specific class
        """
        self.new_data = new_data
        self.existing_data = existing_data
        self.current_race_id = race_id
        if full_info: self.new_row_data, self.consolidated_row_data = full_info
        pass

    def print_entry_info(self, pause=False):
        print(f'\nData mismatch {self.current_race_id}: {self.column_name}. New data: {self.new_data}. '
              f'Consolidated data: {self.existing_data}')


        if pause:
            input("Press enter to continue")


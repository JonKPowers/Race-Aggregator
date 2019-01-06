class Fixer:

    def __init__(self, column_name, source_data_handler, consolidated_db_handler, verbose=False):
        self.column_name = column_name
        self.source_db = source_data_handler
        self.consolidated_db = consolidated_db_handler
        self.verbose = verbose

        self.new_data = None
        self.existing_data = None
        self.current_race_id = None

        self.source_table_structure = self.source_db.get_table_structure()

        self.current_source_race_conditions = None          # Set by set_race_conditions()
        self.current_consolidated_race_conditions = None    # Set by set_race_conditions()

    def fix_discrepancy(self, new_data, existing_data, race_id=None):
        """ Primary function of class. Attempts to fix a discrepancy in the data for its assigned column.

            Needs implementation in the concrete, column-specific class
        """
        self.new_data = new_data
        self.existing_data = existing_data
        self.current_race_id = race_id
        raise NotImplementedError()

    def update_value(self, field, value):
        raise NotImplementedError()

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
        raise NotImplementedError()

    def print_discrepancy_table(self):
        raise NotImplementedError()

from fixer_generic import Fixer


class FixerHorseName(Fixer):

    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None, **kwargs):

        self.verbose = True

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

        self.best_name = self.get_best_name()

        if self.verbose:
            self.print_race_info()
            input('Press enter to continue')

            if self.best_name:
                print(f'Recommend using {self.best_name}')
                response = input(f'Update data to use {self.best_name}? Y/n ').lower()
                if response != 'n' and  self.best_name != self.existing_data:
                    self.update_value(self.column_name, self.best_name)
                    print('Existing data updated')
            else:
                print(f'No name suggestions found')




        return self.discrepancy_resolved()

    def get_best_name(self):
        if self.title_cases_match:
            return self.existing_data.title()

    def title_cases_match(self):
        if self.new_data.title() == self.existing_data.title():
            return True
        else:
            return None

    def discrepancy_resolved(self):
        print('\nReturning False')
        return False

    def set_race_conditions(self):
        race_condition_fields = ['race_conditions_text_1', 'race_conditions_text_2', 'race_conditions_text_3',
                                 'race_conditions_text_4', 'race_conditions_text_5', 'race_conditions_text_6']
        consolidated_race_conditions = ''
        for item in self.consolidated_races_db.get_values(race_condition_fields, self.current_race_id_sql):
            if item:
                consolidated_race_conditions += item
        self.current_consolidated_race_conditions = consolidated_race_conditions

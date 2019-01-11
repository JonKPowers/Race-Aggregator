
from FixerPerformancesGeneric import FixerPerformancesGeneric
from titlecase import titlecase


class FixerHorseName(FixerPerformancesGeneric):

    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None, **kwargs):

        self.verbose = False

        # Set up state variables for current race

        self.new_data = new_data
        self.existing_data = existing_data
        self.current_race_id = race_id
        self.current_race_id_sql = None             # Passed in via kwargs
        self.current_race_id_sql_horse = None       # Passed in via kwargs

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
                if response != 'n' and self.best_name != self.existing_data:
                    self.update_value(self.column_name, self.best_name)
                    print('Existing data updated')
            else:
                print(f'No name suggestions found')
        else:
            if self.best_name is not None and self.best_name != self.existing_data:
                self.update_value(self.column_name, self.best_name)
            elif self.best_name is None:
                self.print_race_info()
                input('Press enter to continue')

        return self.discrepancy_resolved()

    def get_best_name(self):
        if self.title_cases_match:
            return titlecase(self.existing_data)

    def title_cases_match(self):
        if titlecase(self.new_data) == titlecase(self.existing_data):
            return True
        else:
            return None

    def discrepancy_resolved(self):
        if self.best_name is not None:
            return True
        else:
            return False

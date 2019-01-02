from fixer_generic import Fixer
from prettytable import PrettyTable


class FixerRaceType(Fixer):

    race_types = {
        # 'N' is almost always on results_bris
        # Format: best_descriptor: [weaker, descriptors]
        'N': ['FNL', 'SPI'],            # other_stakes
        'G1': ['STK', 'N'],             # grade_1_stakes
        'G2': ['STK', 'CHM', 'N'],      # grade_2_stakes
        'G3': ['STK', 'N'],             # grade_3_stakes
        'STK': ['N'],                   # nongraded_stakes
        'HCP': ['A', 'N'],              # handicap
        'SHP': ['T', 'N'],              # starter handicap
        'ALW': ['A'],                   # allowance
        'AOC': ['AO'],                  # allowance_optional_claiming
        'STR': ['R'],                   # starter_allowance
        'SOC': ['CO', 'N'],             # starter_optional_claiming
        'CLM': ['C'],                   # claiming
        'WCL': ['C', 'N'],              # waiver_claiming
        'MSW': ['S', 'MDN', 'N'],       # maiden_special_weight
        'MOC': ['MO'],                  # maiden_optional_claiming
        'MCL': ['M'],                   # maiden_claiming
        'FUT': ['N'],                   # Futurity
        'DTR': ['N'],                   # Derby Trial
        # todo: FIX DUPE 'STR': ['N'],                   # Trials
        'FTR': ['N'],                   # Futurity Trials
        'FCN': ['N'],                   # Futurity Consolation
        'INS': ['N'],                   # Invitational Stakes
        'HDS': ['N'],                   # Handicap Stakes
        'TRL': ['N'],                   # Trials?

        # SPI--Speed Index Race
        # FNL--Final
    }

    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None, **kwargs):

        self.verbose = False

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

        self.best_race_descriptor = self.get_best_race_type()

        if self.verbose:
            self.print_race_info()
            if self.best_race_descriptor is not None:
                print(f'\nRecommended race descriptor: {self.best_race_descriptor}')
                if self.best_race_descriptor is not self.existing_data:
                    response = input(f'Update data to {self.best_race_descriptor}? Y/n' ).lower()
                    if response != 'n':
                        self.update_value(self.column_name, self.best_race_descriptor)
                        print(f'Data updated')
            input('Press enter to continue')
        else:
            if self.best_race_descriptor is not None and self.best_race_descriptor is not self.existing_data:
                self.update_value(self.column_name, self.best_race_descriptor)
            if self.best_race_descriptor is None:
                pass
        return self.discrepancy_resolved()

    def discrepancy_resolved(self):
        if self.best_race_descriptor is None:
            print('\nReturning false')
            return False
        else:
            return True

    def get_best_race_type(self):
        # If the best race descriptor is the new_data, use that
        if self.new_data in self.race_types and self.existing_data in self.race_types[self.new_data]:
            return self.new_data
        # If the best race_descriptor is the existing_data, use that.
        elif self.existing_data in self.race_types and self.new_data in self.race_types[self.existing_data]:
            return self.existing_data
        else:
            # Search through each value set to see if only second-best values are in the test data
            # 'N' is excluded because it spans multiple best-descriptor listings
            for key, value in self.race_types.items():
                if self.new_data in value and self.new_data.upper() != 'N':
                    return key
                elif self.existing_data in value and self.existing_data.upper() != 'N':
                    return key
            print(f'\nCould not find known race type in new data ({self.new_data}) or existing data ({self.existing_data})')
            self.print_race_info()
            return self.secondary_race_types()

    def secondary_race_types(self):
        if self.existing_data.upper() == 'SHP' and (self.pps_bris_race_type == 'T' or self.results_bris_race_type == 'T'):
            return 'SHP'
        else:
            return None

    def print_discrepancy_table(self):
        self.results_bris_race_type = self.consolidated_db.get_value('results_bris_race_type', self.current_race_id_sql)
        self.results_equibase_race_type = self.consolidated_db.get_value('results_equibase_race_type', self.current_race_id_sql)
        self.pps_bris_race_type = self.consolidated_db.get_value('pps_bris_race_type', self.current_race_id_sql)

        table = PrettyTable(['field', 'New data', 'Existing data'])
        table.add_row([self.column_name, self.new_data, self.existing_data])
        print(f'{table}')

        table = PrettyTable(['codes', 'Results Bris', 'PPs Bris', 'Results EQB'])
        table.add_row(['', self.results_bris_race_type, self.pps_bris_race_type, self.results_equibase_race_type])
        print(f'\n {table}')

    def get_type_from_conditions(self, race_conditions):
        pass

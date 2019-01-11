from FixerPerformancesGeneric import FixerPerformancesGeneric


class FixerDaysSinceLastRace(FixerPerformancesGeneric):
    def fix_discrepancy(self, new_data, existing_data, race_id=None):

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

        self.best_value = self.find_best_value()

        if self.verbose:
            self.print_race_info()
            input('Press enter to continue')
        else:
            pass


        return self.discrepancy_resolved()

    def discrepancy_resolved(self):
        return False

    def find_best_value(self):
        pass

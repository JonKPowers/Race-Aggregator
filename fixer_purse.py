from FixerRacesGeneric import FixerRacesGeneric

# todo make this do something
class FixerPurse(FixerRacesGeneric):
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

        if self.verbose:
            self.print_race_info()
            input('Press enter to continue')

        return self.discrepancy_resolved()

    def discrepancy_resolved(self):
        return False

from fixer_generic import Fixer
from prettytable import PrettyTable
import re


class FixerDistance(Fixer):

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

        # Check whether db has off turf and off turf dist change flag set
        self.off_turf_dist_change = self.consolidated_db.get_value('off_turf_dist_change',
                                                                   self.current_race_id_sql)
        self.off_turf_flag = self.consolidated_db.get_value('off_turf', self.current_race_id_sql)

        # Print out race information
        if full_info:
            self.new_row_data, self.consolidated_row_data = full_info
            if self.verbose: self.print_races_info()

        #todo Add in something to capture unresolved distance changes that aren't related to off_turf changes
        # get_distance_change will return None if no off_turf distance changes are detected; use that to branch off of

        # Look for a potential distance change in the race conditions and report if found
        self.distance_change = self.get_distance_change()
        if self.verbose: print(f'\nRace conditions distance change: {self.distance_change}')

        # Ask whether to update the distance
        # Will be done interactively if self.verbose == True; else automatically based on recommend_updates
        self.recommend_updates = False if self.distance_change is None else self.new_data == self.distance_change
        self.recommend_off_turf_flag_update = self.recommend_updates or (self.distance_change == self.existing_data and self.off_turf_flag != 1)
        self.recommend_off_turf_dist_change_flag_update = self.recommend_updates or (self.distance_change == self.existing_data and self.off_turf_dist_change != 1)
        if self.verbose:
            print(f'\nRecommend updating to new data' if self.recommend_updates or self.recommend_off_turf_flag_update or self.recommend_off_turf_dist_change_flag_update else f'\nNo update recommended')
            if not self.recommend_updates:
                input('Press enter to continue')
            if self.recommend_updates:
                update_response = input(f'Update value to {self.distance_change}? [Y/n]' ).lower()
                if update_response != 'n' and self.distance_change is not None:
                    self.update_value(self.column_name, self.distance_change)
                else:
                    print('No update made')
                    input('Press enter to continue')
            if self.recommend_off_turf_flag_update or self.recommend_off_turf_dist_change_flag_update:
                # Ask whether to update the off turf and distance change flag
                if self.off_turf_dist_change != 1 or self.off_turf_flag != 1:
                    print(f'\nRecommend setting off turf and off turf distance change flag')
                    update_response = input(f'Set off turf flags for {self.current_race_id}? [Y/n] ').lower()
                    if update_response != 'n':
                        if self.off_turf_dist_change != 1 :self.update_value('off_turf_dist_change', '1')
                        if self.off_turf_flag != 1: self.update_value('off_turf', '1')
        else:
            if self.recommend_updates:
                self.update_value(self.column_name, self.distance_change)
            if self.recommend_off_turf_dist_change_flag_update:
                self.update_value('off_turf_dist_change', '1')
            if self.recommend_off_turf_flag_update:
                self.update_value('off_turf', '1')

        return self.discrepancy_resolved()

        # todo add column indicating how much the distance changed if there was an off turf dist change

    def discrepancy_resolved(self):
        """Determines whether the discrepancy has been sufficiently resolved; used as return value for fix_discrepancy."""
        # If there's a discrepancy and distance change matches the existing data, we're good.
        if self.distance_change == self.existing_data:
            return True
        # If recommend_updates, i.e., if self.distance_change == self.new_data, we'll update the data and we're good
        elif self.recommend_updates:
            return True
        else:
            return False

    def print_races_info(self):
        print('\n' + self.current_race_id)
        # Print out table with the review data
        table = PrettyTable(['field', 'New data', 'Existing data'])
        table.add_row([self.column_name, self.new_data, self.existing_data])
        print(f'\n{table}')
        del table

        if self.off_turf_dist_change:
            print(f'Off turf distance change flag: {self.off_turf_dist_change}')
        else:
            print(f'No off turf distance flag found')

        # Print race conditions
        print(f'\nSource race conditions:\n{self.current_source_race_conditions}')
        print(f'\nConsolidated race conditions:\n{self.current_consolidated_race_conditions}')

    def has_off_turf_condition(self, race_conditions):
        if race_conditions is None: return False

        off_turf_condition = re.search(r'\(if.+?\)', race_conditions.lower())
        if off_turf_condition:
            return True
        else:
            return False

    def get_distance_change(self):
        source_distance_change = None
        consolidated_distance_change = None

        if self.current_source_race_conditions:
            result = self.extract_changed_distance(self.current_source_race_conditions)
            unit_type, source_distance_change = result if result is not None else (None, None)
            source_distance_change = self.convert_to_int_distance(unit_type, source_distance_change)

        if self.current_consolidated_race_conditions:
            result = self.extract_changed_distance(self.current_consolidated_race_conditions)
            unit_type, consolidated_distance_change = result if result is not None else (None, None)
            consolidated_distance_change = self.convert_to_int_distance(unit_type, consolidated_distance_change)

        if source_distance_change and consolidated_distance_change:
            if source_distance_change == consolidated_distance_change:
                return source_distance_change
            elif self.verbose:
                print(f'Source and existing conditions show different distance changes.')
                print(f'\tSource: {source_distance_change}\n\tConsolidated:{consolidated_distance_change}')
        elif source_distance_change is None is consolidated_distance_change:
            if self.verbose: print(f'Distance change not found in source or consolidated')
            return None
        elif source_distance_change is None:
            return consolidated_distance_change
        elif consolidated_distance_change is None:
            return source_distance_change

    def convert_to_int_distance(self, unit_type, distance_match):

        if distance_match is None: return None

        num_conversions = {
            'one': 1,
            'two': 2,
            'three': 3,
            'four': 4,
            'five': 5,
            'six': 6,
            'seven': 7,
            'eight': 8,
            'nine': 9,
            'ten': 10,
            '1': 1,
            '2': 2,
            '3': 3,
            '4': 4,
            '5': 5,
            '6': 6,
            '7': 7,
            '8': 8,
            '9': 9,
            '10': 10,
        }

        if unit_type == 'mile':
            mile_dist_in_yards = 1760 if re.search(r'1|one', distance_match.group(2)) else 3520
            fractional_part = distance_match.group(3)
            fractional_dist_in_yards = None
            if fractional_part is None:
                fractional_dist_in_yards = 0
            elif re.search(r'70|seventy', fractional_part):
                fractional_dist_in_yards = 70
            elif re.search(r'40|forty', fractional_part):
                fractional_dist_in_yards = 40
            elif re.search(r'sixteenth|16', fractional_part):
                fractional_dist_in_yards = 110
            elif re.search(r'eighth|8', fractional_part):
                fractional_dist_in_yards = 220
            elif re.search(r'half|1/2', fractional_part):
                fractional_dist_in_yards = 880
            else:
                print(f'Error finding fractional mile distance for {fractional_part}')
                input('Press enter to continue')
            return mile_dist_in_yards + fractional_dist_in_yards
            # todo Add a TypeError catch in case it tries to add an int and None

        if unit_type == 'furlong':
            number_of_furlongs = distance_match.group(2)
            number_of_furlongs = num_conversions[number_of_furlongs]
            furlong_dist_in_yards = number_of_furlongs * 220
            fractional_part = distance_match.group(3)
            fractional_dist_in_yards = None
            if fractional_part is None:
                fractional_dist_in_yards = 0
            elif re.search(r'one half|onehalf|a half|1/2', fractional_part):
                fractional_dist_in_yards = 110
            else:
                print(f'Error finding fractional furlong distance for {fractional_part}')
                input('Press enter to continue')
            return furlong_dist_in_yards + fractional_dist_in_yards

    def extract_changed_distance(self, race_conditions):
        triggers_string = r'(if ?deemed ?inadvisable|if ?necessary|if ?deemed ?inadvisable|' \
                          r'if ?management ?deems ?it|if ?the ?stewards ?consider ?it|if ?stewards ?consider ?it|' \
                          r'if ?the ?management ?considers ?it|if ?management ?considers ?it|if ?transferred ?to|' \
                          r'if ?this ?race ?is ?taken|if ?the ?race ?is ?taken|in ?the ?event ?this ?race|' \
                          r'in ?the ?event ?that ?this ?race|if ?this ?race ?is ?taken|if ?this ?race ?comes ?off)'

        multipart_string = r'(miles?|furlongs).+?(miles?|furlongs)'
        multipart_search_string = re.compile(r'{}.+{}'.format(triggers_string, multipart_string))
        multipart_result = re.search(multipart_search_string, race_conditions.lower())
        if multipart_result:
            if self.verbose: print(f'Multipart race condition found: \n{race_conditions}')
            return None

        numbers = r'([1-9]|one|two|three|four|five|six|seven|eight|nine|ten)'
        fractions = r'(one half|onehalf|a half|1/2)'
        full_furlong_string = r'{}( ?and {})? furlongs?'.format(numbers, fractions)
        furlong_search_string = re.compile(r'{}.+?{}'.format(triggers_string, full_furlong_string))

        furlong_result = re.search(furlong_search_string, race_conditions.lower())
        if furlong_result:
            return ('furlong', furlong_result)

        mile_string = r'(one mile|onemile|1mile|1 mile)'
        fractional_mile_string = r'(one sixteenth|1 sixteenth|1/16|one eighth|1 eighth|1/8|' \
                                 r'seventy yards|70 yards|forty yards|40 yards)'
        full_mile_string=r'{}( ?and {})?'.format(mile_string, fractional_mile_string)
        mile_search_string = re.compile(r'{}.+?{}'.format(triggers_string, full_mile_string))

        mile_result = re.search(mile_search_string, race_conditions.lower())
        if mile_result:
            return ('mile', mile_result)


        # Multipart phrases
        #   "If Deemed inadvisable by management to run this race over the turf course; it will be run on the main track at One Mile. If the race is for two year olds; it will be run at Seven Furlongs",
        #   'If the Stewards consider it inadvisable to run this race on the turf course; Two - Year - Old races will be run at Seven Furlongs and races for Three - Year - Olds and Up will be run at One Mileand One Eighth on the main track.',

        # todo chute-related phrases:
        # 'One mile out of the chute.'






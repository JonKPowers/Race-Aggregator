from fixer_generic import Fixer
from prettytable import PrettyTable
import re


class FixerDistance(Fixer):

    def fix_discrepancy(self, new_data, existing_data, race_id=None, full_info=None, **kwargs):

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

        # Print out race information
        if full_info:
            self.new_row_data, self.consolidated_row_data = full_info
            self.print_races_info()

        # Look for a potential distance change in the race conditions and report if found
        distance_change = self.get_distance_change()
        print(f'Race conditions distance change: {distance_change}')
        input('Press enter to continue')

    def print_races_info(self):
        # Fields to examine:
        review_fields = ['distance', 'off_turf_dist_change']
        source_fields = [self.source_table_structure[field] for field in review_fields]
        consolidated_fields = [field for field in review_fields]
        # Data from source and consolidated tables

        def get_value(value_dict, field):
            if field:
                return value_dict[field]
            else:
                return 'None'

        # Print out table with the review data
        table = PrettyTable(['field', 'New data', 'Existing data'])
        table.add_row([self.column_name, self.new_data, self.existing_data])
        print(f'\n{table}')
        del table

        self.off_turf_dist_change = self.consolidated_db.get_value('off_turf_dist_change',
                                                                   self.current_race_id_sql)

        if self.off_turf_dist_change:
            print(f'Off turf distance change flag: {self.off_turf_dist_change}')
        else:
            print(f'No off turf distance flag found')


        # Print race conditions

        print(f'\nSource race conditions: {self.current_source_race_conditions}')
        print(f'Consolidated race conditions: {self.current_consolidated_race_conditions}')

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
            else:
                print(f'Source and existing conditions show different distance changes.')
                print(f'\tSource: {source_distance_change}\n\tConsolidated:{consolidated_distance_change}')
        elif source_distance_change is None is consolidated_distance_change:
            print(f'Distance change not found in source or consolidated')
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
        #
        #
        #
        #
        #
        # number_string = distance_match.group(2)
        # number = num_conversions[number_string]
        # units = distance_match.group(3)
        # unit_distance = None
        # distance = None
        #
        # if units in ['furlong', 'furlongs']:
        #     unit_distance = 220
        # elif units in ['mile', 'miles']:
        #     unit_distance = 1760
        # else:
        #     print(f'Unrecognized unit: {units}')
        #     input('Press enter to continue')
        #
        # try:
        #     distance = number * unit_distance
        # except TypeError as e:
        #     print(f'Error converting distance to int: {e}')
        #     input('Press enter to continue')
        #
        # return distance

    def extract_changed_distance(self, race_conditions):
        triggers_string = r'(if deemed inadvisable|if necessary|if deemed inadvisable|' \
                          r'if management deems it necessary|if the stewards consider it|if stewards consider it|' \
                          r'if the management considers it|if management considers it|if transferred to|' \
                          r'if this race is taken|if the race is taken|in the event this race|if this race is taken|' \
                          r'if this race comes off)'

        multipart_string = r'(miles?|furlongs).+?(miles?|furlongs)'
        multipart_search_string = re.compile(r'{}.+{}'.format(triggers_string, multipart_string))
        multipart_result = re.search(multipart_search_string, race_conditions.lower())
        if multipart_result:
            print(f'Multipart race condition found: {race_conditions}')
            input('Press enter to continue')
            return None

        numbers = r'([1-9]|one|two|three|four|five|six|seven|eight|nine|ten)'
        fractions = r'(one half|onehalf|a half|1/2)'
        full_furlong_string = r'{}( ?and {})? furlongs?'.format(numbers, fractions)
        furlong_search_string = re.compile(r'{}.+?{}'.format(triggers_string, full_furlong_string))

        furlong_result = re.search(furlong_search_string, race_conditions.lower())
        if furlong_result:
            return ('furlong', furlong_result)

        mile_string = r'(one mile|onemile|1mile|1 mile)'
        fractional_mile_string = r'(one sixteenth|1 sixteenth|1/16|one eighth|1 eighth|1/8|seventy yards|70 yards)'
        full_mile_string=r'{}( ?and {})?'.format(mile_string, fractional_mile_string)
        mile_search_string = re.compile(r'{}.+?{}'.format(triggers_string, full_mile_string))

        mile_result = re.search(mile_search_string, race_conditions.lower())
        if mile_result:
            return ('mile', mile_result)


        # Multipart phrases
        #   "If Deemed inadvisable by management to run this race over the turf course; it will be run on the main track at One Mile. If the race is for two year olds; it will be run at Seven Furlongs",
        #   'If the Stewards consider it inadvisable to run this race on the turf course; Two - Year - Old races will be run at Seven Furlongs and races for Three - Year - Olds and Up will be run at One Mileand One Eighth on the main track.',






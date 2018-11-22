import logging
import db_handler_persistent as dbh

import numpy as np
import pandas as pd
import datetime
import re
from progress.bar import Bar

from aggregation_AdderDataHandler import AdderDataHandler
from aggregation_RaceProcessor import RaceProcessor


class AggRacesDataHandler(AdderDataHandler):
    def build_dataframe(self):
        # get_race_data() returns a dataframe containing information from the target table that
        # will be aggregated into the consolidated table.

        # It first generates a dictionary with [consolidated_field_name]: [target_table_field_name]
        # as the key/value pair. It then splits these up into parallel lists of consolidated and target table
        # field names. The target table fields are used to query the db, and the consolidated field names
        # are used as column headers in the resulting data frame.

        # Because python does not guarantee the order that dictionary entries will be presented,
        # we have to extract the [consolidate]:[target] pairs together and then split them up. If order
        # were guaranteed, we could just use dict.values() and dict.keys() to get the field lists.

        table_index = self.constants.TABLE_TO_INDEX_MAPPINGS[self.table]
        field_dict = {key: value[table_index] for key, value in self.constants.CONSOLIDATED_TABLE_STRUCTURE.items()
                      if value[table_index]}
        fields = [(key, value) for key, value in field_dict.items()]
        source_fields = [item for _, item in fields]
        consolidated_fields = [item for item, _ in fields]

        # Query the db and return the results as a Pandas dataframe

        sql_query = self.db.generate_query(self.table, source_fields, other=self.other)
        db_data = self.db.query_db(sql_query)
        self.data = pd.DataFrame(db_data, columns=consolidated_fields)


class RaceAggregator(RaceProcessor):
    # The RaceAggregator class is intended to aggregate data regarding races that have been run, such as track, date,
    # distance, times, winner, etc. into the consolidated races table. It's primary method is
    # add_to_consolidated_data().

    def __init__(self, db_handler, db_consolidated_handler, db_consolidated_races_handler=None, include_horse=False,
                 verbose=False):
        self.db = db_handler
        self.consolidated_db = db_consolidated_handler
        self.consolidated_races_db = db_consolidated_races_handler

        self.table = db_handler.table
        self.table_index = self.db.constants.TABLE_TO_INDEX_MAPPINGS[self.table]

        # Variable to control whether horse information is part of the source table
        self.include_horse = include_horse

        # Variable to control how much information is printed
        self.verbose = verbose

        # State-tracking variables
        current_race_id = None
        current_date = None
        current_track = None
        current_race_num = None
        current_distance = None

        # Set up dict to track the unresolvable issues that were found
        self.unfixed_data = {}

    def add_to_consolidated_data(self):
        # Setup progress bar
        print(f"Consolidating data from table {self.table}")
        bar = Bar(f'Processing {self.table} data', max=len(self.db.data), suffix='%(percent).3f%% - %(index)d/%(max)d - %(eta)s secs.')

        # Generate a list of the columns to check by pulling a row from the dataframe and extracting the
        # column names (this will be a pandas index since the resulting row is returned as a pandas series
        # with the column names serving as the index). Then we strip off the non-race_id columns from that list
        # and set up the issue-tracking dictionary.

        dummy_row = self.db.data.iloc[0]
        columns = dummy_row.index.tolist()
        del dummy_row

        race_id_fields = ['date', 'track', 'race_num', 'horse_name'] if self.include_horse \
            else ['date', 'track', 'race_num']
        columns_to_check = [item for item in columns if item not in race_id_fields]

        # Create dict keys for the conflict-tracking dictionary
        for column in columns_to_check:
            self.unfixed_data[column] = list()
        self.unfixed_data['other'] = list()
        try:
            del column
        except Exception as e:
            print(f'Issue deleting column variable: {e}')


        # Loop through each row of dataframe and process that race info
        for i in range(len(self.db.data)):
            # Advance progress bar
            bar.next()
            # Set state with current race information
            self.set_current_info(i)

            # Pull the data for the row we're working on, which will be needed either to update/checl
            # values for the existing data or to add a new entry to the table..
            row_data = self.db.data.iloc[i]

            # Check if race is in the consolidated db; if not, add the race to the db.
            # The race is added in exception handling, which will be triggered if the race lookup
            # in the consolidated dataframe fails.

            try:
                self.consolidated_db.data.loc[self.get_current_race_id(include_horse=self.include_horse)]



                # Check if all the non-race_id fields are blank; if so, add our data to the entry.
                if self.consolidated_db.fields_blank(self.get_current_race_id(), columns_to_check, number='all'):
                    self.consolidated_db.update_race_values(columns_to_check,
                                                            row_data[columns_to_check].tolist(),
                                                            self.get_current_race_id(as_sql=True))
                else:   # Resolve partial data
                        # Generate boolean masks for what data is missing in consolidated and new data
                    new_row_data = row_data[columns_to_check]
                    consolidated_data = self.consolidated_db.data.loc[self.get_current_race_id(include_horse=self.include_horse), columns_to_check]

                    missing_row_data = [self.db.is_blank(item) for item in new_row_data]
                    missing_consolidated_data = [self.db.is_blank(item) for item in consolidated_data]

                    # Check to make sure the row sizes match, which we expect
                    # TO-DO TAKE THIS OUT FOR PRODUCTION
                    assert len(missing_row_data) == len(missing_consolidated_data)

                    # If there's an entry that already has data in it, compare each data entry, see where
                    # discrepancies are, resolve them, and then update the consolidated db entry.
                    self.resolve_data(zip(missing_row_data, missing_consolidated_data),
                                      zip(new_row_data, consolidated_data),
                                      columns_to_check)
                # If some of the non-race_id fields are not blank, we have to resolve those against our new data

            except KeyError:    # Add the race if there isn't already an entry in the consolidated db
                self.consolidated_db.add_blank_entry(self.get_current_race_id(as_tuple=True, include_horse=self.include_horse),
                                                     include_horse=self.include_horse)
                self.consolidated_db.update_race_values(columns,
                                                        row_data.tolist(),
                                                        self.get_current_race_id(as_sql=True, include_horse=self.include_horse))

        with open(f'logs/unfixed_data_{self.table} {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}.txt', 'w') as file:
            for key in self.unfixed_data.keys():
                file.write(f'{key}:\t')
                for item in self.unfixed_data[key]:
                    file.write(f'{item}, ')
                file.write('\n')

        bar.finish()

    def reconcile_discrepancy(self, new_data, existing_data, column):
        # Skip any columns that we want to ignore discrepancies for
        keys_to_ignore= ['source_file', 'race_conditions_text_1', 'race_conditions_text_2', 'race_conditions_text_3',
                         'race_conditions_text_4', 'race_conditions_text_5', 'race_conditions_text_6',]
        if column in keys_to_ignore: return

        def print_mismatch(pause=False):
            print(f'\nData mismatch {self.current_race_id}: {column}. New data: {new_data}. Consolidated data: {existing_data}')
            if pause: input("Press enter to continue")

        def update_to_new_data():
            self.consolidated_db.update_race_values([column], [new_data], self.get_current_race_id(as_sql=True))

        def add_to_unfixed_data():
            self.unfixed_data[column].append(self.current_race_id)

        def distances():
            # If we find a discrepancy in the distances, delete the race and note the race_id in the tracking
            # dictionary. There probably isn't a simple way to resolve these discrepancies without manually going
            # through and working out what is driving the issue. Further research may reveal patterns in the
            # discrepancies that we can code a solution to.
            self.unfixed_data['distance'].append(self.current_race_id)
            # self.consolidated_db.delete_entry(self.get_current_race_id(as_tuple=True))

        def fix_race_type():
            # todo Change to equibase race types, which are more descriptive and varied; prefer those over race_info:
            # fix_it_dict = {
            #     # Format: fix_name: race_general_results_type, race_info_type, equibase_race_type, replacement_value
            #     'SOC_fix': ['N', 'CO', 'SOC', 'SOC'],
            #     'WCL_fix': ['N', 'C', 'WCL', 'WCL'],
            #     'MDT_fix': ['S', 'N', 'MDT', 'MDT'],
            #     'STR_fix': ['R', 'N', 'STR', 'STR'],
            #     'HCP_fix': ['A', 'N', 'HCP', 'HCP'],
            # }
            # Types of mismatches:
            # Race info: N. Horse PPS: C
            # Race info: N. Horse PPS: CO
            if (new_data in ['C', 'CO'] and existing_data == 'N') or (new_data == 'N' and existing_data in ['C', 'CO']):
                race_set_as_claiming = existing_data in ['C', 'CO']
                race_is_claiming_race = self.consolidated_db.data.loc[self.current_race_id, 'claiming_price_base'] != 0
                if race_is_claiming_race and not race_set_as_claiming:
                    if self.verbose: print('\nUpdating db to mark race as claiming race')
                    update_to_new_data()
                elif not race_is_claiming_race and race_set_as_claiming:
                    if self.verbose: print('\nRace incorrectly set as claiming... fixing')
                    update_to_new_data()
            # todo Race info: N. Horse PPS: S
            # todo N and A combos, S and N, R and N

            else:
                if self.verbose: print_mismatch()
                add_to_unfixed_data()

        def fix_surface():
            # Most of the surface discrepancies stem from changes from a planned turf race to dirt or all weather
            # surfaces. todo Work out a way to check whether there was a planned change that matches the observed discrepancy
            if (existing_data in ['T', 't'] and new_data in ['D', 'd', 'A']) or (new_data in ['T', 't'] and existing_data in ['D', 'd', 'A']):
                # Assume that there was a move to the dirt or all weather surface, and set the value to that.
                # todo is this a good assumption? Can we cross check with a switch flag?
                if self.verbose: print('\nLikely move from planned turf race')
                if existing_data in ['T', 't']: update_to_new_data()
            elif existing_data in ['T', 't'] and new_data in ['T', 't']:
                # For our purposes turf is turf, regardless of whether it's inner or outer turf.
                pass
            elif existing_data in ['A', 'D', 'd'] and new_data in ['A', 'D', 'd']:
                # todo Something
                pass
            else:
                print_mismatch(pause=True)
                add_to_unfixed_data()
            # todo: [T/t]

        # Run the appropriate discrepancy resolver depending on the column involved.
        if column == 'distance':
            print_mismatch(pause=True)
        elif column == 'race_type':
            fix_race_type()
        elif column == 'surface':
            fix_surface()
        elif column == 'claiming_price_base': add_to_unfixed_data()
        elif column == 'track_condition': add_to_unfixed_data()
        elif column == 'purse': add_to_unfixed_data()
        elif column in ['allowed_colts_geldings', 'allowed_mares', 'allowed_fillies', ]:
            add_to_unfixed_data()
        elif column == 'statebred_race': add_to_unfixed_data()
        elif column == 'field_size': add_to_unfixed_data()
        elif column in ['allowed_age_two', 'allowed_age_three', 'allowed_age_four', 'allowed_age_five',
                        'allowed_age_older']:
            add_to_unfixed_data()
        elif column == 'breed': add_to_unfixed_data()
        elif column == 'race_conditions_1_not_won_limit': add_to_unfixed_data()
        elif column == 'race_conditions_1_time_limit': add_to_unfixed_data()

        else:
            print('Other type of discrepancy')
            self.unfixed_data['other'].append(self.current_race_id)
            print(f'Data mismatch: {column}. New data: {new_data}. Consolidated data: {existing_data}')
            print('')














class Trash:
    def __init__(self):
        self.errata_table = 'aggregation_notes'
        self.errata_table_structure = {
            'notes_on_data': ('TEXT',),
            'looks_like_bad_data': ('TINYINT',),
        }
        self.errata_table_structure.update(self.consolidated_table_structure)

        # Initialize tables
        unique = ['track', 'date', 'race_num']

        consolidated_races_dtypes = {key: value[0] for key, value in self.consolidated_table_structure.items()}
        self.db_consolidated_races.initialize_table(self.consolidated_table, consolidated_races_dtypes,
                                                    unique_key=unique, foreign_key=None)

        errata_dtypes = {key: value[0] for key, value in self.errata_table_structure.items()}
        self.db_errata.initialize_table(self.errata_table, errata_dtypes, unique_key=unique, foreign_key=None)

    def fix_race_type(self, db_handler, race_info_type, race_general_results_type, mismatch_category, track, date,
                      race_num):
        fix_it_dict = {
            # Format: fix_name: race_general_results_type, race_info_type, equibase_race_type, replacement_value
            'SOC_fix': ['N', 'CO', 'SOC', 'SOC'],
            'WCL_fix': ['N', 'C', 'WCL', 'WCL'],
            'MDT_fix': ['S', 'N', 'MDT', 'MDT'],
            'STR_fix': ['R', 'N', 'STR', 'STR'],
            'HCP_fix': ['A', 'N', 'HCP', 'HCP'],
        }

        # Dict for items to ignore b/c they've already been fixed
        already_fixed_dict = {key: [value[2], value[1]] for key, value in fix_it_dict.items()}

        equibase_race_type = self.get_single_race_value(self.db_horses_data, 'race_general_results',
                                                        'race_type_equibase',
                                                        track, date, race_num)
        print(f'race_general_results data: {race_general_results_type}')
        print(f'race_info data: {race_info_type}')
        print(f'equibase_race_type: {equibase_race_type}')

        for fix in fix_it_dict:
            values = fix_it_dict[fix]
            if race_general_results_type == values[0] and race_info_type == values[1] and equibase_race_type == \
                    values[2]:
                self.update_single_race_value(db_handler, 'horses_consolidated_races', mismatch_category,
                                              values[3], track, date, race_num)
                self.add_blank_race_entry(self.db_errata, 'aggregation_notes', track, date, race_num)
                self.update_single_race_value(self.db_errata, 'aggregation_notes', mismatch_category,
                                              values[3], track, date, race_num)
                return 1

        for fixed in already_fixed_dict:
            values = already_fixed_dict[fixed]
            if race_general_results_type == values[0] and race_info_type == values[1]:
                print('No change needed--already processed')
                return 1

        return 0

    def prompt_for_user_correction_input(self, key, race):
        print('Unable to fix this issue.')
        user_input = input('(s)kip this mismatch category/mark as (b)ad/'
                           'add (n)ote/(e)nter new value/(q)uit/(C)ontinue: ').lower()
        if user_input == 'q':
            return user_input
        elif user_input == 's':
            return user_input
        elif user_input == 'b':
            self.add_blank_race_entry(self.db_errata, 'aggregation_notes', *race)
            self.update_single_race_value(self.db_errata,
                                          'aggregation_notes',
                                          'looks_like_bad_data',
                                          '1',
                                          *race)
        elif user_input == 'n':
            note = input('Enter note: ')
            self.add_blank_race_entry(self.db_errata, 'aggregation_notes', *race)
            old_note = self.get_single_race_value(self.db_errata,
                                                  'aggregation_notes',
                                                  'notes_on_data',
                                                  *race,
                                                  no_table_mapping=True)
            self.update_single_race_value(self.db_errata,
                                          'aggregation_notes',
                                          'notes_on_data',
                                          (str(old_note) + ' NEW NOTE: ' + note).strip(),
                                          *race)
        elif user_input == 'e':
            new_value = input('Enter new value: ')
            self.add_blank_race_entry(self.db_errata, 'aggregation_notes', *race)
            self.update_single_race_value(self.db_consolidated_races,
                                          'horses_consolidated_races',
                                          key,
                                          new_value,
                                          *race)
            self.update_single_race_value(self.db_errata, 'aggregation_notes', key, new_value, *race)


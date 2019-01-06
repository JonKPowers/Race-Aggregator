import logging
from aggregation_RaceProcessor import RaceProcessor
from AdderDataHandler import AdderDataHandler

from progress.bar import Bar
import numpy as np
import pandas as pd
import datetime
import math

from fixer_horse_name import FixerHorseName
from fixer_lead_or_beaten import FixerLeadOrBeaten



class PPAdderDataHandler(AdderDataHandler):
    def dummy(self):
        pass


class PPRaceProcessor(RaceProcessor):

    def __init__(self, db_handler, db_consolidated_handler, db_consolidated_races_handler, include_horse=False, verbose=False):
        self.db = db_handler
        self.consolidated_db = db_consolidated_handler
        self.consolidated_races_db = db_consolidated_races_handler

        self.table = db_handler.table
        self.table_index = self.db.constants.TABLE_TO_INDEX_MAPPINGS[self.table]

        self.verbose = verbose
        self.verbose_print = print if verbose else lambda *args, **kwargs: None

        # Variable to control whether horse information is part of the source table
        self.include_horse = include_horse

        # State-tracking variables
        current_race_id = None
        current_date = None
        current_track = None
        current_race_num = None
        current_horse = None
        current_distance = None

        # Set up dict to track the unresolvable issues that were found
        self.unfixed_data = {}

        # Set up discrepancy resolvers:
        # todo use
        self.fixers = {
            'horse_name': FixerHorseName('horse_name', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_0': FixerLeadOrBeaten('lead_or_beaten_0', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_330': FixerLeadOrBeaten('lead_or_beaten_330', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_440': FixerLeadOrBeaten('lead_or_beaten_440', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_660': FixerLeadOrBeaten('lead_or_beaten_660', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_880': FixerLeadOrBeaten('lead_or_beaten_880', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_990': FixerLeadOrBeaten('lead_or_beaten_990', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1100': FixerLeadOrBeaten('lead_or_beaten_1100', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1210': FixerLeadOrBeaten('lead_or_beaten_1210', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1320': FixerLeadOrBeaten('lead_or_beaten_1320', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1430': FixerLeadOrBeaten('lead_or_beaten_1430', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1540': FixerLeadOrBeaten('lead_or_beaten_1540', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1610': FixerLeadOrBeaten('lead_or_beaten_1610', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1650': FixerLeadOrBeaten('lead_or_beaten_1650', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1760': FixerLeadOrBeaten('lead_or_beaten_1760', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1830': FixerLeadOrBeaten('lead_or_beaten_1830', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1870': FixerLeadOrBeaten('lead_or_beaten_1870', self.db, self.consolidated_db, self.consolidated_races_db),
            'lead_or_beaten_1980': FixerLeadOrBeaten('lead_or_beaten_1980', self.db, self.consolidated_db, self.consolidated_races_db),
        }

    def add_to_consolidated_data(self):
        # Setup progress bar
        print(f'Consolidating data from {self.table}')
        bar = Bar(f'Processing {self.table} data', max=len(self.db.data),
                  suffix='%(percent).3f%% - %(index)d/%(max)d - %(eta)s secs.')

        # Generate a list of the columns to check by pulling a row from the dataframe and extracting the
        # column names (this will be a pandas index since the resulting row is returned as a pandas series
        # with the column names serving as the index). Then we strip off the non-race_id columns from that list
        # and set up the issue-tracking dictionary.

        # Set up log for unresolved discrepancies
        columns = self.consolidated_db.constants.CONSOLIDATED_TABLE_STRUCTURE.keys()
        race_id_fields = ['date', 'track', 'race_num']
        columns_to_check = [item for item in columns if item not in race_id_fields]
        self.set_up_issue_log(columns_to_check)

        # Loop through each row of dataframe and process that race info
        for i in range(len(self.db.data)):
            # Advance progress bar
            bar.next()
            # Set state with current race information
            self.set_current_info(i)

            # Check that race_distance is in distances_to_process list; if not, skip the entry
            # todo Add to list of distances that can be processed.
            # todo confirm that the distance resolver is run before adding performances to get calls right
            ###############
            # NEED TO DO SOMETHING BETTER ABOUT THIS DISTANCE CHECKING; IT REQUIRES THAT
            # THE CONSOLIDATED TABLE BE SEEDED WITH A TABLE CONTAINING DISTANCE INFO--TIGHT COUPLING
            #
            distance = self.get_race_distance()
            if distance is None or distance not in self.db.constants.DISTANCES_TO_PROCESS:
                continue

            # Use the dataHandler to pull the race data for the current race and generate column list
            # todo Seems like there has to be a better way to do this rather than generate a new distance-specific
            # todo list for each row of the dataframe
            row_data = self.db.get_trimmed_row_data(i, distance)
            columns = row_data.index.tolist()

            # Check if there is an entry in the consolidated db for this race; if not, add it.
            if self.race_entry_exists(self.get_current_race_id(include_horse=self.include_horse)):

                self.verbose_print(f'Race {self.current_race_id} found--checking for discrepancies')

                # Check if all the non-race_id fields are blank; if so, add our data to the entry.
                if self.consolidated_db.fields_blank(self.current_race_id, columns, number='all'):
                    self.verbose_print('All consolidated fields found blank; adding data')
                    self.consolidated_db.update_race_values(columns,
                                                            row_data[columns].tolist(),
                                                            self.get_current_race_id(as_sql=True, include_horse=self.include_horse))

                # If some of the non-race_id fields are not blank, we have to resolve those against our new data
                else:   # Resolve partial data
                        # Generate boolean masks for what data is missing in consolidated and new data
                    self.new_row_data = row_data[columns]
                    self.consolidated_row_data = self.consolidated_db.data.loc[self.get_current_race_id(include_horse=self.include_horse), columns]

                    missing_row_data = [self.db.is_blank(item) for item in self.new_row_data]
                    missing_consolidated_data = [self.db.is_blank(item) for item in self.consolidated_row_data]

                    # Check to make sure the row sizes match, which we expect
                    # todo TAKE THIS OUT FOR PRODUCTION
                    assert len(missing_row_data) == len(missing_consolidated_data)

                    # If there's an entry that already has data in it, compare each data entry, see where
                    # discrepancies are, resolve them, and then update the consolidated db entry.
                    self.resolve_data(zip(missing_row_data, missing_consolidated_data),
                                      zip(self.new_row_data, self.consolidated_row_data),
                                      columns)

            else:    # Add the race if there isn't already an entry in the consolidated db
                # todo Figure out a race-adding mechanism that doesn't use update. Will overwrite entries if the
                # todo reference dataframe isn't updated after adding new entries.
                self.verbose_print(f'Race {self.current_race_id} not found--adding to db')

                self.consolidated_db.add_blank_entry(self.get_current_race_id(as_tuple=True, include_horse=self.include_horse),
                                                     include_horse=self.include_horse)
                self.consolidated_db.update_race_values(columns,
                                                        row_data.tolist(),
                                                        self.get_current_race_id(as_sql=True, include_horse=self.include_horse))

        with open(f'logs/performances_unfixed_data_{self.table} {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}.txt', 'w') as file:
            for key in self.unfixed_data.keys():
                file.write(f'\n**********\n{key} ({len(self.unfixed_data[key])}):\t')
                for item in self.unfixed_data[key]:
                    file.write(f'{item}, ')
                file.write('\n')
        bar.finish()

    def race_entry_exists(self, race_id):
        """Checks whether the consolidated dataframe has an entry for a given race"""
        try:
            self.consolidated_db.data.loc[race_id]  # Throws a KeyError if no entry is in the dataframe.
            return True
        except KeyError:
            return False

    def get_race_distance(self):
        """Tries to find distance for a given race. Returns distance if found, otherwise None."""
        race_id = self.get_current_race_id(include_horse=False)
        race_id_with_horse = self.get_current_race_id(include_horse=True)
        try:
            distance = self.consolidated_races_db.data.loc[race_id, 'distance']
            return distance
        except KeyError:
            try:
                distance = self.db.data.loc[race_id_with_horse, 'distance']
                return distance
            except KeyError:
                self.verbose_print(f'No race distance info found for {self.current_race_id}')
                return None

    def set_up_issue_log(self, columns):
        # Create dict keys for the conflict-tracking dictionary
        for column in columns:
            self.unfixed_data[column] = list()
        self.unfixed_data['other'] = list()
        try: # Clean up unused variable
            del column
        except Exception as e:
            print(f'Issue deleting column variable: {e}')


    def reconcile_discrepancy(self, new_data, existing_data, column):
        # Skip any columns that we want to ignore discrepancies for
        keys_to_ignore = []
        if column in keys_to_ignore: return

        def add_to_unfixed_data():
            self.unfixed_data[column].append(self.current_race_id)

        def distances():
            # If we find a discrepancy in the distances, delete the race and note the race_id in the tracking
            # dictionary. There probably isn't a simple way to resolve these discrepancies without manually going
            # through and working out what is driving the issue. Further research may reveal patterns in the
            # discrepancies that we can code a solution to.
            pass

        def print_mismatch(pause=False):
            print(f'\nData mismatch{self.current_race_id}: {column}. New data: {new_data}. Consolidated data: {existing_data}')
            if pause: input("Press enter to continue")



        def fix_jockey_name():
            # Most of these issues involve the name being truncated or abbreviated. This method will prefer
            # the jockey name that is longest, on the premise that it will contain the most information.
            # todo Maybe add in some checks to make sure that the strings are reasonably similar

            # Rudimentary check to see if the jockey name starts with the same first letter... not that robust
            if existing_data[0] != new_data[0]: return
            elif len(existing_data) >= len(new_data):
                print(f'Keeping longest name: {existing_data}. New data: {new_data}. Existing data: {existing_data}')
            elif len(new_data) > len(existing_data):
                print(f'Keeping longest name: {new_data}. New data: {new_data}. Existing data: {existing_data}')
                self.consolidated_db.update_race_values([column], [new_data], self.get_current_race_id(as_sql=True))


        try:
            fixer_kwargs = {
                'current_race_id_sql' : self.get_current_race_id(as_sql=True),
                'current_race_id_sql_horse': self.get_current_race_id(as_sql=True, include_horse=True),
                'source_row_data': self.new_row_data,
                'consolidated_row_data': self.consolidated_row_data,
            }

            # Run the appropriate discrepancy resolver depending on the column involved.
            if column == 'distance':
                # print(f'Discrepancy is in {column} column.');
                self.unfixed_data['distance'].append(self.current_race_id)
            elif column == 'horse_name':
                discrepancy_resolved = self.fixers[column].fix_discrepancy(new_data, existing_data,
                                                                           race_id=self.current_race_id,
                                                                           **fixer_kwargs)
                if not discrepancy_resolved:
                    add_to_unfixed_data()
            elif column == 'source_file': add_to_unfixed_data()
            elif column == 'race_type': add_to_unfixed_data()
            elif column == 'days_since_last_race': add_to_unfixed_data()
            elif column == 'favorite': add_to_unfixed_data()
            elif column == 'horse_id': add_to_unfixed_data()
            elif column == 'weight': add_to_unfixed_data()
            elif column == 'state_bred': add_to_unfixed_data()
            elif column == 'post_position': add_to_unfixed_data()
            elif column == 'dead_heat_finish': add_to_unfixed_data()
            elif column in ['position_0', 'position_330', 'position_440', 'position_660', 'position_880',
                            'position_990', 'position_1100', 'position_1210', 'position_1320', 'position_1430',
                            'position_1540', 'position_1610', 'position_1650', 'position_1760', 'position_1830',
                            'position_1870', 'position_1980']: add_to_unfixed_data()

            elif column in ['equip_blinkers', 'equip_front_bandages', 'equip_bar_shoe', 'equip_no_shoes',
                            'meds_bute', 'equip_screens', 'meds_lasix', 'equip_cheek_piece',
                            'equip_mud_calks', 'meds_adjunct_bleeder', 'equip_no_whip',
                            'equip_shields', 'equip_goggles']: add_to_unfixed_data()

            elif column in ['jockey', 'trainer']: add_to_unfixed_data()
                # print_mismatch()
                # fix_jockey_name()
            elif column in ['jockey_id', 'trainer_id']:
                self.unfixed_data['dead_heat_finish'].append(self.current_race_id)
                # print_mismatch()
                # self.unfixed_data[column].append(self.current_race_id)
            ##########
            # Lead or beaten fields
            ##########
            elif column in ['lead_or_beaten_0', 'lead_or_beaten_330', 'lead_or_beaten_440', 'lead_or_beaten_660',
                            'lead_or_beaten_880', 'lead_or_beaten_990', 'lead_or_beaten_1100', 'lead_or_beaten_1210',
                            'lead_or_beaten_1320', 'lead_or_beaten_1430', 'lead_or_beaten_1540', 'lead_or_beaten_1610',
                            'lead_or_beaten_1650', 'lead_or_beaten_1760', 'lead_or_beaten_1830', 'lead_or_beaten_1870',
                            'lead_or_beaten_1980']:
                discrepancy_resolved = self.fixers[column].fix_discrepancy(new_data, existing_data,
                                                                           race_id=self.current_race_id,
                                                                           **fixer_kwargs)
                if not discrepancy_resolved:
                    add_to_unfixed_data()
            else:
                self.unfixed_data['other'].append(f'{column}/{self.current_race_id}')
                print('Other type of discrepancy')
                print(f'\nData mismatch: {column}. New data: {new_data}. Consolidated data: {existing_data}')
                print('')
        except KeyError as e:
            print(f'KeyError: {e}')

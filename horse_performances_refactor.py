import logging
from aggregation_RaceProcessor import RaceProcessor
from aggregation_AdderDataHandler import AdderDataHandler

from progress.bar import Bar
import numpy as np
import pandas as pd
import datetime
import math



class PPAdderDataHandler(AdderDataHandler):
    def dummy(self):
        pass


class PPRaceProcessor(RaceProcessor):
    def add_to_consolidated_data(self):
        # Setup progress bar
        print("Consolidating data")
        bar = Bar(f'Processing {self.table} data', max=len(self.db.data),
                  suffix='%(percent).3f%% - %(index)d/%(max)d - %(eta)s secs.')

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
        issue_columns = [self.db.constants.CONSOLIDATED_TABLE_STRUCTURE[key][1] for key in self.db.constants.CONSOLIDATED_TABLE_STRUCTURE.keys()]
        for column in issue_columns:
            self.unfixed_data[column] = list()
        self.unfixed_data['Other'] = list()
        try: # Clean up unused variable
            del column
        except Exception as e:
            print(f'Issue deleting column variable: {e}')

        # Loop through each row of dataframe and process that race info
        for i in range(len(self.db.data)):
            # Advance progress bar
            bar.next()
            # Set state with current race information
            self.set_current_info(i)

            # Check that race_distance is in distances_to_process list; if not, skip the entry
            ###############
            # NEED TO DO SOMETHING BETTER ABOUT THIS DISTANCE CHECKING; IT REQUIRES THAT
            # THE CONSOLIDATED TABLE BE SEEDED WITH A TABLE CONTAINING DISTANCE INFO--TIGHT COUPLING
            #

            try:
                distance = self.consolidated_races_db.data.loc[self.get_current_race_id(include_horse=False), 'distance']
            except KeyError:
                try:
                    distance = self.db.data.loc[self.get_current_race_id(include_horse=self.include_horse), 'distance']
                except KeyError:
                    # print(f'No race distance info found for {self.current_race_id}')
                    continue
            if distance not in self.db.constants.DISTANCES_TO_PROCESS: continue

            # Use the dataHandler to pull the race data for the current race and generate column list
            row_data = self.db.get_trimmed_row_data(i, distance)
            columns = row_data.index.tolist()


            try:    # Check if there is an entry in the consolidated db for this race; if not, add it.

                # This will throw an exception if there is no entry in the consolidated table. The race info is
                # added by the exception handler.
                self.consolidated_db.data.loc[self.get_current_race_id(include_horse=self.include_horse)]
                if self.verbose: print(f'Race {self.current_race_id} found--checking for discrepancies')

                # If we've gotten this far, there is an entry.
                # We'll only be checking whether the non-race_id fields are blank, so generate a list of those
                # non-race_id columns
                race_id_fields = ['source_file', 'date', 'track', 'race_num', 'horse_name'] if self.include_horse \
                    else ['source_file', 'date', 'track','race_num']
                columns_to_check = [item for item in columns if item not in race_id_fields]

                # Check if the non-race_id fields are blank; if so, add our data to the entry.
                if self.consolidated_db.fields_blank(self.current_race_id, columns_to_check, number='all'):
                    if self.verbose: print('All consolidated fields found blank; adding data')
                    # Add all our data (other than the race_id fields, which must already be in there)
                    self.consolidated_db.update_race_values(columns_to_check,
                                                            row_data[columns_to_check].tolist(),
                                                            self.get_current_race_id(as_sql=True,
                                                                                        include_horse=self.include_horse))
                # If some of the non-race_id fields are not blank, we have to resolve those against our new data
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

            except KeyError:    # Add the race if there isn't already an entry in the consolidated db
                if self.verbose: print(f'Race {self.current_race_id} not found--adding to db')
                self.consolidated_db.add_blank_entry(self.get_current_race_id(as_tuple=True, include_horse=self.include_horse),
                                                     include_horse=self.include_horse)
                self.consolidated_db.update_race_values(columns,
                                                        row_data.tolist(),
                                                        self.get_current_race_id(as_sql=True, include_horse=self.include_horse))

        with open(f'logs/unfixed_data_{self.table} {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}.txt', 'w') as file:
            for key in self.unfixed_data.keys():
                file.write(f'\n**********\n{key}:\t')
                for item in self.unfixed_data[key]:
                    file.write(f'{item}, ')
                file.write('\n')
        bar.finish()

    def reconcile_discrepancy(self, new_data, existing_data, column):
        # Skip any columns that we want to ignore discrepancies for
        keys_to_ignore = []
        if column in keys_to_ignore: return

        def distances():
            # If we find a discrepancy in the distances, delete the race and note the race_id in the tracking
            # dictionary. There probably isn't a simple way to resolve these discrepancies without manually going
            # through and working out what is driving the issue. Further research may reveal patterns in the
            # discrepancies that we can code a solution to.
            pass

        def print_mismatch(pause=False):
            print(f'\nData mismatch{self.current_race_id}: {column}. New data: {new_data}. Consolidated data: {existing_data}')
            if pause: input("Press enter to continue")

        def get_precision(num):
            max_digits = 14
            int_part = int(abs(num))
            magnitude = 1 if int_part == 0 else int(math.log10(int_part) + 1)
            fractional_part = abs(num) - int_part
            multiplier = 10 ** (max_digits - magnitude)
            fractional_digits = multiplier + int(multiplier * fractional_part + 0.5)
            while fractional_digits % 10 == 0:
                fractional_digits /= 10
            return int(math.log10(fractional_digits))

        def find_highest_precision():
            # Returns the data item with the highest prevision, e.g., 1.43 is more precise than 1.
            # In the event of a tie in the precision, the existing data will be returned.
            # todo do something better when the precision matches but the values do not

            return new_data if get_precision(new_data) > get_precision(existing_data) else existing_data

        def fix_lead_or_beaten():
            # If the current data is zero, use the new data if it isn't also zero
            if existing_data == 0 and new_data != 0:
                self.consolidated_db.update_race_values([column], [new_data], self.get_current_race_id(as_sql=True))
                if self.verbose: print(f'\nUsing new data for {column}. New data: {new_data}. Consolidated data: {existing_data}')

            # If they have the same precision but don't match, pick the one that's least "round" or keep existing data
            # if they are within 1 of each other
            elif (get_precision(new_data) == get_precision(existing_data) and new_data != existing_data):
                # If new data is round and existing data isn't, keep the existing data
                if (new_data % 1) % 0.5 == 0 and (existing_data % 1) % 0.5 != 0:
                    if self.verbose: print(f'Using least round data: {existing_data}. New data: {new_data}. Consolidated data: {existing_data}')
                    self.unfixed_data[column].append(self.current_race_id)
                # if existing data is round and new data isn't, use the new data if it's within one; otherwise
                # keep the existing data and note the discrepancy in the log
                elif (new_data % 1) % 0.5 != 0 and (existing_data % 1) % 0.5 == 0:
                    if abs(new_data - existing_data) < 1:
                        if self.verbose: print(f'Using least round data: {new_data}. New data: {new_data}. Consolidated data: {existing_data}')
                        self.consolidated_db.update_race_values([column], [new_data],
                                                                self.get_current_race_id(as_sql=True))
                    else:
                        if self.verbose: print(f'Data too far apart. New data: {new_data}. Consolidated data: {existing_data}')
                        self.unfixed_data[column].append(self.current_race_id)

            # If they're within 1 of eachoter (i.e., (abs(x- y) <= 1), pick the one with the highest precision. Need to address when they are within 1 of each other and have the same precision
            elif abs(new_data - existing_data) < 1:
                best_value = find_highest_precision()
                self.consolidated_db.update_race_values([column], [best_value], self.get_current_race_id(as_sql=True))
                if self.verbose: print(f'\nUsing most precise value: {best_value} for {column}. New data: {new_data}. Consolidated data: {existing_data}')

            else:
                if self.verbose: print('Couldn\'t fix lead/beaten discrepancy:')
                if self.verbose: print_mismatch()
                self.unfixed_data[column].append(self.current_race_id)
            # todo Maybe add special handling if one of the values is zero and they aren't within 1 of each other

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
            # Run the appropriate discrepancy resolver depending on the column involved.
            if column == 'distance':
                # print(f'Discrepancy is in {column} column.');
                self.unfixed_data['distance'].append(self.current_race_id)
            elif column == 'source_file':
                self.unfixed_data['source_file'].append(self.current_race_id)
            elif column == 'race_type':
                self.unfixed_data['race_type'].append(self.current_race_id)
            elif column == 'days_since_last_race':
                self.unfixed_data['days_since_last_race'].append(self.current_race_id)
            elif column == 'favorite':
                self.unfixed_data['favorite'].append(self.current_race_id)
            elif column == 'horse_id':
                self.unfixed_data['horse_id'].append(self.current_race_id)
            elif column == 'weight':
                self.unfixed_data['weight'].append(self.current_race_id)
            elif column == 'state_bred':
                self.unfixed_data['state_bred'].append(self.current_race_id)
            elif column == 'post_position':
                self.unfixed_data['post_position'].append(self.current_race_id)
            elif column == 'dead_heat_finish':
                self.unfixed_data['dead_heat_finish'].append(self.current_race_id)

            elif column in ['position_0', 'position_330', 'position_440', 'position_660', 'position_880',
                            'position_990', 'position_1100', 'position_1210', 'position_1320', 'position_1430',
                            'position_1540', 'position_1610', 'position_1650', 'position_1760', 'position_1830',
                            'position_1870', 'position_1980']:
                self.unfixed_data[column].append(self.current_race_id)

            elif column in ['equip_blinkers', 'equip_front_bandages', 'equip_bar_shoe', 'equip_no_shoes',
                            'meds_bute', 'equip_screens', 'meds_lasix']:
                self.unfixed_data[column].append(self.current_race_id)

            elif column in ['jockey', 'trainer']:
                print_mismatch()
                fix_jockey_name()
            elif column in ['jockey_id', 'trainer_id']:
                print_mismatch()
                self.unfixed_data[column].append(self.current_race_id)
            ##########
            # Lead or beaten fields
            ##########
            elif column in ['lead_or_beaten_0', 'lead_or_beaten_330', 'lead_or_beaten_440', 'lead_or_beaten_660',
                            'lead_or_beaten_880', 'lead_or_beaten_990', 'lead_or_beaten_1100', 'lead_or_beaten_1210',
                            'lead_or_beaten_1320', 'lead_or_beaten_1430', 'lead_or_beaten_1540', 'lead_or_beaten_1610',
                            'lead_or_beaten_1650', 'lead_or_beaten_1760', 'lead_or_beaten_1830', 'lead_or_beaten_1870',
                            'lead_or_beaten_1980']:
                fix_lead_or_beaten()

            else:
                if column not in self.unfixed_data['Other']: self.unfixed_data['Other'].append(column)
                print('Other type of discrepancy')
                print(f'\nData mismatch: {column}. New data: {new_data}. Consolidated data: {existing_data}')
                print('')
        except KeyError as e:
            print(f'KeyError: {e}')

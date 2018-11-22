from progress.bar import Bar

class RaceProcessor:
    # The RaceProcessor class is intended to process the race or past performance data and aggregate it into
    # consolidated tables. It's primary method is add_to_consolidated_data().

    def __init__(self, db_handler, db_consolidated_handler, db_consolidated_races_handler, include_horse=False, verbose=False):
        self.db = db_handler
        self.consolidated_db = db_consolidated_handler
        self.consolidated_races_db = db_consolidated_races_handler

        self.table = db_handler.table
        self.table_index = self.db.constants.TABLE_TO_INDEX_MAPPINGS[self.table]

        self.verbose = verbose

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

    def add_to_consolidated_data(self):
        # Setup progress bar
        print("Consolidating data")
        bar = Bar(f'Processing {self.table} data', max=len(self.db.data),
                  suffix='%(percent).3f%% - %(index)d/%(max)d - %(eta)s secs.')

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
                distance = self.consolidated_races_db.data.loc[self.get_current_race_id(include_horse=False),
                                                               'distance']
            except KeyError:
                try:
                    distance = self.db.data.loc[self.get_current_race_id(include_horse=self.include_horse), 'distance']
                except KeyError:
                    self.unfixed_data['distance_missing'].append(self.current_race_id)
                    print(f'No race distance info found for {self.current_race_id}')
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

        with open(f'unfixed_data_{self.table}.txt', 'w') as file:
            for key in self.unfixed_data.keys():
                file.write(f'{key}:\t')
                for item in self.unfixed_data[key]:
                    file.write(f'{item}, ')
                file.write('\n')
        bar.finish()

    def set_current_info(self, i):

        date_col = self.db.data.columns.get_loc('date')
        track_col = self.db.data.columns.get_loc('track')
        race_num_col = self.db.data.columns.get_loc('race_num')
        horse_name_col = self.db.data.columns.get_loc('horse_name') if self.include_horse else None

        self.current_date = self.db.data.iloc[i, date_col]
        self.current_track = self.db.data.iloc[i, track_col]
        self.current_race_num = self.db.data.iloc[i, race_num_col]
        self.current_horse = self.db.data.iloc[i, horse_name_col] if self.include_horse else None

        self.current_race_id = self.get_current_race_id(include_horse=self.include_horse)

    def get_current_race_id(self, include_horse=False, as_sql=False, as_tuple=False):
        if as_sql:
            race_id = f'track=\'{self.current_track}\' ' \
                      f'AND date=\'{self.current_date}\' ' \
                      f'AND race_num=\'{self.current_race_num}\''
            if include_horse: race_id += f' AND horse_name=\'{self.current_horse}\''
        elif as_tuple:
            if include_horse:
                race_id= (str(self.current_date), str(self.current_track), str(self.current_race_num), str(self.current_horse))
            else:
                race_id = (str(self.current_date), str(self.current_track), str(self.current_race_num))
        else:
            race_id = str(self.current_date) + str(self.current_track) + str(self.current_race_num)
            if include_horse: race_id += str(self.current_horse).upper()
        return race_id

    def resolve_data(self, zipped_masks, zipped_data, columns):
        # resolve_data() compares the data from the consolidated pp table to the new data to see what, if any,
        # information needs to be added or updated. It takes two zip objects. The first contains boolean masks
        # for each set of data indicating whether that data field is blank in the consolidated data or new data, as
        # applicable. The second contains the actual rows of data themselves.
        #
        # The resolution process follows the following basic logic:
        #   If data is missing from both sources, do nothing
        #   If data is missing from new data but not consolidated, do nothing
        #   If data is missing from the consolidated table but not the new data, insert the new data
        #   If there is data in both sources:
        #       (a) See if the data match; if so, do nothing.
        #       (b) If they don't match, reach out to helper methods to resolve the discrepancy
        #
        #   new data is in the first position in the zip objects and the consolidated data is in the second position

        global_columns_to_ignore = ['source_file', 'race_conditions_text_1', 'race_conditions_text_2',
                                    'race_conditions_text_3', 'race_conditions_text_4', 'race_conditions_text_5',
                                    'race_conditions_text_6', ]

        for mask, data, column in zip(zipped_masks, zipped_data, columns):
            if column in global_columns_to_ignore: continue
            if mask[0] == True == mask[1]:              # If both sources are missing data, do nothing and move on
                continue
            elif mask[0] == True and mask[1] == False:    # If data is only missing from new data, leave the
                continue                                # consolidated data alone.
            elif mask[0] == False and mask[1] == True:    # If the consolidated data is missing a value that the new data
                                                        # has, then insert the new data
                self.consolidated_db.update_race_values([column], [data[0]], self.get_current_race_id(as_sql=True))
            elif mask[0] == False == mask[1]:             # If they both have data:
                if data[0] == data[1]:                  # If the data matches, move on.
                    continue
                else:                                   # if it doesn't match, try to resolve the discrepancy.
                    self.reconcile_discrepancy(data[0], data[1], column)

    def reconcile_discrepancy(self, new_data, existing_data, column):
        pass
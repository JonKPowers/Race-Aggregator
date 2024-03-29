import logging

import numpy as np
import pandas as pd
import datetime

import db_handler as dbh

class AdderDataHandler:
    # The AdderDBHandler class is intended to provide a general interface to work with the database in the process
    # of adding and consolidating race information from various source types, such as results and PP files.

    def __init__(self, db_name, table_name, data_pack, include_horse=False, other=None,
                 initialize_db= False, initialize_table=False, verbose_db=False, verbose=False):
        # Attach configuration constants
        self.constants = data_pack
        self.verbose = verbose
        # Set up the database handler and connect to the database
        self.db = dbh.QueryDB(db_name, initialize_db=initialize_db, verbose=verbose_db)
        self.db.connect()
        self.table = table_name
        try:
            self.table_index = self.constants.TABLE_TO_INDEX_MAPPINGS[self.table]
        except KeyError:
            print(f'Table index not found for {self.table}')
            self.table_index = 0

        # Variable to hold the db info dataframe generated by self.build_dataframe()
        self.data = None

        # Variable to control whether horse name is included in race_id construction
        self.include_horse = include_horse

        # Variable to limit SQL entries retrieved during development
        self.other = other

        # Initalize the table is specified
        if initialize_table: self.initialize_table()

        # Go ahead and attach data to this data handler
        self.set_up_data()

    def set_up_data(self):
        self.build_dataframe()
        self.add_race_ids()

    def initialize_table(self):
        unique = self.constants.UNIQUE
        schema = {key: value[0] for key, value in self.constants.CONSOLIDATED_TABLE_STRUCTURE.items()}
        self.db.initialize_table(self.table, schema, unique_key=unique, foreign_key=None)

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

        if self.table == 'horses_consolidated_races':
            source_fields = ['date', 'track', 'race_num', 'distance']
            consolidated_fields = ['date', 'track', 'race_num', 'distance']
        else:
            table_index = self.constants.TABLE_TO_INDEX_MAPPINGS[self.table]
            field_dict = {key: value[table_index] for key, value in self.constants.CONSOLIDATED_TABLE_STRUCTURE.items()
                          if value[table_index]}
            extra_fields = {key: value[table_index] for key, value in self.constants.ADDITIONAL_FIELDS.items()
                            if value[table_index]}
            field_dict.update(extra_fields)
            fields = [(key, value) for key, value in field_dict.items()]
            source_fields = [item for _, item in fields]
            consolidated_fields = [item for item, _ in fields]

        # Query the db and return the results as a Pandas dataframe

        sql_query = self.db.generate_query(self.table, source_fields, other=self.other)
        db_data = self.db.query_db(sql_query)
        self.data = pd.DataFrame(db_data, columns=consolidated_fields)

    def add_race_ids(self):
        # add_race_ids() is intended to generate a unique string for each row in the self.data
        # Pandas dataframe, which is used as an index for the dataframe.

        # Zip up date, track, and race_num for races in the df being processed.
        # For horses_consolidated_data, don't include a horse name (because there isn't one in that table)
        race_id_data = zip(self.data['date'], self.data['track'], self.data['race_num'], self.data['horse_name']) \
            if self.include_horse else zip(self.data['date'], self.data['track'], self.data['race_num'])

        print(f'Adding race ids to {self.table}')

        # Concatenate the zipped fields, add them as a column to the df, and set that as the df's index
        if self.include_horse:
            race_ids = [str(item[0]) + str(item[1]) + str(item[2]) + str(item[3]).upper() for item in race_id_data]
        else:
            race_ids = [str(item[0]) + str(item[1]) + str(item[2]) for item in race_id_data]

        self.data['race_id'] = race_ids
        self.data.set_index('race_id', inplace=True)

    def get_race_data(self):
        self.build_dataframe()
        self.add_race_ids()

    def get_trimmed_row_data(self, i, distance):
        # get_trimmed_row_data() returns a single row of race data from the dataframe with unused columns dropped,
        # and with columns renamed with field names appropriate for entry into the consolidated db.

        def generate_drop_cols():
            # generate_drop_cols() returns a list of the columns that need to be dropped from a
            # row of data based on the distance of the race represented by that row.
            # The domain of columns that could be dropped is ADDITIONAL FIELDS, which contains
            # fields that are specific to the source-data format and will not necessarily
            # be common to all data sources.
            #
            # The need for this process comes is driven by the fact that the source data provides
            # times and lengths based on the call rather than the distance of that call. In order to aggregate data
            # that provides that info at different calls, we need to translate those calls into distances and then
            # use those distances when aggregating the data.

            additional_cols = [key for key in self.constants.ADDITIONAL_FIELDS if self.constants.ADDITIONAL_FIELDS[key][self.table_index] is not None]
            position_cols = [key for key in get_position_mapping().keys()]
            margin_cols = [key for key in get_margin_mapping().keys()]
            drop_columns = [col for col in additional_cols if col not in position_cols and col not in margin_cols]

            return drop_columns

        def get_position_mapping():
            mapping = {value[self.table_index]: key for key, value in self.constants.POSITION_DISTANCE_MAPPINGS[distance].items()}
            return mapping

        def get_margin_mapping():
            mapping = {value[self.table_index]: key for key, value in self.constants.LEAD_OR_BEATEN_DISTANCE_MAPPINGS[distance].items()}
            return mapping

        # Get the list of columns that need to be dropped from the row
        drop_columns = generate_drop_cols()

        # Pull row data and set appropriate position/margin column names for distance
        row_data = self.data.iloc[i]
        row_data = row_data.rename(get_position_mapping())
        row_data = row_data.rename(get_margin_mapping())

        # Drop unused columns that conflict with consolidated table schema
        row_data = row_data.drop(drop_columns)

        # return the row data and the columns
        return row_data

    def fields_blank(self,  race_id, fields, number='all'):
        data = self.data.loc[race_id, fields]
        missing_items = [self.is_blank(item) for item in data]
        if number == 'all':
            return True if all(missing_items) else False
        else:
            return True if any(missing_items) else False

    def add_blank_entry(self, *race_id, include_horse=False):
        # add_blank_entry() adds a new blank entry into the consolidated_pp_db with barebones information:
        # the race_id: the date, the track, the race number, and the horse name.
        # Additional information will be added to the entry by other methods, but this will provide
        # a base point and a race_id to use for the WHERE portion of the SQL query.

        columns = ['date', 'track', 'race_num', 'horse_name'] if include_horse else ['date', 'track', 'race_num']
        self.db.add_to_table(self.table, [*race_id], columns )

    def update_race_values(self, fields, values, race_id_sql):
        sql = self.db.generate_update_query(self.table, fields, values, where=race_id_sql)
        self.db.update_db(sql)

    def delete_entry(self, race_id):
        columns = ['date', 'track', 'race_num', 'horse_name'] if self.include_horse else ['date', 'track', 'race_num']
        self.db.delete_from_table(self.table, columns, race_id)
        if self.verbose: print(f'Deleted {race_id} from database')

    def is_blank(self, item):
        # item_is_blank() is intended to determine whether a particular entry in a dataframe is empty
        # for purposes of aggregating the race/performances data from various sources.

        if item == None:                    # An item is "blank" if:
            return True                     # (1) it's value is None
                                            # (2) it's NaN (isnan() will throw a TypeError if the input is str or date)
        elif type(item) != str and not isinstance(item, datetime.date) and np.isnan(item):
            return True
        elif type(item) == str and item == '':
            return True                     # (3) it's a blank string
        else:
            return False

        # NOTE: Old code had this in it, and we probably do need to cover datetimes, but this doesn't seem like
        # the right approach:
        #elif type(item) !=str and not isinstance(item, datetime.date) and np.isnan(item): return True

    def get_table_structure(self):
        table_structure = {key: self.constants.CONSOLIDATED_TABLE_STRUCTURE[key][self.table_index] for key in self.constants.CONSOLIDATED_TABLE_STRUCTURE.keys()}
        return table_structure

    def get_value(self, field, race_id_sql):
        sql_query = self.db.generate_query(self.table, [field], where=race_id_sql)
        db_data = self.db.query_db(sql_query)[0][0]
        return db_data

    def get_values(self, fields, race_id_sql):
        sql_query = self.db.generate_query(self.table, fields, where=race_id_sql)
        db_data = self.db.query_db(sql_query)[0]
        return db_data
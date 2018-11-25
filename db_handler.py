import logging
import MySQLdb
import re

logging.basicConfig(filename='db_handler.log', filemode='w', level=logging.INFO)


class QueryDB:

    def connect(self):
        if not self.connection: self.connection = MySQLdb.connect(host='localhost',
                                                                  user=self.user,
                                                                  password=self.password)

    def close(self):
        if self.connection:
            self.connection.close()

    def query_db(self, sql_query, return_col_names=False):
        cursor = self.connection.cursor()
        self._use_db(self.connection, cursor)
        print('Sending SQL query')
        if self.verbose: print(sql_query)
        cursor.execute(sql_query)
        print('Processing response')
        results = list(cursor)
        results_cols = [item[0] for item in cursor.description]
        return (results, results_cols) if return_col_names else results

    def generate_query(self, table_name, fields, where='', other='', print_query=False):
        sql = f'SELECT {", ".join(fields)}'
        sql += f' FROM {table_name}'
        if where: sql += f' WHERE {where}'
        if other: sql += f' {other}'
        if print_query: print(sql)
        return sql

    def generate_update_query(self, table, field_list, value_list, where='', other='', print_query=False):
        sql = f'UPDATE {table} SET {self.concatenate_field_value_pairs(field_list, value_list)} '
        if where: sql += f'WHERE {where}'
        if other: sql += f'{other}'
        if print_query: print(sql)
        return sql

    def update_db(self, sql_query):
        cursor = self.connection.cursor()
        self._use_db(self.connection, cursor)
        sql_query = re.sub(r'[\'"](NULL|nan|None)[\'"]', "NULL", sql_query)     # Correct NULL entry for insertion into db
        if self.verbose: print(f'Update SQL: {sql_query}')
        cursor.execute(sql_query)
        self.connection.commit()
        # print('Update query sent; change committed')

    def concatenate_field_value_pairs(self, field_list, value_list, separator=', '):
        value_list = [self.escape_and_clean(item) for item in value_list]
        pairs = [f'{field} = \'{value}\'' for field, value in zip(field_list, value_list)]
        return separator.join(pairs)

    def escape_and_clean(self, item):
        escaped_item = re.sub(r"(['\\])", r'\\\1', str(item))   # Escape textual backslashes and tick marks
        cleaned_item = re.sub(u"\uFFFD", "", escaped_item)      # Fix oddball <?> character
        return cleaned_item.strip()                             # Strip off any leading or trailing whitespace

    def initialize_db(self):
        """Checks to see if db exists. If not, creates it."""
        if not self.connection: self.connect()
        cursor = self.connection.cursor()
        sql = f'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = "{self.db}"'
        if self.verbose: print(sql)
        db_exists = cursor.execute(sql)
        if db_exists:
            print(f'Database {self.db} already exists--skipping create step')
        elif db_exists == 0:
            self._create_db(self.connection, cursor)
            print(f'Created new database {self.db}')
        else:
            print(f'There was a problem checking whether {self.db} exists--unexpected db_exists value.')

    def add_to_table(self, table_name, table_data, sql_col_names):
        cursor = self.connection.cursor()
        self._use_db(self.connection, cursor)
        self._insert_records(self.connection, cursor, table_name, table_data, sql_col_names)

    def delete_from_table(self, table_name, where_fields, where_values):
        if len(where_fields) < 1 or len(where_values) < 1:
            print('Deletion of entire table not allowed.')
            return
        elif len(where_fields) != len(where_values):
            print('Lengths of fields and values for deletion do not match')
            return
        else:
            where_clause = self.concatenate_field_value_pairs(where_fields, where_values, separator=" and ")
            sql = f'DELETE FROM {table_name} WHERE {where_clause};'

        if self.verbose: print(sql)
        self._send_query_and_commit(sql)

    def initialize_table(self, table_name, dtypes, unique_key, foreign_key):
        """Checks to see if a table exists. If not, creates it."""
        cursor = self.connection.cursor()
        self._use_db(self.connection, cursor)
        sql = 'SELECT count(*) FROM information_schema.TABLES '
        sql += f"WHERE (TABLE_SCHEMA = '{self.db}') AND (TABLE_NAME = '{table_name}')"
        if self.verbose: print(sql)
        cursor.execute(sql)
        table_exists = [item for item in cursor][0][0]
        if table_exists:
            logging.info(f'Table {table_name} already exists--skipping creation step.')
        elif table_exists == 0:
            self._create_table(self.connection, cursor, table_name, dtypes, unique_key, foreign_key)
        else:
            print(f'There was a problem checking whether {table_name} exists--unexpected table_exists value.')

    def _create_db(self, db, cursor):
        cursor.execute(f'CREATE DATABASE {self.db}')
        db.commit()

    def _use_db(self, db, cursor):
        cursor.execute(f'USE {self.db}')
        db.commit()

    def _create_table(self, db, cursor, table_name, dtypes, unique_key, foreign_key):
        logging.info(f'Creating table {table_name}')
        sql = f'CREATE TABLE {table_name} ('
        sql += 'id INT NOT NULL AUTO_INCREMENT, '
        for column_name, column_dtype in dtypes.items():
            sql += f'{column_name} {column_dtype}, '
        sql += 'PRIMARY KEY (id)'
        if unique_key:
            sql += ', UNIQUE ('
            for key in unique_key:
                sql += key + ', '
            sql = sql[:-2]  # Chop off last ', '
            sql += ")"
        if foreign_key:
            for constraint in foreign_key:
                sql += f', FOREIGN KEY({constraint[0]}) REFERENCES {constraint[1]} '
        sql += ')'          # ... and balance parentheses before sending.
        logging.info(f'Creating table {table_name}:\n\t{sql}')
        try:
            cursor.execute(sql)
        except MySQLdb.ProgrammingError as e:
            print(f'Error creating table {table_name}:\n\t{sql}\n\t{e}')
            logging.info(f'Error creating table{table_name}:\n\t{sql}')
        db.commit()

    def _insert_records(self, db, cursor, table_name, table_data, sql_col_names, print_sql=False):
        for i in range(len(table_data)):
            values_string = ''
            for item in table_data[i]:
                values_string += self.escape_and_clean(item) + "', '"   # Strip off any leading or trailing whitespace
            values_string = values_string[:-4]                          # Chop off extra "', '"
            sql = f"INSERT INTO {table_name} ({', '.join(sql_col_names)}) VALUES ('{values_string}')"
            sql = re.sub(r"'(NULL|nan|None)'", "NULL", sql)             # NULL should be sent in SQL w/o quote marks
                                                                        # nan and None should be stored as NULL
            if self.verbose: print(sql)

            try:
                cursor.execute(sql)
            except (MySQLdb.ProgrammingError, MySQLdb.IntegrityError) as e:
                if re.search(r'Duplicate entry', repr(e)):
                    logging.debug(f'Error adding entry: \n\t{e}')
                    logging.debug(f'\t{i+1} of {len(table_data)}: {sql}')
                else:
                    logging.info(f'Error adding entry: \n\t{e}')
                    logging.info(f'\t{i+1} of {len(table_data)}: {sql}')
        db.commit()

    def _send_query_and_commit(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()


    def __init__(self, db, username='codelou', password='ABCabc123!', initialize_db=False, verbose=False):
        self.db = db
        self.user = username
        self.password = password
        self.connection = None
        self.verbose = verbose

        if initialize_db: self.initialize_db()

    def __enter__(self):
        self.connection = MySQLdb.connect(
            host='localhost',
            user=self.user,
            password=self.password)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()


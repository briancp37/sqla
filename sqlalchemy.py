import time

import pandas as pd
import sqlalchemy as sql
from sqlalchemy.schema import Table

from constants import _DATABASE_UID, _DATABASE_PWD, _DATABASE_HOST



class SqlAlchemy:
    """
    A class for interacting with a PostgreSQL database using SQLAlchemy.

    Parameters:
        schema (str): The database schema to use.
        engine (sqlalchemy.engine.base.Connection): A preconfigured SQLAlchemy engine (optional).
        driver (str): The database driver (default is 'postgresql').
        server (str): The database server address (default is '13.112.113.186').
        port (str): The database server port (default is '5432').
        database (str): The name of the database (default is 'postgres').

    Attributes:
        schema (str): The database schema.
        driver (str): The database driver.
        server (str): The database server address.
        port (str): The database server port.
        database (str): The name of the database.
        engine (sqlalchemy.engine.base.Connection): The SQLAlchemy engine.
        metadata (sqlalchemy.MetaData): The SQLAlchemy metadata object.

    """
    def __init__(self, schema='flights', engine=None, driver='postgresql', server=_DATABASE_HOST, port='5432', database='sports_data'):
        self.schema = schema
        self.driver = driver
        self.server = server
        self.port = port
        self.database = database
        self.engine = engine if engine != None else sql.create_engine(f"{driver}://{_DATABASE_UID}:{_DATABASE_PWD}@{server}:{port}/{database}")
        # if engine != None:
        #     self.engine = engine
        # else:
        #     url = f"{driver}://{_DATABASE_UID}:{_DATABASE_PWD}@{server}:{port}/{database}"
        # engine = sql.create_engine(f"{driver}://{uid}:{pwd}@{server}:{port}/{database}")#, echo=True)
        # print(engine)

        self.metadata = sql.MetaData(schema=self.schema)
        self.metadata.reflect(bind=self.engine)

    def add_records_to_table(self, table_name:str, record_arr:list):
        """
        Add records to a table.

        Parameters:
            table_name (str): The name of the table.
            record_arr (list): List of dictionaries to insert into the table.
        """
        with self.engine.begin() as conn:
            tbl = Table(table_name, self.metadata, schema=self.schema, autoload_with=self.engine)
            query = sql.insert(tbl).values(record_arr) #.on_conflict_do_nothing(index_elements=[tbl.c.id])
            # query = pg_insert(tbl).values(record_arr) #.on_conflict_do_nothing(index_elements=[tbl.c.id])
            result = conn.execute(query)

    def clear_table(self, table_name):
        """
        Clear all rows from a table.

        Parameters:
            table_name (str): The name of the table.
        """
        with self.engine.begin() as conn:
            tbl = Table(table_name, self.metadata, schema=self.schema, autoload_with=self.engine)
            result = conn.execute(sql.delete(tbl))

    def create_temp_table(self, table_name, limit=100):
        """
        Create a temporary table and copy data from an existing table into it.

        Parameters:
            table_name (str): The name of the source table.
            limit (int, optional): The maximum number of rows to copy (default is 100).
        """
        with self.engine.begin() as conn:
            tbl_source = Table(table_name, self.metadata, schema='twoprime', autoload_with=self.engine)
            tbl_temp = Table(f"{table_name}_temp", self.metadata, *[c.copy() for c in tbl_source.columns], schema='twoprime', autoload_with=self.engine)
            tbl_temp.create()

            select_stmt = sql.select([tbl_source]).limit(limit)
            for row in conn.execute(select_stmt):
                conn.execute(tbl_temp.insert().values(**row))

    def check_if_columns_in_table(self, table_name:str, column_names:list):
        """
        Check if columns exist in a table.

        Parameters:
            table_name (str): The name of the table.
            column_names (list): List of column names to check.

        Returns:
            dict: A dictionary indicating if each column exists in the table.
        """
        tbl = Table(table_name, self.metadata, schema='twoprime', autoload_with=self.engine)
        d_out = {col_name:True if col_name in tbl.c else False for col_name in column_names}
        # return column_name in tbl.c
        return d_out

    def add_columns_to_table(self, table_name:str, columns:list):
        """
        Add columns to a table.

        Parameters:
            table_name (str): The name of the table.
            columns (list): List of dictionaries specifying columns to add. Each dictionary in the 'columns' list should have 'name' and 'type' keys.
                Example: columns = [{'name': 'id', 'type': 'INTEGER'}, {'name': 'dt', 'type': 'DATETIME'}, ...]
        """
        column_names = [column['name'] for column in columns]
        check_if_in_table = self.check_if_columns_in_table(table_name, column_names)
        with self.engine.begin() as conn:
            for column in columns:
                if check_if_in_table[column['name']] == False:
                    conn.execute(sql.text(f"ALTER TABLE {self.schema}.{table_name} ADD COLUMN {column['name']} {column['type']}")) # AFTER {column['after']}"))

    def get_all_val_in_table_a_not_table_b(self, table_a_name, table_a_column, table_b_name, table_b_column):
        tbl_a = sql.Table(table_a_name, self.metadata, autoload_with=self.engine)
        tbl_b = sql.Table(table_b_name, self.metadata, autoload_with=self.engine)
        with self.engine.begin() as conn:
            subquery = sql.select(tbl_b.c[table_b_column]).where(tbl_b.c[table_b_column] == tbl_a.c[table_a_column])
            query = sql.select(tbl_a.c[table_a_column]).where(sql.not_(sql.exists(subquery)))
            arr = conn.execute(query).scalars().all()
            return arr

    def get_last_row(self, table_name, col='id', ascending=True):
        """
        Retrieves the last row of the specified table (based on the highest ID).

        Parameters:
            table_name (str): The name of the table from which to retrieve the last row.
            col (str): The name of the column to compare (default is 'id').
            ascending (bool, optional): Set to True for ascending order, False for descending (defaults is True).

        Returns:
            dict: A dictionary representing the last row of the table, with column names as keys.
        """
        with self.engine.begin() as conn:
            tbl = Table(table_name, self.metadata, schema=self.schema, autoload_with=self.engine)
            column_names = [column.name for column in tbl.columns]
            query = sql.select(tbl).order_by(sql.desc(tbl.c.id)).limit(1)
            result = conn.execute(query)
            row = result.fetchone()
            row_dict = dict(zip(column_names, row))
            return row_dict

    def get_first_row_after(self, table_name, col, value):
        """
        Retrieves the first row from the specified table after a certain value in a given column.

        Parameters:
            table_name (str): The name of the table to query.
            col (str): The name of the column to compare.
            value (any): The value to compare against in the specified column.

        Returns:
            dict: A dictionary representing the row found, with column names as keys.
        """
        with self.engine.begin() as conn:
            tbl = Table(table_name, self.metadata, schema=self.schema, autoload_with=self.engine)
            column_names = [column.name for column in tbl.columns]
            query = sql.select(tbl).order_by(sql.desc(tbl.c.id)).limit(1)
            result = conn.execute(query)
            row = result.fetchone()
            row_dict = dict(zip(column_names, row))
            return row_dict

    def read_table(self, table_name, columns=None, order_by_col=None, order_by_asc=True, after_last_col=None, after_val=None,
                   null_cols=None, non_null_cols=None, equals_col=None, equals_val=None, equals_dct=None, col_in_arr=None, between_col=None, between_arr=None, limit=10000):
        """
        Reads data from a specified table with options to specify columns, order, filtering conditions on null/non-null values, and limits on the number of rows.

        Parameters:
            table_name (str): The name of the table to read data from.
            columns (list of str, optional): The list of column names to retrieve. Defaults to all columns.
            order_by_col (str, optional): The column name to order the results by.
            order_by_asc (bool, optional): Set to True for ascending order, False for descending. Defaults to True.
            after_last_col (str, optional): The column to compare for retrieving rows after a certain value.
            after_val (any, optional): The value to compare in the after_last_col column.
            null_cols (list of str, optional): List of columns to filter where values are NULL.
            non_null_cols (list of str, optional): List of columns to filter where values are NOT NULL.
            limit (int, optional): The maximum number of rows to return. Defaults to 10000.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the query results.
        """
        # print(f"Reading Table --> update_table(table_name={table_name}, columns={columns}, order_by_col={order_by_col}, order_by_asc={order_by_asc}, between_col={between_col}, between_arr={between_arr})")
        tbl = sql.Table(table_name, self.metadata, schema=self.schema, autoload_with=self.engine)
        columns = columns if columns != None else [column.name for column in tbl.columns]
        # print(column_names)
        with self.engine.connect() as conn:

            columns_to_select = [getattr(tbl.c, col_name) for col_name in columns]
            # print(columns_to_select)
            source_query = sql.select(*columns_to_select)#.where(sql.sql.and_(*conditions))

            conditions = []
            if between_col and between_arr and len(between_arr) == 2:
                start_date, end_date = between_arr
                if start_date and end_date:
                    conditions_between = [tbl.c[between_col].between(start_date, end_date)]
                elif start_date:
                    conditions_between = [tbl.c[between_col] >= start_date]
                elif end_date:
                    conditions_between = [tbl.c[between_col] <= end_date]
                conditions = conditions + conditions_between
            if after_last_col != None:
                try:
                    # last_row = self.get_last_row(table_name)
                    # last_primary_key = last_row[after_last_col]
                    conditions_after_last_col = [tbl.c[after_last_col] > after_val]
                    conditions = conditions + conditions_after_last_col
                except:
                    last_row = None
            if null_cols != None:
                conditions_null_cols = [getattr(tbl.c, col_name) == None for col_name in columns]
                conditions = conditions + conditions_null_cols
                # source_query = source_query.where(sql.sql.and_(*conditions))
            if non_null_cols != None:
                conditions_non_null_cols = [getattr(tbl.c, col_name).is_not(None) for col_name in columns]
                conditions = conditions + conditions_non_null_cols
                # source_query = source_query.where(sql.sql.and_(*conditions))
            if equals_col != None and equals_val != None:
                conditions_equals = [tbl.c[equals_col] == equals_val]
                conditions = conditions + conditions_equals
            if equals_dct != None: 
                for equals_c,equals_v in equals_dct.items():
                    conditions_equals = [tbl.c[equals_c] == equals_v]
                conditions = conditions + conditions_equals
            if col_in_arr != None:
                conditions_in_arr = [tbl.c[col_in_arr[0]].in_(col_in_arr[1])]
                conditions = conditions + conditions_in_arr
            if conditions != []:
                source_query = source_query.where(sql.sql.and_(*conditions))
            if order_by_col != None:
                source_query = source_query.order_by(tbl.c[order_by_col] if order_by_asc==True else sql.desc(tbl.c[order_by_col]))
            if limit != None:
                source_query = source_query.limit(limit)

            df = pd.read_sql(sql=source_query, con=conn)

            return df

    def rename_columns(self, table_name, column_dict):
        """
        Renames columns in the specified table based on a provided dictionary mapping.

        Parameters:
            table_name (str): The name of the table where the columns will be renamed.
            column_dict (dict): A dictionary where keys are old column names and values are the new column names.
        """
        with self.engine.begin() as conn:
            tbl = Table(table_name, self.metadata, schema=self.schema, autoload_with=self.engine)
            for old_name, new_name in column_dict.items():
                # Check if the old column name exists in the table
                if old_name in tbl.c:
                    # Rename column
                    rename_stmt = sql.text(f'ALTER TABLE {self.schema}.{table_name} RENAME COLUMN {old_name} TO {new_name};')
                    conn.execute(rename_stmt)
                else:
                    print(f"Column {old_name} not found in table {table_name}")

    def batch_update_table(self, table_name, arr_rows, col_merge, batch_size=1000):
        """
        This method updates rows in the specified table using the provided array of row dictionaries.
        The update is performed in batches for efficiency, with each batch size controlled by batch_size.

        Parameters:
            table_name (str): The name of the table to be updated.
            arr_rows (list): A list of dictionaries where each dictionary represents a row to be updated. Each dictionary must include the column specified by 'col_merge'.
            col_merge (str): The column name in the table to merge on (usually a primary key or a unique column).
            batch_size (int, optional): The number of rows to be updated in each batch (default is 1000).
        """
        with self.engine.begin() as conn:
            tbl = Table(table_name, self.metadata, schema=self.schema, autoload_with=self.engine)
            total_rows = len(arr_rows)
            n_batches = total_rows //batch_size + 1
            print(f"total_rows = {total_rows}\nbatch_size = {batch_size}\nn_batches  = {n_batches}")

            t_0 = time.time()
            for n, offset in enumerate(range(0, total_rows, batch_size)):

                i_low = offset
                i_high = offset + min(batch_size, total_rows-offset)
                sub_arr_rows = arr_rows[i_low:i_high]

                sec_since_start = time.time() - t_0
                est_sec_duration = sec_since_start * (n_batches / (n+0.0001))
                est_sec_remaining = int(est_sec_duration - sec_since_start)
                print(f"\nBatch {n}/{n_batches} --> {i_low} to {i_high} of {total_rows} total rows --> {est_sec_remaining//3600:02d}h{(est_sec_remaining%3600)//60:02d}m remaining")
                print(tab(f"sub_arr_rows[0] = {sub_arr_rows[0]}\nsub_arr_rows[-1] = {sub_arr_rows[-1]}"))


                for row in sub_arr_rows:
                    stmt = sql.update(tbl).where(tbl.c.id == row[col_merge]).values(fee_price=row['fee_price'], fee_value=row['fee_value'])
                    conn.execute(stmt)
                conn.commit()
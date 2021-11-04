from sql_parser.contents import *
from pandas import DataFrame


class Table:
    def __init__(self, table_name, cols=None):
        self.name = table_name
        self._columns = []
        self.columns = []
        if cols:
            for col in cols:
                self.add_col(col)

    @staticmethod
    def factory_from_existing(table_name, connection):
        table = Table(table_name)
        cursor = connection.cursor()
        cursor.execute(f"pragma table_info('{table_name}')")
        table_info = cursor.fetchall()
        for column in table_info:
            col_name = column[1]
            col_type = column[2]
            not_null = bool(column[3])
            default_value = column[4]
            pk = bool(column[5])
            table.add_col(Column.factory_column(col_name, col_type,
                                                not_null=not_null,
                                                default=default_value,
                                                pk=pk))
        return table

    def add_col(self, col: Column):
        '''
        Add a column to the Table.
        :param col: a sql_parser Column. One may use sql_parser.contents.Column.factory_column().
        '''
        self._columns.append(col)
        self.columns.append(col.name)

    def insert_values(self, values: dict, connection):
        '''
        :param values: dictionary/Series with index/keys matching table column names
        :param connection: a sqlite3 connection to a database
        '''
        cursor = connection.cursor()
        statement = f"""
        INSERT INTO {self.name} 
        ({self._string_list_columns()})
        VALUES
        ({self._prepare_values_string(values)})
        """
        cursor.execute(statement)

    def insert_many_values(self, values, connection):
        '''
        :param values: DataFrame or list of dictionaries with columns/keys matching table column names
        :param connection: a sqlite3 connection to a database
        '''
        cursor = connection.cursor()
        statement = f"""
                INSERT INTO {self.name} 
                ({self._string_list_columns()})
                VALUES
                {self._prepare_many_values_string(values)}
                """
        cursor.execute(statement)

    def _string_list_columns(self):
        string = ''
        for col in self._columns:
            string += f"{col.name},"
        return string[:-1]

    def _prepare_values_string(self, values):
        string = ''
        for col in self._columns:
            string += f"{col.to_sql_string(values[col.name])},"
        return string[:-1]

    def _prepare_many_values_string(self, values):
        string = ''
        if isinstance(values, DataFrame):
            for row in values.iterrows():
                string += f"({self._prepare_values_string(row[1])}),"
        elif isinstance(values, list):
            for item in values:
                string += f"({self._prepare_values_string(item)}),"
        return string[:-1]

    def create_table(self, connection):
        '''
        Create table if not exists.
        :param connection: a sqlite3 connection to the database.
        '''
        cursor = connection.cursor()
        statement = f"""
        CREATE TABLE IF NOT EXISTS {self.name}
        ({self._complete_list_of_columns()})"""
        cursor.execute(statement)

    def _complete_list_of_columns(self):
        string = ''
        for col in self._columns:
            string += f"{col.get_full_description()},"
        return string[:-1]




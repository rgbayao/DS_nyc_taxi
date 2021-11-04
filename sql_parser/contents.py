from datetime import datetime
import sqlite3

TEXT_TYPE = 'TEXT'
PRIMARY_KEY = 'PRIMARY_KEY'
INTEGER = 'INTEGER'
FLOAT = 'FLOAT'
TIMESTAMP = 'TIMESTAMP'
NONE_TYPE = '#NONE_TYPE#'

def fetch_database(db_name,statement):
    try:
        con = sqlite3.connect(db_name)
        cursor = con.cursor()
        cursor.execute(statement)
        result = cursor.fetchall()
        description = cursor.description
    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        con.commit()
        con.close()
    return result, description

class Column:
    def __init__(self, name, not_null=False, default=None, pk=False):
        self.name = name
        self.not_null = not_null
        self.default = default
        self.pk = pk
        self.type = NONE_TYPE

    @staticmethod
    def factory_column(name, col_type, not_null=False, default=None, pk=False):
        if col_type == TEXT_TYPE:
            return TextCol(name, not_null, default, pk)
        if col_type == INTEGER:
            return IntCol(name, not_null, default, pk)

    def to_sql_string(self, value):
        return f"{value}"

    def _get_full_description(self):
        s = f'{self.name} {NONE_TYPE}'
        if self.not_null:
            s += ' NOT NULL'
        if self.pk:
            s += ' PRIMARY KEY'
        if self.default:
            s += f'DEFAULT {self.default}'
        return s

    def get_full_description(self):
        s = self._get_full_description()
        return s.replace(NONE_TYPE, self.type)


class TextCol(Column):
    def __init__(self, name, not_null=False, default=None, pk=False):
        super().__init__(name, not_null, default, pk)
        self.type = TEXT_TYPE

    def to_sql_string(self, value):
        return f"'{value}'"


class IntCol(Column):
    def __init__(self, name, not_null=False, default=None, pk=False):
        super().__init__(name, not_null, default, pk)
        self.type = INTEGER

    def to_sql_string(self, value):
        return f"{int(value)}"


class FloatCol(Column):
    def __init__(self, name, not_null=False, default=None, pk=False):
        super().__init__(name, not_null, default, pk)
        self.type = FLOAT

    def to_sql_string(self, value):
        return f"{float(value)}"


class TimestampCol(Column):
    def __init__(self, name, not_null=False, default=None, pk=False):
        super().__init__(name, not_null, default, pk)
        self.type = TIMESTAMP

    def to_sql_string(self, value, value_format='ISO'):
        if value_format == 'ISO':
            date = datetime.fromisoformat(value)
        else:
            date = datetime.strptime(value, value_format)
        return f"'{date.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

import sqlite3 as db
from abc import ABC
import logging

# import numpy

logger = logging.getLogger(__name__)


class ColumnType:
    INTEGER = "INTEGER"
    REAL = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"
    NULL = "NULL"


class ForeignKey:
    def __init__(self, table, col):
        self.table = table
        self.column = col


class DBVariable:
    primary_key = False
    entry_type = "INTEGER"

    def __init__(self, entry_type,
                 foreign_key: ForeignKey = None, primary_key=False,
                 auto_inc=False, is_unique=False):
        self.name = None
        self.entry_type = entry_type
        self.primary_key = primary_key
        self.auto_inc = auto_inc
        self.is_unique = is_unique
        self.foreign_key = foreign_key


class Table:
    columns: list[DBVariable] = []

    def __init__(self, name, columns: list):
        self.name = name
        self.columns = columns
        self.unique = ""

    def add_unique_sets(self, sets: tuple[tuple]):
        for unique_set in sets:
            cols = []
            for var in unique_set:
                cols.append(var)
            self.unique += f", UNIQUE{str(tuple(cols))}"

    def _assemble_statement(self):
        statement = f"CREATE TABLE {self.name}("
        foreign_keys = ""
        for i in range(len(self.columns)):
            col = self.columns[i]
            statement += f"{col.name} {col.entry_type}"
            if col.primary_key:
                statement += " PRIMARY KEY"
            if col.is_unique:
                statement += " UNIQUE"
            if col.foreign_key is not None:
                foreign_keys += f", FOREIGN KEY({col.name})" \
                                + f" REFERENCES {col.foreign_key.table}" \
                                + f"({col.foreign_key.column})"
            if i != len(self.columns) - 1:
                statement += ", "

        statement += self.unique
        statement += foreign_keys
        statement += ")"
        return statement

    def create_table(self, database_file):
        con, cur = _connect_db(database_file)
        statement = self._assemble_statement()
        logger.info(f"Executing Statement: {statement}")
        cur.execute(statement)
        con.commit()
        _disconnect_db(con, cur)


def dict_from_row(row):
    return dict(zip(row.keys(), row))


def _connect_db(database_file):
    con = db.connect(database_file)
    con.row_factory = db.Row
    cur = con.cursor()
    return con, cur


def _disconnect_db(con, cur):
        cur.close()
        con.close()


def set_class(c):
    c._class = c
    return c


class DBQuery:
    _db_filepath = None
    _obj = None

    def __init__(self, obj, file_path):
        self._obj = obj
        self._db_filepath = file_path

    def _connect_db(self):
        self.con = db.connect(self._db_filepath)
        self.con.row_factory = db.Row
        self.cur = self.con.cursor()

    def _disconnect_db(self):
        self.cur.close()
        self.con.close()

    @staticmethod
    def get_value_string(length):
        value_string = ""
        for i in range(0, length):
            if i == length - 1:
                value_string += "?"
            else:
                value_string += "?, "
        return value_string

    def execute_query(self, statement, args=None):
        self._connect_db()
        print(f"DEBUG EXECUTING STATEMENT: {statement} WITH ARGS {args}")
        if args is None:
            logger.debug(f"Executing Statement: {statement}:")
            sql_res = self.cur.execute(statement).fetchall()
        else:
            logger.debug(f"Executing Statement: {statement} with args {args}:")
            sql_res = self.cur.execute(statement, args).fetchall()
        resp = []
        for row in sql_res:
            resp.append({key: row[key] for key in row.keys()})
        self.con.commit()
        _disconnect_db(self.con, self.cur)
        return resp  # Dictionary assembled response

    def raw_query(self, statement, args, suppress_warning=False):
        if not suppress_warning:
            logger.warning(f"Raw query executed for {self._obj.__name__}. To avoid errors"
                           + f" use built in functions.")
        return self.execute_query(statement, args)

    def assemble_raw(self, sql_response):
        objects = []
        for row in sql_response:
            objects.append(self._obj(**row))
        return objects


class DBGetQuery(DBQuery):
    def __init__(self, obj, file_path):
        super().__init__(obj, file_path)

    def all(self):
        statement = f"SELECT * FROM {self._obj.__name__}"
        response = self.execute_query(statement)
        return self.assemble_raw(response)

    def where(self, **kwargs):
        where_clause = "WHERE "
        args = []
        last_key = list(kwargs)[-1]
        for key, value in kwargs.items():
            args.append(value)
            if key == last_key:
                where_clause += f"{key} = ? "
            else:
                where_clause += f"{key} = ? AND "


        statement = f"SELECT * FROM {self._obj.__name__} {where_clause}"
        print(kwargs)

        print(args)
        args = tuple(args)
        response = self.execute_query(statement, args)
        return self.assemble_raw(response)


class StringVariable(DBVariable):
    def __init__(self, entry_type, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(entry_type, foreign_key, primary_key, auto_inc, is_unique)


class IntegerVariable(DBVariable):
    def __init__(self, entry_type, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(entry_type, foreign_key, primary_key, auto_inc, is_unique)


class BoolVariable(DBVariable):
    def __init__(self, entry_type, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(entry_type, foreign_key, primary_key, auto_inc, is_unique)

    @staticmethod
    def convert_bool(self, b: bool):
        return 1 if b else 0


class BlobVariable(DBVariable):
    def __init__(self, entry_type, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(entry_type, foreign_key, primary_key, auto_inc, is_unique)


class FileVariable(DBVariable):
    def __init__(self, entry_type, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(entry_type, foreign_key, primary_key, auto_inc, is_unique)

    def get_file_data(self):
        pass

    def get_numpy_data(self):
        pass

    def save_file_data(self):
        pass

    def save_numpy_data(self):
        pass


class DBObject(ABC):
    _class = None
    _db_mapping = {}
    _primary_key = None
    _file_path = None

    def __init__(self, **kwargs):
        d = self.__class__.__dict__
        for key, value in d.items():

            if type(value) is DBVariable:
                self._db_mapping[key] = value
                value.name = key

    def save(self):
        keys, values = self._parse_columns()
        query = DBQuery(self.__class__, self._file_path)
        # Sanitization
        statement_values = query.get_value_string(len(values))
        statement = f"INSERT OR IGNORE INTO {self.__class__.__name__} {str(keys)} VALUES({statement_values})"
        args = []
        for v in values:
            args.append(getattr(self, v.name))
        resp = query.execute_query(statement, args)

    def _parse_columns(self):
        orm_objects: dict[str, DBVariable] = {}
        for key, value in self._db_mapping.items():
            if type(value) is DBVariable:
                orm_objects[key] = value
        return tuple(orm_objects.keys()), tuple(orm_objects.values())

    @classmethod
    def create_table(cls):
        t_class = cls._class
        d = get_db_mapping(t_class)
        cols = []
        for key, value in d.items():
            if value is not None:
                if value.name is None:
                    value.name = key
                cols.append(value)

        table = Table(t_class.__name__, cols)
        table.create_table(cls._file_path)


def get_db_mapping(t_class) -> dict[str, DBVariable]:
    orm_objects: dict[str, DBVariable] = {}
    d = t_class.__dict__
    for key, value in d.items():
        if type(value) is DBVariable:
            orm_objects[key] = value
    return orm_objects


class TestObject(DBObject):
    test_string = DBVariable(ColumnType.TEXT)
    test_float = DBVariable(ColumnType.REAL)
    test_bool = DBVariable(ColumnType.INTEGER)
    test_int = DBVariable(ColumnType.INTEGER)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_string = "Test String"
        self.test_float = 123.456
        self.test_bool = False
        self.test_int = 123

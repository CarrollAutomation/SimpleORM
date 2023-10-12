import os
import sqlite3 as db
import timeit
from abc import abstractmethod, ABC
import logging

import numpy

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


class Column:
    primary_key = False
    entry_type = "INTEGER"

    def __init__(self, name, entry_type,
                 foreign_key: ForeignKey = None, primary_key=False,
                 auto_inc=False, is_unique=False):
        self.name = name
        self.entry_type = entry_type
        self.primary_key = primary_key
        self.auto_inc = auto_inc
        self.is_unique = is_unique
        self.foreign_key = foreign_key


class Table:
    columns: list[Column] = []

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


def execute_query(db_file, statement, args=None):
    con, cur = _connect_db(db_file)
    if args is None:
        logger.debug(f"Executing Statement: {statement}:")
        sql_res = cur.execute(statement).fetchall()
    else:
        logger.debug(f"Executing Statement: {statement} with args {args}:")
        sql_res = cur.execute(statement, args).fetchall()
    resp = []
    for row in sql_res:
        resp.append({key: row[key] for key in row.keys()})
    con.commit()
    _disconnect_db(con, cur)
    return resp


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


class ORMVar:
    def __init__(self, entry_type,
                 foreign_key: ForeignKey = None, primary_key=False,
                 auto_inc=False, is_unique=False):
        self.name = None
        self.entry_type = entry_type
        self.primary_key = primary_key
        self.auto_inc = auto_inc
        self.is_unique = is_unique
        self.foreign_key = foreign_key

    def generate_column(self):
        return Column(self.name, self.entry_type,
                      foreign_key=self.foreign_key, primary_key=self.primary_key,
                      auto_inc=self.auto_inc, is_unique=self.is_unique)


class DBModel(ABC):
    _db_mapping: dict[str, ORMVar] = {}
    _primary_key = 1

    def __init__(self):
        d = get_db_mapping(self.__class__)
        for key, value in d.items():
            # Store configuration for later use
            value.name = key
            self._db_mapping[key] = value
            # Reset data to be usable by subclass
            setattr(self, key, None)

    def create(self):
        pass

    def retrieve(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass


class ObjectDatabase:
    def __init__(self, db_file, objects):
        self.db_file = db_file
        self.objects: list = objects

    def create_sqlite_table(self):
        for t_class in self.objects:
            d = get_db_mapping(t_class)
            cols = []
            for key, value in d.items():
                if value is not None:
                    if value.name is None:
                        value.name = key
                    cols.append(value.generate_column())

            table = Table(t_class.__name__, cols)
            table.create_table(self.db_file)


def get_db_mapping(t_class) -> dict[str, ORMVar]:
    orm_objects: dict[str, ORMVar] = {}
    d = t_class.__dict__
    for key, value in d.items():
        if type(value) is ORMVar:
            orm_objects[key] = value
    return orm_objects

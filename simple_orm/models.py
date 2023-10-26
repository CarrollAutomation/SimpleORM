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
    def __init__(self, col, obj_class):
        self.fk_attribute = col
        self.obj_class = obj_class
        self.table = obj_class.__name__


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
        statement = f"CREATE TABLE IF NOT EXISTS {self.name}("
        foreign_keys = ""
        for i in range(len(self.columns)):
            col = self.columns[i]
            statement += f"{col.name} {col.entry_type}"
            if col.primary_key:
                statement += " PRIMARY KEY"
            if col.is_unique:
                statement += " UNIQUE"
            if col.foreign_key is not None:
                print(col.__dict__)
                foreign_keys += f", FOREIGN KEY({col.name})" \
                                + f" REFERENCES {col.foreign_key.table}" \
                                + f"({col.foreign_key.fk_attribute})"
            if i != len(self.columns) - 1:
                statement += ", "

        statement += self.unique
        statement += foreign_keys
        statement += ")"
        return statement

    def create_table(self, database_file):
        con, cur = _connect_db(database_file)
        logger.debug(f"Table creation for class {self.name}")
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

    def get_filepath(self):
        return self._db_filepath

    def get_object(self):
        return self._obj

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
        try:
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
        except Exception as e:
            raise e
        finally:
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
            objects.append(self._obj.raw(**row))
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

        args = tuple(args)
        response = self.execute_query(statement, args)
        return self.assemble_raw(response)

    def by_primary_key(self, pk_value):
        logger.debug(f"Getting object {self.get_object()} by pk: {pk_value}")
        mapping = get_db_mapping(self.get_object())
        for key, value in mapping.items():
            if value.primary_key:
                pk = value.name
                obj = self.where(**{pk: pk_value})
                print(obj)
                return obj


class StringVariable(DBVariable):
    def __init__(self, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(ColumnType.TEXT, foreign_key, primary_key, auto_inc, is_unique)


class IntegerVariable(DBVariable):
    def __init__(self, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(ColumnType.INTEGER, foreign_key, primary_key, auto_inc, is_unique)


class BoolVariable(DBVariable):
    def __init__(self, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(ColumnType.INTEGER, foreign_key, primary_key, auto_inc, is_unique)

    @staticmethod
    def convert_bool(self, b: bool):
        return 1 if b else 0


class BlobVariable(DBVariable):
    def __init__(self, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(ColumnType.BLOB, foreign_key, primary_key, auto_inc, is_unique)


class FileVariable(DBVariable):
    def __init__(self, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(ColumnType.INTEGER, foreign_key, primary_key, auto_inc, is_unique)

    def get_file_data(self):
        pass

    def get_numpy_data(self):
        pass

    def save_file_data(self):
        pass

    def save_numpy_data(self):
        pass


class ListVariable(DBVariable):
    def __init__(self, entry_type, list_object, foreign_key: ForeignKey = None, primary_key=False, auto_inc=False, is_unique=False):
        super().__init__(entry_type, foreign_key, primary_key, auto_inc, is_unique)
        self.list_object_class = list_object


class DBObject(ABC):
    _primary_key = None
    _file_path = None
    get = None

    def __init__(self, **kwargs):
        mapping = get_db_mapping(self.__class__)
        for key, value in mapping.items():
            if type(value) is DBVariable:
                if value.primary_key:
                    self._primary_key = value

                # Ensure value is usable in DB.
                setattr(self, key, None)

    @classmethod
    def raw(cls, **kwargs):
        obj = cls.__new__(cls)
        mapping = get_db_mapping(obj.__class__)
        for key, value in kwargs.items():
            db_var = mapping.get(key)
            if db_var.foreign_key is not None:
                _class: DBObject = db_var.foreign_key.obj_class
                fk_obj = _class.get.by_primary_key(value)
                setattr(obj, key, fk_obj)
            else:
                setattr(obj, key, value)
        return obj

    def save(self):
        keys, values = self._parse_columns()
        query = DBQuery(self.__class__, self._file_path)
        # Sanitization
        statement_values = query.get_value_string(len(values))
        statement = (f"INSERT OR REPLACE INTO "
                     + f"{self.__class__.__name__} {str(keys)} VALUES({statement_values}) RETURNING *")
        args = []
        logger.debug(f"Saving object {self.__class__.__name__} with values: \n{values}")
        for v in values:

            if v.foreign_key is not None:
                fk: ForeignKey = v.foreign_key
                fk_object = getattr(self, v.name)
                if fk_object is not None:
                    fk_object.save()
                    fk_pk = fk_object.get_primary_key()
                    fk_pk_value = getattr(fk_object, fk_pk.name)
                    args.append(fk_pk_value)
                else:
                    args.append(None)
            else:
                args.append(getattr(self, v.name))


        resp = query.execute_query(statement, args)
        self._update_primary_key(resp)
        logger.debug(f"{self.__class__.__name__} saved successfully")

    def get_primary_key(self):
        if self._primary_key is None:
            mapping = get_db_mapping(self.__class__)
            for key, value in mapping:
                if value.primary_key:
                    self._primary_key = value
        return self._primary_key

    def _update_primary_key(self, sql_row):
        if len(sql_row) == 0:
            return
        if self._primary_key is not None and type(self._primary_key) is DBVariable:
            new_pk_value = sql_row[0].get(self._primary_key.name)
            setattr(self, self._primary_key.name, new_pk_value)

    def _parse_columns(self):
        logger.debug(f"Parsing variables for class {self.__class__.__name__}")
        orm_objects: dict[str, DBVariable] = {}
        mapping = get_db_mapping(self.__class__)
        for key, value in mapping.items():
            if type(value) is DBVariable:
                orm_objects[key] = value
        logger.debug(f"Found keys for {self.__class__.__name__}: {orm_objects.keys()}")
        logger.debug(f"Found values for {self.__class__.__name__}: {orm_objects.values()}")
        return tuple(orm_objects.keys()), tuple(orm_objects.values())

    @classmethod
    def create_table(cls):
        links = []
        t_class = cls.__new__(cls).__class__
        logger.debug(f"Creating table for {t_class}")
        mapping = get_db_mapping(t_class)
        cols = []
        for key, value in mapping.items():
            if value is not None:
                if value.name is None:
                    value.name = key
                if type(value) is ListVariable:
                    _obj = value.list_object_class.create_table()
                    links.append([value, t_class, value.list_object_class])
                cols.append(value)

        table = Table(t_class.__name__, cols)
        table.create_table(cls._file_path)

        for link in links:
            col1 = IntegerVariable(primary_key=True,
                                   foreign_key=ForeignKey(link[0].name, link[1]))
            col1.name = "link_id"
            col2 = IntegerVariable(
                                   foreign_key=ForeignKey('_id', link[2]))
            col2.name = "child_id"
            link_table = Table(f"{link[2].__name__}_link", (col1, col2))
            link_table.create_table(cls._file_path)

    def _create_link(self):
        pass


def get_db_mapping(t_class) -> dict[str, DBVariable]:
    logger.debug(f"Getting db mapping for class: {t_class}")
    d = t_class.__dict__
    mapping = {}
    for key, value in d.items():
        if issubclass(value.__class__, DBVariable):
            mapping[key] = value
    return mapping

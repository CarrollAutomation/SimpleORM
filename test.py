import os
import unittest

from simple_orm.models import *


class ModelTestClass(DBModel):
    test_string = ORMVar(ColumnType.TEXT)
    test_float = ORMVar(ColumnType.REAL)
    test_bool = ORMVar(ColumnType.INTEGER)
    test_int = ORMVar(ColumnType.INTEGER)

    def __init__(self):
        super().__init__()
        self.test_string = "Test String"


class ORMTest(unittest.TestCase):
    test_db_path = os.getcwd()

    def setUp(self) -> None:
        self.test_class = ModelTestClass()
        self.test_db_path = f"{self.test_db_path}\\test.db"
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)

    def test_object_init(self):
        self.assertNotEqual(self.test_class, None, f"Object did not exist")
        self.assertNotEqual(len(self.test_class._db_mapping), 0, f"DB Objects not assigned")

    def test_object_change(self):
        self.assertNotEqual(self.test_class.test_string, None, "Test String does not exist")
        new_val = "new string"
        self.test_class.test_string = new_val
        self.assertEqual(self.test_class.test_string, new_val, "Test Value did not change")

    def test_database_creation(self):
        classes = DBModel.__subclasses__()
        db = ObjectDatabase(self.test_db_path, classes)
        db.create_sqlite_table()
        self.assertEqual(os.path.exists(self.test_db_path), True, "DB file not created")


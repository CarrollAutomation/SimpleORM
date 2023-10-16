import os
import unittest

import simple_orm.orm
from simple_orm.models import *


class UnitTestClassFK1(DBObject):
    _id = DBVariable(ColumnType.INTEGER, primary_key=True)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class UnitTestClass1(DBObject):
    _id = DBVariable(ColumnType.INTEGER, primary_key=True)
    test_string = DBVariable(ColumnType.TEXT)
    test_float = DBVariable(ColumnType.REAL)
    test_bool = DBVariable(ColumnType.INTEGER)
    test_int = DBVariable(ColumnType.INTEGER)
    test_fk = DBVariable(ColumnType.INTEGER, ForeignKey("test_fk", UnitTestClassFK1))

    def __init__(self, **kwargs):
        super().__init__()


class ORMTest(unittest.TestCase):
    test_db_path = f"C:\\Users\\Cactus\\Documents\\unit_test.db"
    initialized = False

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.test_db_path):
            os.remove(cls.test_db_path)
        simple_orm.orm.initialize_orm(cls.test_db_path)

    def setUp(self) -> None:
        self.test_class = UnitTestClass1()

    def test_object_init(self):
        self.assertNotEqual(self.test_class, None, f"Object did not exist")
        self.assertNotEqual(len(self.test_class._db_mapping), 0, f"DB Objects not assigned")

    def test_object_change(self):
        self.assertEqual(self.test_class.test_string, None, f"Test String not none: {self.test_class.test_string}")
        new_val = "new string"
        self.test_class.test_string = new_val
        self.assertEqual(self.test_class.test_string, new_val, "Test Value did not change")

    def test_database_creation(self):
        self.test_class.create_table()
        self.test_class.create_table()
        self.assertEqual(os.path.exists(self.test_db_path), True, "DB file not created")

    def test_object_save(self):
        self.test_class.save()
        self.assertNotEqual(self.test_class._id, None, "ID Not updated")
        self.test_class.test_string = "unit test updated"
        self.test_class.save()
        self.test_classes = self.test_class.get.all()
        self.assertEqual(len(self.test_classes), 1, "Test rows not inserted correctly")

        self.test_class = UnitTestClass1()
        self.test_class.test_string = "Second Test String"
        self.test_class.save()
        self.test_classes = self.test_class.get.all()
        self.assertEqual(len(self.test_classes), 2, "Test rows not inserted correctly")

        self.test_class.test_string = "Second string updated"
        self.test_class.save()
        self.test_classes = self.test_class.get.all()
        self.assertEqual(len(self.test_classes), 2, "Test rows not inserted correctly")
        self.assertEqual(self.test_class.test_string, "Second string updated", "Test String did not update")


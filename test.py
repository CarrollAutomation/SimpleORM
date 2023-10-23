import os
import unittest
import logging
import coloredlogs
import simple_orm.orm
from simple_orm.models import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
coloredlogs.install(level='DEBUG')


class UnitTestClassFK1(DBObject):
    _id = DBVariable(ColumnType.INTEGER, primary_key=True)
    name = DBVariable(ColumnType.TEXT)
    other_val = DBVariable(ColumnType.INTEGER)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.name = "created for unit test"
        self.other_val = 123


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

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.test_db_path):
            os.remove(cls.test_db_path)
        simple_orm.orm.initialize_orm(cls.test_db_path)

    def setUp(self) -> None:
        self.test_class = UnitTestClass1()

    def test_object_init(self):
        self.assertNotEqual(self.test_class, None, f"Object did not exist")

    def test_object_change(self):
        self.assertEqual(self.test_class.test_string, None, f"Test String not none: {self.test_class.test_string}")
        new_val = "new string"
        self.test_class.test_string = new_val
        self.assertEqual(self.test_class.test_string, new_val, "Test Value did not change")

    def test_database_creation(self):
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

        self.test_class.test_fk = UnitTestClassFK1()
        self.test_class.save()
        self.assertEqual(self.test_class.test_fk.name, "created for unit test", "Foreign Key not saved correctly")

        self.test_class = UnitTestClass1()
        self.test_class.test_string = "None FK"
        self.test_class.test_fk = None
        self.test_class.save()
        self.assertEqual(self.test_class.test_fk, None, "Foreign Key test failed. FK not none")

    def test_get_objects(self):
        self.test_class = None
        self.test_class = UnitTestClass1.get.by_primary_key('2')
        self.assertNotEqual(self.test_class, None, "Did not successfully get object by Primary Key")

import os
import random
import unittest
import logging
import coloredlogs
import simple_orm.orm
from simple_orm.models import *
from simple_orm.tests.models import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
coloredlogs.install(level='DEBUG')


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
    test_db_path = f"{os.getcwd()}\\unit_test.db"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.test_db_path):
            os.remove(cls.test_db_path)
        simple_orm.orm.initialize_orm(cls.test_db_path)

    def setUp(self) -> None:
        self.test_class = UnitTestClass1()

    def test_object_init(self):
        self.assertNotEqual(self.test_class, None, f"Object did not exist")

    def test_database_creation(self):
        self.test_class.create_table()
        self.assertEqual(os.path.exists(self.test_db_path), True, "DB file not created")

    def test_save_object(self):
        self.test_class.save()
        self.assertNotEqual(self.test_class._id, None, "ID Not updated")

        for i in range(0,5):
            test_pre_length = len(self.test_class.get.all())
            self.test_class = UnitTestClass1()
            self.test_class.test_string = f"unit test updated {random.randint(0,1000)}"
            self.test_class.save()
            test_classes = self.test_class.get.all()
            self.assertEqual(len(test_classes), test_pre_length + 1, "Test rows not inserted correctly")

        test_pre_length = len(self.test_class.get.all())
        self.test_class.test_string = "Updated existing entity"
        self.test_class.save()
        self.test_classes = self.test_class.get.all()
        self.assertEqual(len(self.test_classes), test_pre_length, "Test rows not inserted correctly")

        self.test_class.test_fk = UnitTestClassFK1()
        self.test_class.save()
        self.assertEqual(self.test_class.test_fk.name, "created for unit test", "Foreign Key not saved correctly")

        self.test_class = UnitTestClass1()
        self.test_class.test_string = "None FK"
        self.test_class.test_fk = None
        self.test_class.save()
        self.assertEqual(self.test_class.test_fk, None, "Foreign Key test failed. FK not none")

    def test_get_objects(self):
        self.test_class.test_string = "Get By Primary Key Test"
        self.test_class.save()
        id = self.test_class._id
        self.test_class = None
        self.test_class = UnitTestClass1.get.by_primary_key(id)[0]
        self.assertNotEqual(self.test_class, None, "Did not successfully get object by Primary Key")
        self.assertNotEquals(self.test_class.test_string, None, "Test String is None")

    def test_one_two_many_object(self):
        self.test_class = OneToManyTestClassParent()
        self.test_class.save()

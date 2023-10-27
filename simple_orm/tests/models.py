from simple_orm.models import *

class UnitTestClassFK1(DBObject):
    _id = IntegerVariable(primary_key=True)
    name = StringVariable()
    other_val = IntegerVariable()

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


class OneToManyTestClassChild(DBObject):
    _id = IntegerVariable(primary_key=True)
    link_id = IntegerVariable()
    value = StringVariable()

    def __init__(self, parent_id, value,**kwargs):
        super().__init__(**kwargs)
        self.parent_id = parent_id
        self.value = value


class OneToManyTestClassParent(DBObject):
    _id = IntegerVariable(primary_key=True)
    obj_list = ListVariable(ColumnType.INTEGER, OneToManyTestClassChild)
    name = StringVariable()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        name = "UnitTestOTMParent"

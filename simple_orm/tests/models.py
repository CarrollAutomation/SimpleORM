from simple_orm.models import *


class UnitTestClassFK1(DBObject):
    _id = IntegerVariable(primary_key=True)
    name = StringVariable()
    other_val = IntegerVariable()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.name = "created for unit test"
        self.other_val = 123


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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

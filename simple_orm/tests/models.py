from simple_orm.models import *


class UnitTestClassFK1(DBObject):
    _id = DBVariable(ColumnType.INTEGER, primary_key=True)
    name = DBVariable(ColumnType.TEXT)
    other_val = DBVariable(ColumnType.INTEGER)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.name = "created for unit test"
        self.other_val = 123


class OneToManyTestClassParent(DBObject):
    _id = IntegerVariable(ColumnType.INTEGER, primary_key=True)
    object_list = None


class OneToManyTestClassChild(DBObject):
    parent_id = IntegerVariable(ColumnType.INTEGER, foreign_key=ForeignKey('_id', OneToManyTestClassParent))
    value = StringVariable(ColumnType.TEXT)

    def __init__(self, parent_id, value,**kwargs):
        super().__init__(**kwargs)
        self.parent_id = parent_id
        self.value = value


    def __init__(self, **kwargs):
        super().__init__(**kwargs)

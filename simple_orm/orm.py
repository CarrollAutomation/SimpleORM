from simple_orm.models import DBGetQuery, DBObject

DATA_BASE_FILES = {}


def initialize_orm(db_path, classes=None):
    global DATA_BASE_FILES
    filename = db_path.split('\\')[-1]
    db_name = filename.split('.')[0]
    path = db_path
    DATA_BASE_FILES[db_name] = path

    if classes is None:
        classes = DBObject.__subclasses__()

    for _class in classes:
        _class._file_path = DATA_BASE_FILES[db_name]
        _class.get = DBGetQuery(_class, DATA_BASE_FILES[db_name])
        _class._class = _class
        _class.create_table()

class RustObject: #The real implementation of the class is injected when the module is loaded
    @classmethod
    def obj(cls):
        return RustObject()

    @classmethod
    def list(cls):
        return RustObject()

    @classmethod
    def int(cls, _int_value: int):
        return RustObject()

    @classmethod
    def float(cls, _float_value: float):
        return RustObject()

    @classmethod
    def str(cls, _str_value: str):
        return RustObject()

    @classmethod
    def bool(cls, _bool_value: bool):
        return RustObject()

    @classmethod
    def bytes(cls, _byte_value: bytes):
        return RustObject()

    def append(self, _value):
        pass

    def set_attr(self, _key: str, _value):
        pass

    def remove_attr(self, _key: str):
        pass


class Consumer: #The real implementation of the class is injected when the module is loaded
    def find(self):
        pass

    def count(self):
        pass

    def value_as_str(self):
        pass

    def key_as_str(self):
        pass

    def value_as_hex(self):
        pass

    def key_as_hex(self):
        pass

    def execute(self):
        pass


class Topic: #The real implementation of the class is injected when the module is loaded
    def find(self, filter_func) -> Consumer:
        pass

    def count(self, filter_func) -> Consumer:
        pass

class Cluster: #The real implementation of the class is injected when the module is loaded
    def get_topic(self, name: str) -> Topic:
        pass

class Header: #The real implementation of the class is injected when the module is loaded
    def __init__(self):
        self.key = None
        self.value = None

class Record:
    def __init__(self):
        self.headers = []
        self.key = None
        self.value = None


def to_rust_object(py_obj):
    if py_obj is None:
        return RustObject.obj()

    _type = type(py_obj)

    if _type in (Cluster, Topic, Record, Header):
        return py_obj

    if _type is Consumer:
        obj_list = RustObject.list()
        for event in py_obj:
            obj = RustObject.obj()
            obj.set_attr("key", to_rust_object(event.key()))
            obj.set_attr("value", to_rust_object(event.value()))
            obj_list.append(obj)
        return obj_list

    if _type is int:
        return RustObject.int(py_obj)

    if _type is float:
        return RustObject.float(py_obj)

    if _type is str:
        return RustObject.str(py_obj)

    if _type is bytes:
        return RustObject.bytes(py_obj)

    if _type is bool:
        return RustObject.bool(py_obj)

    if _type in (list, set, tuple):
        message_list = RustObject.list()

        for item in py_obj:
            message_list.append(to_rust_object(item))
        return message_list

    as_dict = py_obj if _type is dict else py_obj.__dict__
    message = RustObject.obj()
    for attr, attr_value in as_dict.items():
        if not callable(attr_value):
            message.set_attr(attr, to_rust_object(attr_value))

    return message
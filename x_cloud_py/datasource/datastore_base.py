from abc import ABCMeta, abstractmethod


class DataStoreBase(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_table(self, table_name, **kwargs):
        pass

    @abstractmethod
    def put_element(self, table_name, item, **kwargs):
        pass

    @abstractmethod
    def get_element(self, table_name, key, **kwargs):
        pass

    @abstractmethod
    def delete_element(self, table_name, key, **kwargs):
        pass

    @abstractmethod
    def delete_table(self, table_name, **kwargs):
        pass

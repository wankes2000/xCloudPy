import unittest

from x_cloud_py.datasource.datastore_factory import DataStoreFactory


class TestIntegrationGoogleDataStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._datastore = DataStoreFactory.factory("GCP",**{'project':'test'})

    def test_given_a(self):
        self._datastore.put_element(table_name="Arturo",item={"a":"b"},key='23')
        self._datastore.put_element(table_name="Arturo", item={"a": "b"}, key='24')

        self._datastore.delete_table(table_name="Arturo")

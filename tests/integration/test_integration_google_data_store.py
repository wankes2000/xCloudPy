import unittest
import os

from tests.fixtures.datastore_google_fixture import DatastoreGoogleFixture
from x_cloud_py.datastore.datastore_factory import DataStoreFactory
from  google.auth.credentials import Credentials


class DoNothingCreds(Credentials):
    def refresh(self, request):
        pass


class TestIntegrationGoogleDataStore(unittest.TestCase):
    __TEST_TABLE = "google_test_table"

    @classmethod
    def setUpClass(cls):
        os.environ['DATASTORE_DATASET'] = 'test'
        os.environ['DATASTORE_EMULATOR_HOST'] = '127.0.0.1:8282'
        os.environ['DATASTORE_EMULATOR_HOST_PATH'] = '127.0.0.1:8282/datastore'
        os.environ['DATASTORE_HOST'] = 'http://127.0.0.1:8282'
        os.environ['DATASTORE_PROJECT_ID'] = 'test'
        cls.datastore = DataStoreFactory.factory("GCP", **{'project': 'test', 'credentials': DoNothingCreds()})

    @classmethod
    def tearDownClass(cls):
        cls.datastore.delete_table(table_name=cls.__TEST_TABLE)

    def test_given_valid_key_and_table_when_call_put_element_then_element_is_inserted(self):
        element = {"a": "b", "key": "23"}
        self.datastore.put_element(table_name=self.__TEST_TABLE, item=element)

        result = self.datastore.get_element(table_name=self.__TEST_TABLE, key="23")
        self.assertEqual(element, result)

    def test_given_multiple_items_when_call_pul_multiple_items_then_items_are_inserted(self):
        items = DatastoreGoogleFixture.generate_multiple_items(1050)

        self.datastore.put_elements(table_name=self.__TEST_TABLE, items=items)

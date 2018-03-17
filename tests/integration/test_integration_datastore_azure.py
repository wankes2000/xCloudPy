from x_cloud_py.datastore.datastore_azure import CosmosDBDataStore
import unittest

class TestIntegrationDataStoreAws(unittest.TestCase):
    __TABLE_NAME = 'test22'
    __TABLE_NAME_HASH = 'test_hash'
    __OTHER_TABLE_NAME = 'other'
    __HASH_KEY = 'id'
    __RANGE_KEY = 'age'

    @classmethod
    def setUpClass(cls):

        connection_string = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;' \
                            'AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;' \
                            'TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;'
        connection_props = {
            'connection_string': connection_string
        }
        cls.__cosmos_service = CosmosDBDataStore(**connection_props)
        cls.__cosmos_service.create_table(cls.__TABLE_NAME)

    @classmethod
    def tearDownClass(cls):
        cls.__cosmos_service.delete_table(cls.__TABLE_NAME)

    def test_given_valid_name_when_call_list_tables_then_table_names_are_returned(self):
        result = self.__cosmos_service.list_tables()
        self.assertEqual(result,[self.__TABLE_NAME])
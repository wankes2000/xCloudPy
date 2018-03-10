from localstack.services import infra
from x_cloud_py.datastore.datastore_aws import DynamoDBDataStore
from tests.fixtures.datastore_aws_fixture import DataStoreAwsFixture
import unittest
import tempfile
from botocore.exceptions import ClientError
import os


class TestIntegrationDataStoreAws(unittest.TestCase):
    __TABLE_NAME = 'test'
    __OTHER_TABLE_NAME = 'other'
    __HASH_KEY = 'id'
    __RANGE_KEY = 'age'

    @classmethod
    def setUpClass(cls):
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'foo'
        os.environ['AWS_ACCESS_KEY_ID'] = 'bar'
        os.environ['DATA_DIR'] = '/tmp/localstack'
        cls.__dynamodb_datastore = DynamoDBDataStore(**{'endpoint_url': 'http://localhost:4569','region_name':'eu-west-1'})
        create_table_request = DataStoreAwsFixture.get_create_table_request(cls.__TABLE_NAME, cls.__HASH_KEY,
                                                                            cls.__RANGE_KEY)
        cls.__dynamodb_datastore.create_table(cls.__TABLE_NAME, **create_table_request)

    @classmethod
    def tearDownClass(cls):
        cls.__dynamodb_datastore.delete_table(cls.__TABLE_NAME)

    def test_001_given_not_exist_table_when_create_table_then_table_is_created(self):
        self.__dynamodb_datastore.create_table(self.__OTHER_TABLE_NAME)

        self.assertTrue(self.__OTHER_TABLE_NAME in self.__dynamodb_datastore.list_tables())

    def test_002_given_exist_table_when_call_to_delete_table_then_table_is_deleted(self):
        self.__dynamodb_datastore.delete_table(self.__OTHER_TABLE_NAME)

        self.assertTrue(self.__OTHER_TABLE_NAME not in self.__dynamodb_datastore.list_tables())

    def test_003_given_not_exist_table_when_call_to_delete_table_then_exception_is_raised(self):
        self.assertRaises(ClientError, self.__dynamodb_datastore.delete_table, self.__OTHER_TABLE_NAME)

    def test_given_valid_element_when_call_put_element_then_element_is_inserted(self):
        item = DataStoreAwsFixture.create_item_with_hash_key_and_range_key(self.__HASH_KEY, 'random', self.__RANGE_KEY,
                                                                           'random')
        self.__dynamodb_datastore.put_element(self.__TABLE_NAME, item)

        result = self.__dynamodb_datastore.get_element(self.__TABLE_NAME, key={
            self.__HASH_KEY: 'random',
            self.__RANGE_KEY: 'random'
        })
        self.assertEqual(result, item)

    def test_given_dynamodb_table_when_call_query_then_elemens_are_returned(self):
        items = list()
        items.append(
            DataStoreAwsFixture.create_item_with_hash_key_and_range_key(self.__HASH_KEY, '1', self.__RANGE_KEY, '1'))
        items.append(
            DataStoreAwsFixture.create_item_with_hash_key_and_range_key(self.__HASH_KEY, '1', self.__RANGE_KEY, '2'))
        items.append(
            DataStoreAwsFixture.create_item_with_hash_key_and_range_key(self.__HASH_KEY, '1', self.__RANGE_KEY, '3'))

        self.__dynamodb_datastore.put_elements(self.__TABLE_NAME, items)

        query_params = [
            {
                'field_name': self.__HASH_KEY,
                'value': '1',
                'operator': 'eq'
            },
            {
                'field_name': self.__RANGE_KEY,
                'value': '1',
                'operator': 'gt'
            }
        ]

        result = self.__dynamodb_datastore.query(self.__TABLE_NAME, query_params)

        self.assertEqual(len(result), 2)

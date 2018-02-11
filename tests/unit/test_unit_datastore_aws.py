import unittest

from mock import MagicMock, patch
from x_cloud_py.datastore.datastore_aws import DynamoDBDataStore
from tests.fixtures.datastore_aws_fixture import DataStoreAwsFixture
from botocore.exceptions import ClientError

from boto3.dynamodb.table import BatchWriter


class TestUnitDataStoreAws(unittest.TestCase):
    __dynamodb_table = None
    __boto_table_mock = None
    __HASH_KEY_ITEM_NAME = 'hash_key'
    __HASH_KEY_ITEM_VALUE = 'es#28529#48#b#p'
    __RANGE_KEY_ITEM_VALUE = '2015-05'
    __TABLE_NAME = 'PythonFrameworkUnitTest'
    __AVG_ITEM_VALUE = 40
    __TXS_ITEM_VALUE = 2
    __INFO_ITEM_VALUE = {'name': 'test merchants'}
    __READ_CAPACITY_UNITS = 10
    __WRITE_CAPACITY_UNITS = 5

    def setUp(self):
        boto_client_patch = patch('boto3.client')
        self.__boto_client_mock = boto_client_patch.start()
        boto_client_resource = patch('boto3.resource')
        self.__boto_resource_mock = boto_client_resource.start()

    def tearDown(self):
        self.__boto_client_mock.stop()
        self.__boto_resource_mock.stop()

    def test_given_valid_item_when_put_element_then_item_is_inserted(self):
        self.__boto_resource_mock.return_value.Table.return_value.put_item = MagicMock()
        dynamodb_datastore = DynamoDBDataStore()
        item = DataStoreAwsFixture.create_valid_item()
        dynamodb_datastore.put_element(self.__TABLE_NAME, item)
        self.__boto_resource_mock.return_value.Table.return_value.put_item.assert_called_with(Item=item)

    def test_given_invalid_item_when_put_element_then_exception_is_throw(self):
        self.__boto_resource_mock.return_value.Table.return_value.put_item = MagicMock(
            side_effect=DataStoreAwsFixture.get_client_error_validation_exception_put_element())
        self.assertRaises(ClientError, DynamoDBDataStore().put_element, self.__TABLE_NAME, {})

    def test_given_valid_table_id_and_valid_key_when_get_element_then_element_is_return(self):
        item = DataStoreAwsFixture.create_item_with_hash_key_and_range_key(hash_key=self.__HASH_KEY_ITEM_NAME,
                                                                           hash_key_value=self.__HASH_KEY_ITEM_VALUE)
        self.__boto_resource_mock.return_value.Table.return_value.get_item = MagicMock(
            return_value=DataStoreAwsFixture.get_dynamodb_item_response(item))

        key = {self.__HASH_KEY_ITEM_NAME: self.__HASH_KEY_ITEM_VALUE}
        result = DynamoDBDataStore().get_element(table_name=self.__TABLE_NAME, key=key)
        self.assertEqual(item, result)
        self.__boto_resource_mock.return_value.Table.return_value.get_item.assert_called_with(Key=key)

    def test_given_valid_table_id_and_invalid_key_when_get_element_then_element_is_not_return(self):
        self.__boto_resource_mock.return_value.Table.return_value.get_item = MagicMock(
            return_value={})

        key = {self.__HASH_KEY_ITEM_NAME: 'not_exists'}
        result = DynamoDBDataStore().get_element(table_name=self.__TABLE_NAME, key=key)
        self.assertEqual({}, result)
        self.__boto_resource_mock.return_value.Table.return_value.get_item.assert_called_with(Key=key)

    def test_given_valid_params_when_call_update_throughput_then_table_is_updated(self):
        self.__boto_resource_mock.return_value.Table.return_value.update = MagicMock()
        DynamoDBDataStore().update_throughput(table_name=self.__TABLE_NAME, read_capacity_units=10,
                                              write_capacity_units=10)
        self.__boto_resource_mock.return_value.Table.return_value.update.assert_called_with(
            **DataStoreAwsFixture.get_update_throughput_request(10, 10))

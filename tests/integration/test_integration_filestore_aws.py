from localstack.services import infra
from x_cloud_py.filestore.filestore_aws import S3FileStore
import unittest


class TestUnitFileStoreAws(unittest.TestCase):

    __BUCKET_NAME = 'python-bucket-test'

    def setUp(self):
        infra.start_infra(async=False,apis=list('S3'))
        self.S3_FileStore = S3FileStore({'endpoint-url':'http://localhost:4572'})

    def tearDown(self):
        infra.stop_infra()

    def test_given_valid_bucket_name_when_call_create_bucket_then_bucket_is_created(self):
        self.S3_FileStore.create_bucket(self.__BUCKET_NAME)

    def test_given_valid_key_when_call_put_object_then_objetc_is_put(self):
        self.S3_FileStore.put_object(self.__BUCKET_NAME,'test-key',b'object')

    def test_given_valid_key_for_a_object_that_exist_when_call_get_object_then_object_is_return(self):
        obj = b'object'
        self.S3_FileStore.put_object(self.__BUCKET_NAME, 'test-key-2', obj)
        response = self.S3_FileStore.get_object(self.__BUCKET_NAME, 'test-key-2').read()
        self.assertEqual(obj,response)

    def test_given_valid_prefix_when_call_list_object_then_keys_are_returned(self):
        obj = b'object'
        self.S3_FileStore.put_object(self.__BUCKET_NAME, 'list/test-key-2', obj)
        self.S3_FileStore.put_object(self.__BUCKET_NAME, 'list/test-key-3', obj)

        response = self.S3_FileStore.list_files(self.__BUCKET_NAME,'list/')
        self.assertEqual(2,len(response))
        self.assertTrue('list/test-key-2' in response)

from localstack.services import infra
from x_cloud_py.filestore.filestore_aws import S3FileStore
import unittest
import tempfile
from botocore.exceptions import ClientError


class TestUnitFileStoreAws(unittest.TestCase):
    __BUCKET_NAME = 'python-bucket-test'
    __FILE_KEY = 'test-key'
    __FILE_CONTENT = b'object'

    @classmethod
    def setUpClass(cls):
        infra.start_infra(async=False, apis=list('S3'))
        cls.S3_FileStore = S3FileStore({'endpoint-url': 'http://localhost:4572'})

    @classmethod
    def tearDownClass(cls):
        infra.stop_infra()

    def test_001_given_valid_bucket_name_when_call_create_bucket_then_bucket_is_created(self):
        self.S3_FileStore.create_bucket(self.__BUCKET_NAME)

    def test_002_given_valid_key_when_call_put_object_then_objetc_is_put(self):
        self.S3_FileStore.put_object(self.__BUCKET_NAME, self.__FILE_KEY, self.__FILE_CONTENT)

    def test_003_given_valid_key_for_a_object_that_exist_when_call_get_object_then_object_is_return(self):
        obj = b'object'
        self.S3_FileStore.put_object(self.__BUCKET_NAME, 'test-key-2', obj)
        response = self.S3_FileStore.get_object(self.__BUCKET_NAME, 'test-key-2').read()
        self.assertEqual(obj, response)

    def test_004_given_valid_prefix_when_call_list_object_then_keys_are_returned(self):
        obj = b'object'
        self.S3_FileStore.put_object(self.__BUCKET_NAME, 'list/test-key-2', obj)
        self.S3_FileStore.put_object(self.__BUCKET_NAME, 'list/test-key-3', obj)

        response = self.S3_FileStore.list_files(self.__BUCKET_NAME, 'list/')
        self.assertEqual(2, len(response))
        self.assertTrue('list/test-key-2' in response)

    def test_005_given_valid_file_when_call_upload_fileobj_then_file_is_uploaded(self):
        temp = tempfile.TemporaryFile()
        temp.write(b'file')
        temp.flush()
        temp.seek(0)
        self.S3_FileStore.upload_fileobj(self.__BUCKET_NAME, 'temp-key', temp)

        temp2 = tempfile.TemporaryFile()
        self.S3_FileStore.download_fileobj(self.__BUCKET_NAME, 'temp-key', temp2)
        temp2.flush()
        temp2.seek(0)
        self.assertEquals(b'file', temp2.read(100))
        temp.close()
        temp2.close()

    def test_006_given_invalid_key_when_call_download_fileobj_then_then_exception_is_throw(self):
        temp2 = tempfile.TemporaryFile()
        self.assertRaises(ClientError, self.S3_FileStore.download_fileobj, self.__BUCKET_NAME, 'invalid-key', temp2)
        temp2.close()

    def test_007_given_already_exist_bucket_when_call_create_bucket_then_exception_is_throw(self):
        self.assertRaises(ClientError, self.S3_FileStore.create_bucket, 'test')

    def test_008_given_valid_path_when_call_donwload_file_then_file_is_downloaded(self):
        temp = tempfile.NamedTemporaryFile()
        self.S3_FileStore.download_file(self.__BUCKET_NAME, self.__FILE_KEY, temp.name)
        with open(temp.name, 'rb') as file:
            self.assertEqual(file.read(100), self.__FILE_CONTENT)
        temp.close()

    def test_009_given_not_exist_key_when_call_get_object_then_exception_is_throw(self):
        self.assertRaises(ClientError, self.S3_FileStore.get_object, self.__BUCKET_NAME, 'not-exists')

    def test_010_given_not_exist_key_when_call_download_file_then_exception_is_throw(self):
        self.assertRaises(ClientError, self.S3_FileStore.download_file, self.__BUCKET_NAME, 'not-exists', '/tmp')

    def test_011_given_path_with_no_element_when_call_list_files_then_return_empty_list(self):
        response = self.S3_FileStore.list_files(self.__BUCKET_NAME, 'no-elements')
        self.assertEqual(len(response), 0)

    def test_012_given_invalid_bucket_when_call_list_files_then_exception_is_throw(self):
        self.assertRaises(ClientError, self.S3_FileStore.list_files, 'invalid-bucket', 'no-elements')

    def test_013_given_valid_key_when_call_delete_object_then_object_is_deleted(self):
        self.S3_FileStore.delete_object(self.__BUCKET_NAME, self.__FILE_KEY)
        self.assertRaises(ClientError, self.S3_FileStore.get_object, self.__BUCKET_NAME, self.__FILE_KEY)

    def test_014_given_not_exist_key_when_call_delete_object_then_exception_is_not_throw(self):
        self.S3_FileStore.delete_object(self.__BUCKET_NAME, 'invalid-key')

    def test_015_given_not_exist_bucket_when_call_delete_object_then_exception_is_throw(self):
        self.assertRaises(ClientError,self.S3_FileStore.delete_object,'invalid-bucket', 'invalid-key')

    def test_016_given_valid_bucket_when_call_delete_bucket_in_not_empty_bucket_then_exception_is_throw(self):
        self.assertRaises(ClientError,self.S3_FileStore.delete_bucket,self.__BUCKET_NAME)

    def test_017_given_empty_bucket_when_call_delete_bucket_then_bucket_is_deleted(self):
        bucket_name = 'python-empty-bucket'
        self.S3_FileStore.create_bucket(bucket_name)
        self.S3_FileStore.delete_bucket(bucket_name)
        self.assertRaises(ClientError, self.S3_FileStore.list_files, bucket_name, 'no-elements')


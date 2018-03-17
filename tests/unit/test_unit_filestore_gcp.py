import unittest

from mock import MagicMock, patch, mock
from x_cloud_py.filestore.filestore_cgp import GoogleCloudStorage
from google.cloud.exceptions import NotFound, GoogleCloudError


class TestUnitFileStoreGCP(unittest.TestCase):
    __BUCKET_NAME = 'test'

    def setUp(self):
        google_client_patch = patch('x_cloud_py.filestore.filestore_cgp.Client')
        self.__google_client_mock = google_client_patch.start()
        self.google_cloud_storage = GoogleCloudStorage()

    def tearDown(self):
        self.__google_client_mock.stop()

    def test_given_valid_bucket_name_when_call_create_bucket_then_bucket_is_created(self):
        self.google_cloud_storage.create_bucket(self.__BUCKET_NAME)
        self.__google_client_mock.return_value.create_bucket.assert_called_with(bucket_name=self.__BUCKET_NAME)

    def test_given_exist_bucket_when_call_delete_bucket_then_bucket_is_deleted(self):
        mock_bucket = MagicMock()
        self.__google_client_mock.return_value.get_bucket.return_value = mock_bucket
        self.google_cloud_storage.delete_bucket(self.__BUCKET_NAME)
        mock_bucket.delete.assert_called()
        self.__google_client_mock.return_value.get_bucket.assert_called_with(self.__BUCKET_NAME)

    def test_given_not_exists_bucket_when_call_delete_bucket_then_exception_is_raised(self):
        self.__google_client_mock.return_value.get_bucket.return_value.delete = MagicMock(
            side_effect=NotFound('Bucket not found'))
        self.assertRaises(NotFound, self.google_cloud_storage.delete_bucket, 'not_exists')

    @patch("x_cloud_py.filestore.filestore_cgp.Blob")
    def test_given_valid_object_when_call_put_object_then_object_is_uploaded(self, mock_blob):
        data = b'data'
        self.google_cloud_storage.put_object(self.__BUCKET_NAME, 'key', data)
        mock_blob.return_value.upload_from_string.assert_called_with(data)
        self.__google_client_mock.return_value.get_bucket.assert_called_with(self.__BUCKET_NAME)

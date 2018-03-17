from x_cloud_py.filestore.filestore_base import FileStoreBase

import logging
from google.cloud.storage.client import Client
from google.cloud.storage.blob import Blob


class GoogleCloudStorage(FileStoreBase):
    def __init__(self, *args, **kwargs):
        self.client = Client(**kwargs)

    def list_files(self, bucket_name, prefix, **kwargs):
        try:
            basic_request = {
                'prefix': prefix
            }
            list_files_request = {**basic_request, **kwargs}
            bucket = self.client.get_bucket(bucket_name)
            response_elements = bucket.list_blobs(**list_files_request)
            return [x.name for x in response_elements]
        except Exception as e:
            logging.exception(
                'Exception in [GoogleCloudStorage.list_files] with bucket_name {} and prefix {} and kwargs {}'.format(
                    bucket_name, prefix,
                    kwargs))
            raise e

    def delete_object(self, bucket_name, key, **kwargs):
        try:
            bucket = self.client.get_bucket(bucket_name)
            bucket.delete_blob(key)
        except Exception as e:
            logging.exception(
                'Exception in [GoogleCloudStorage.delete_object] with bucket_name {} and key {}'.format(
                    bucket_name, key))
            raise e

    def put_object(self, bucket_name, key, obj, **kwargs):
        try:
            bucket = self.client.get_bucket(bucket_name)
            blob = Blob(key, bucket)
            blob.upload_from_string(obj)
        except Exception as e:
            logging.exception(
                'Exception in [GoogleCloudStorage.put_object] with bucket_name {} and key {}'.format(
                    bucket_name, key))
            raise e

    def delete_bucket(self, bucket_name):
        try:
            bucket = self.client.get_bucket(bucket_name)
            bucket.delete()
        except Exception as e:
            logging.exception(
                'Exception in [GoogleCloudStorage.delete_bucket] with bucket_name {}'.format(
                    bucket_name))
            raise e

    def create_bucket(self, bucket_name, **kwargs):
        try:
            basic_request = {
                'bucket_name': bucket_name
            }
            create_bucket_request = {**basic_request, **kwargs}
            self.client.create_bucket(**create_bucket_request)
        except Exception as e:
            logging.exception(
                'Exception in [GoogleCloudStorage.create_bucket] with bucket_name {} and kwargs {}'.format(bucket_name,
                                                                                                           kwargs))
            raise e

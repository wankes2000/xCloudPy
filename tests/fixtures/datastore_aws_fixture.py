from botocore.exceptions import ClientError
import uuid


class DataStoreAwsFixture(object):
    @staticmethod
    def create_valid_item():
        return {'id': 1, 'age': 2}

    @staticmethod
    def get_update_throughput_request(read_capacity, write_capacity):
        return {
            'ProvisionedThroughput': {
                'ReadCapacityUnits': read_capacity,
                'WriteCapacityUnits': write_capacity
            }
        }

    @staticmethod
    def create_item_with_hash_key_and_range_key(hash_key, hash_key_value, range_key=None, range_key_value=None):
        item = {
            hash_key: hash_key_value,
            'random': str(uuid.uuid4())
        }
        if range_key:
            item[range_key] = range_key_value
        return item

    @staticmethod
    def get_client_error_validation_exception_put_element():
        return DataStoreAwsFixture.get_client_error_validation_exception("PutElement")

    @staticmethod
    def get_client_error_validation_exception(operation):
        error_response = {
            "Error": {
                "Code": "ValidationError",
                "Message": "The input fails to satisfy the constraints specified by an AWS service."
            }
        }
        return ClientError(error_response, operation)

    @staticmethod
    def get_dynamodb_item_response(item):
        return {
            'Item': item
        }

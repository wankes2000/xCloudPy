from x_cloud_py.datastore.datastore_base import DataStoreBase
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import logging


class CosmosDBDataStore(DataStoreBase):
    """
    """



    def __init__(self, *args, **kwargs):
        """
        Default constructor
        :param args:
        :param kwargs: dict of values that must contains connection-string
            { 'connection_string':'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;TableEndpoint=myendpoint;'}
        """
        self.table_service = TableService(**kwargs)

    def create_table(self, table_name, **kwargs):
        """
        Create table
        :param table_name: str name of the table to be created
        :param kwargs:
        :return:
        """
        try:
            self.table_service.create_table(table_name)
        except Exception as e:
            logging.exception(
                'Exception in [CosmosDBDataStore.create_table] with table_name {} '.format(table_name))
            raise e

    def update_throughput(self, table_name, read_capacity_units, write_capacity_units, **kwargs):
        pass

    def get_element(self, table_name, key, **kwargs):
        pass

    def delete_element(self, table_name, key, **kwargs):
        pass

    def query(self, table_name, query_params):
        pass

    def delete_table(self, table_name, **kwargs):
        try:
            self.table_service.delete_table(table_name)
        except Exception as e:
            logging.exception(
                'Exception in [CosmosDBDataStore.delete_table] with table_name {} '.format(table_name))
            raise e

    def get_elements(self, table_name, keys, **kwargs):
        pass

    def put_element(self, table_name, item, **kwargs):
        pass

    def put_elements(self, table_name, items, **kwargs):
        pass

    def list_tables(self):
        tables = self.table_service.list_tables(**{'num_results':10})
        return [table.name for table in tables]
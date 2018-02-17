class DatastoreGoogleFixture(object):
    @staticmethod
    def generate_multiple_items(n, key_name='key'):
        items = list()
        for x in range(0, n):
            items.append({key_name: str(x), 'age': x})
        return items

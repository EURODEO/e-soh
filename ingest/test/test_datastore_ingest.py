import json

from esoh.ingest.datastore import DatastoreConnection


def test_datastore_ingest():
    pass


if __name__ == "__main__":
    with open("test/test_data/test_payload.json") as file:
        payload = json.load(file)

    test_connection = DatastoreConnection("localhost", "50050")

    test_connection.ingest(payload)

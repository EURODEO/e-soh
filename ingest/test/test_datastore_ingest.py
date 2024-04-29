import json

from api.datastore import build_grpc_messages


def test_datastore_ingest():
    pass


if __name__ == "__main__":
    with open("test/test_data/test_payload.json") as file:
        payload = json.load(file)

    build_grpc_messages(payload)

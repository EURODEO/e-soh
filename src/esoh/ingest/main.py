from esoh.ingest.send_mqtt import mqtt_connection
from esoh.ingest.messages import messages
from esoh.ingest.datastore import datastore_connection

from jsonschema import Draft202012Validator
import json

import pkg_resources

import logging
import os

logger = logging.getLogger(__name__)


class ingest_to_pipeline():
    """
    This class should be the main interaction with this python package.
    Should accept paths or objects to pass on to the datastore and mqtt broker.
    """

    def __init__(self, mqtt_conf: dict,
                 dstore_conn: dict,
                 uuid_prefix: str,
                 testing: bool = False,
                 schema_path=None):
        self.uuid_prefix = uuid_prefix

        if not schema_path:
            self.schema_path = pkg_resources.resource_filename("esoh", "schemas")
        else:
            self.schema_path = schema_path

        esoh_mqtt_schema = os.path.join(self.schema_path, "e-soh-message-spec.json")
        with open(esoh_mqtt_schema, "r") as file:
            self.esoh_mqtt_schema = json.load(file)
        self.schema_validator = Draft202012Validator(self.esoh_mqtt_schema)

        if testing:
            return

        self.dstore = datastore_connection(dstore_conn["dshost"], dstore_conn["dsport"])
        self.mqtt = mqtt_connection(mqtt_conf["host"], mqtt_conf["topic"])

    def ingest(self, message: [str, object], input_type: str = None):
        """
        Method designed to be main interaction point with this package.
        Will interpret call all methods for deciding input type, build the mqtt messages, and
        publish them.

        """
        if not input_type:
            input_type = self.decide_input_type(message)

        self.publish_messages(self._build_messages(message, input_type))

    def publish_messages(self, messages: list):
        """
        This method accepts a list of json strings ready to be ingest to datastore
         and published to the mqtt topic.
        """
        for msg in messages:
            if msg:
                self.dstore.ingest(msg)
                self.mqtt.send_message(msg)

    def _decide_input_type(self, message) -> str:
        """
        Internal method for deciding what type of input is being provided.
        """
        match message.split(".")[-1]:
            case "nc":
                return "netCDF"
            case "bufr":
                return "bufr"
            case _:
                logger.critical(f"Unknown filetype provided. Got {message.split('.')[-1]}")
                raise ValueError(f"Unknown filetype provided. Got {message.split('.')[-1]}")

    def _build_messages(self, message: [str, object], input_type: str = None) -> list:
        """
        Internal method for calling the message building.
        """
        if not input_type:
            if isinstance(message, str):
                input_type = self._decide_input_type(message)
            else:
                logger.critical("Illegal usage, not allowed to input"
                                + "objects without specifying input type")
                raise TypeError("Illegal usage, not allowed to input"
                                + "objects without specifying input type")
        return messages(message,
                        input_type,
                        self.uuid_prefix,
                        self.schema_path,
                        self.schema_validator)

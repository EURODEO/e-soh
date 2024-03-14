import json
import logging
import os
import re
from typing import Union

import grpc
import pkg_resources
from jsonschema import Draft202012Validator

from api.datastore import DatastoreConnection
from api.messages import messages
from api.send_mqtt import MQTTConnection

logger = logging.getLogger(__name__)


class IngestToPipeline:
    """
    This class should be the main interaction with this python package.
    Should accept paths or objects to pass on to the datastore and mqtt broker.
    """

    def __init__(
        self,
        mqtt_conf: dict,
        dstore_conn: dict,
        uuid_prefix: str,
        testing: bool = False,
        schema_path=None,
        schema_file=None,
    ):
        self.uuid_prefix = uuid_prefix

        if not schema_path:
            self.schema_path = pkg_resources.resource_filename("src", "schemas")
        else:
            self.schema_path = schema_path
        if not schema_file:
            self.schema_file = "e-soh-message-spec.json"
        else:
            self.schema_file = schema_file
        esoh_mqtt_schema = os.path.join(self.schema_path, self.schema_file)

        with open(esoh_mqtt_schema, "r") as file:
            self.esoh_mqtt_schema = json.load(file)
        self.schema_validator = Draft202012Validator(self.esoh_mqtt_schema)

        if testing:
            return

        self.dstore = DatastoreConnection(dstore_conn["dshost"], dstore_conn["dsport"])
        if "username" in mqtt_conf:
            self.mqtt = MQTTConnection(
                mqtt_conf["host"], mqtt_conf["topic"], mqtt_conf["username"], mqtt_conf["password"]
            )
        else:
            self.mqtt = MQTTConnection(mqtt_conf["host"], mqtt_conf["topic"])

    def ingest(self, message: Union[str, object], input_type: str = None):
        """
        Method designed to be main interaction point with this package.
        Will interpret call all methods for deciding input type, build the mqtt messages, and
        publish them.

        """
        if not input_type:
            input_type = self._decide_input_type(message)
        messages = self._build_messages(message, input_type)
        if isinstance(messages, list):
            response, status = self.publish_messages(messages)
            return response, status
        else:
            return messages, 400

    def publish_messages(self, messages: list):
        """
        This method accepts a list of json strings ready to be ingest to datastore
         and published to the mqtt topic.
        """
        for msg in messages:
            if msg:
                try:
                    self.dstore.ingest(msg)
                    self.mqtt.send_message(msg)
                except grpc.RpcError as v_error:
                    # self.dstore.is_channel_ready()
                    return "Failed to ingest" + "\n" + str(v_error), 500
                except Exception as e:
                    return "Failed to ingest" + "\n" + str(e), 500
        return "succesfully published", 200

    def _decide_input_type(self, message) -> str:
        """
        Internal method for deciding what type of input is being provided.
        """
        file_name = os.path.basename(message)
        if re.match(r"data[0-9][0-9][0-9][05]$", file_name):
            return "bufr"
        match message.split(".")[-1].lower():
            case "nc":
                return "netCDF"
            case "bufr" | "buf":
                return "bufr"
            case _:
                logger.critical(f"Unknown filetype provided. Got {message.split('.')[-1]}")
                raise ValueError(f"Unknown filetype provided. Got {message.split('.')[-1]}")

    def _build_messages(self, message: Union[str, object], input_type: str = None) -> list:
        """
        Internal method for calling the message building.
        """
        if not input_type:
            if isinstance(message, str):
                input_type = self._decide_input_type(message)
            else:
                logger.critical("Illegal usage, not allowed to input" + "objects without specifying input type")
                raise TypeError("Illegal usage, not allowed to input" + "objects without specifying input type")
        return messages(message, input_type, self.uuid_prefix, self.schema_path, self.schema_validator)

import json
import logging
import os
import re
from typing import Union

import grpc
import pkg_resources
from fastapi import HTTPException
from jsonschema import Draft202012Validator

from api.datastore import ingest
from api.messages import messages
from api.send_mqtt import MQTTConnection

logger = logging.getLogger(__name__)


class IngestToPipeline:
    """
    This class should be the main interaction with this python package.
    Should accept paths or objects to pass on to mqtt broker.
    """

    def __init__(
        self,
        mqtt_conf: dict,
        uuid_prefix: str,
        schema_path=None,
        schema_file=None,
    ):
        self.mqtt = None
        self.uuid_prefix = uuid_prefix

        if not schema_path:
            self.schema_path = pkg_resources.resource_filename("ingest", "schemas")
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

        if mqtt_conf["host"] is not None:
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
        self.publish_messages(messages)

    def publish_messages(self, messages: list):
        """
        This method accepts a list of json strings ready to be ingest to datastore
         and published to the mqtt topic.
        """
        if len(messages) > 0:
            topic = messages[0]["properties"]["naming_authority"]
        for msg in messages:
            if msg:
                try:
                    ingest(msg)
                except grpc.RpcError as e:
                    logger.error("Failed to reach datastore, " + "\n" + str(e))
                    raise HTTPException(status_code=500, detail="API could not reach datastore")

                except Exception as e:
                    logger.error("Failed to ingest to datastore, " + "\n" + str(e))
                    raise HTTPException(status_code=500, detail="Internal server error")
                if self.mqtt is not None:
                    try:
                        self.mqtt.send_message(msg, topic)
                    except Exception as e:
                        logger.error("Failed to publish to mqtt, " + "\n" + str(e))
                        raise HTTPException(
                            status_code=500, detail="Data ingested to datastore. But unable to publish to mqtt"
                        )

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
                raise HTTPException(status_code=400, detail=f"Unknown filetype provided. Got {message.split('.')[-1]}")

    def _build_messages(self, message: Union[str, object], input_type: str = None) -> list:
        """
        Internal method for calling the message building.
        """
        if not input_type:
            if isinstance(message, str):
                input_type = self._decide_input_type(message)
            else:
                logger.critical("Illegal usage, not allowed to input" + "objects without specifying input type")
                raise HTTPException(
                    status_code=400,
                    detail="Illegal usage, not allowed to input" + "objects without specifying input type",
                )
        return messages(message, input_type, self.uuid_prefix, self.schema_path, self.schema_validator)

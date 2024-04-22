import logging
import os
import re
from typing import Union

import grpc
import pkg_resources
from fastapi import HTTPException

from api.datastore import ingest
from api.messages import build_messages
from api.send_mqtt import MQTTConnection

logger = logging.getLogger(__name__)


class IngestToPipeline:
    """
    This class should accept paths or objects to pass on to mqtt broker.
    """

    def __init__(
        self,
        mqtt_conf: dict,
        uuid_prefix: str,
        schema_path=None,
    ):
        self.mqtt = None
        self.uuid_prefix = uuid_prefix

        if not schema_path:
            self.schema_path = pkg_resources.resource_filename("ingest", "schemas")
        else:
            self.schema_path = schema_path

        if mqtt_conf["host"] is not None:
            try:
                if "username" in mqtt_conf:
                    self.mqtt = MQTTConnection(mqtt_conf["host"], mqtt_conf["username"], mqtt_conf["password"])
                else:
                    self.mqtt = MQTTConnection(mqtt_conf["host"])
                logger.info("Established connection to mqtt")
            except Exception as e:
                logger.error("Failed to establish connection to mqtt, " + "\n" + str(e))
                raise HTTPException(status_code=500, detail="API failed to establish connection to mqtt")

    def ingest(self, message: Union[str, object], input_type: str = None):
        """
        This method will interpret call all methods for deciding input type, build the mqtt messages, and
        publish them.

        """
        if not input_type:
            input_type = self._decide_input_type(message)
        messages = build_messages(message, input_type, self.uuid_prefix, self.schema_path)
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
                    logger.info("Succesfully ingested to datastore")
                except grpc.RpcError as e:
                    logger.error("Failed to reach datastore, " + "\n" + str(e))
                    raise HTTPException(status_code=500, detail="API could not reach datastore")

                except Exception as e:
                    logger.error("Failed to ingest to datastore, " + "\n" + str(e))
                    raise HTTPException(status_code=500, detail="Internal server error")
                if self.mqtt is not None:
                    try:
                        self.mqtt.send_message(msg, topic)
                        logger.info("Succesfully published to mqtt")
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

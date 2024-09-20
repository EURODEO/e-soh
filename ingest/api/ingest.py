import logging
from typing import Union

import grpc

from fastapi import HTTPException

from api.datastore import build_grpc_messages
from api.messages import build_messages
from api.send_mqtt import connect_mqtt, send_message
from api.grpc_putter import putObsRequest

import datastore_pb2 as dstore

logger = logging.getLogger(__name__)


class IngestToPipeline:
    """
    This class should accept paths or objects to pass on to mqtt broker.
    """

    def __init__(
        self,
        mqtt_conf: dict,
        uuid_prefix: str,
    ):

        self.uuid_prefix = uuid_prefix
        self.client = None
        if mqtt_conf["host"]:
            try:
                self.client = connect_mqtt(mqtt_conf)
            except Exception as e:
                logger.error("Failed to establish connection to mqtt, " + "\n" + str(e))
                raise e

    def convert_to_meter(self, level: int) -> str:

        level = str(float(level) / 100)

    async def ingest(self, message: Union[str, object]):
        """
        This method will interpret call all methods for deciding input type, build the mqtt messages, and
        publish them.

        """
        messages = build_messages(message, self.uuid_prefix)
        await self.publish_messages(messages)

    async def publish_messages(self, messages: list):
        """
        This method accepts a list of json strings ready to be ingest to datastore
         and published to the mqtt topic.
        """
        obs_requests = dstore.PutObsRequest(observations=[build_grpc_messages(msg) for msg in messages])
        try:
            await putObsRequest(obs_requests)
            logger.debug("Succesfully ingested to datastore")
        except grpc.RpcError as e:
            logger.error("Failed to reach datastore, " + "\n" + str(e))
            raise HTTPException(status_code=500, detail="API could not reach datastore")

        if self.client is not None:

            for msg in messages:
                topic = (
                    msg["properties"]["naming_authority"]
                    + "/"
                    + msg["properties"]["platform"]
                    + "/"
                    + msg["properties"]["content"]["standard_name"]
                )

                # modify the period back to iso format and level back to meter
                period_iso = msg["properties"]["period_convertable"]
                level_string = self.convert_to_meter(msg["properties"]["level"])
                msg["properties"]["level"] = level_string
                msg["properties"]["period"] = period_iso
                try:
                    send_message(topic, msg, self.client)
                    logger.info("Succesfully published to mqtt")
                except Exception as e:
                    logger.error("Failed to publish to mqtt, " + "\n" + str(e))
                    raise HTTPException(
                        status_code=500, detail="Data ingested to datastore. But unable to publish to mqtt"
                    )

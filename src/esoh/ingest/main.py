from esoh.ingest.send_mqtt import mqtt_connection
from esoh.ingest.messages import load_files, build_message


import xarray as xr


class ingest_to_pipeline():
    """
    This class should be the main interaction with this python package.
    Should accept paths or objects to pass on to the datastore and mqtt broker.
    """

    def __init__(self, mqtt_conf: dict, uuid_prefix: str, testing: bool = False):
        self.uuid_prefix = uuid_prefix

        if testing:
            return

        self.mqtt = mqtt_connection(mqtt_conf["host"], mqtt_conf["topic"])

    def ingest_message(self, message: [str, object], input_type: str = None):
        if not input_type:
            input_type = self.decide_input_type(message)

        return self.build_message(message, input_type)

    def decide_input_type(self):
        pass

    def build_messages(self, message: [str, object], input_type: str = None):
        match input_type:
            case "netCDF":
                if isinstance(message, str):
                    return load_files(message, input_type=input_type, uuid_prefix=self.uuid_prefix)
                elif isinstance(message, xr.Dataset):
                    return build_message(message,
                                         input_type=input_type,
                                         uuid_prefix=self.uuid_prefix)
                else:
                    raise TypeError("Unknown netCDF type, expected path"
                                    + f"or xarray.Dataset, got {type(message)}")
            case "bufr":
                raise NotImplementedError("Handeling of bufr not implemented")

    def publish_messages(self):
        pass

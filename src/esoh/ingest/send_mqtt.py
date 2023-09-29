import paho.mqtt.client as mqtt


class mqtt_connection():
    def __init__(self, mqtt_host, mqtt_topic):
        self.mqtt_host = mqtt_host
        self.mqtt_topic = mqtt_topic

        # Initiate MQTT Client
        self.pub_client = mqtt.Client(client_id="")

        # Connect with MQTT Broker
        self.pub_client.connect(self.mqtt_host)

    def send_message(self, message: str):
        try:
            self.pub_client.publish(self.mqtt_topic, message)
        except Exception as e:
            print(e.with_traceback())

from paho.mqtt import client as mqtt_client
import random
import time

broker = ''
port = 
# client_id = f'python-mqtt-{random.randint(0, 100)}'
# generate client ID with pub prefix randomly
username = ''
password = ''


def connect_mqtt(client_id) -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker! {client_id}")
        else:
            print("Failed to connect, return code %d\n", rc)
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client, topic):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        
    client.subscribe(topic)
    client.on_message = on_message


def publish(client):
    msg_count = 0

    time.sleep(1)
    msg = f"messages: {msg_count}"
    result = client.publish("python-mqtt-3", msg)

    # result: [0, 1]
    # status = result[0]
    # if status == 0:
    # print(f"Send `{msg}` to topic `{python-mqtt-3}`")
    # else:
    # print(f"Failed to send message to topic {python-mqtt-3}")
    msg_count += 1


def run():
    i = 0
    while (i < 7000):
        client_id = f'python-mqtt-{i}'
        client = connect_mqtt(client_id)
        topic = f'python/mqtt-{i}'
        subscribe(client, topic)
        client.loop_start()
        i += 1

    while (1):
        publish(client)
        time.sleep(1)


if __name__ == '__main__':
    run()

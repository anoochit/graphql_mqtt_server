import network
import time
import ujson
from umqtt.simple import MQTTClient
import machine

# WiFi Configuration
WIFI_SSID = "your_wifi_ssid"
WIFI_PASSWORD = "your_wifi_password"

# MQTT Configuration
MQTT_BROKER = "your_mqtt_broker_ip"  # e.g., "192.168.1.100" or "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "esp32_client_" + str(machine.unique_id())
MQTT_TOPIC = "your/topic/here"  # Change this to your desired topic
MQTT_USER = None  # Set to your username if authentication is required
MQTT_PASSWORD = None  # Set to your password if authentication is required
# LED for status indication (optional)
led = machine.Pin(2, machine.Pin.OUT)


def connect_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)

    if not wlan.isconnected():
        print("Connecting to WiFi...")
        wlan.connect(WIFI_SSID, WIFI_PASSWORD)

        timeout = 10
        while not wlan.isconnected() and timeout > 0:
            time.sleep(1)
            timeout -= 1

        if wlan.isconnected():
            print("WiFi connected!")
            print("Network config:", wlan.ifconfig())
            return True
        else:
            print("WiFi connection failed!")
            return False
    else:
        print("WiFi already connected")
        return True


def mqtt_callback(topic, msg):
    """Callback for received MQTT messages"""
    print(f"Received message on topic '{topic.decode()}': {msg.decode()}")

    # Blink briefly to indicate message reception
    led.on()
    time.sleep(0.1)
    led.off()

    try:
        # Parse JSON message
        data = ujson.loads(msg)
        if "content" in data:
            message = data["content"].strip()
        else:
            print("No 'content' key in message")
            return
    except ValueError:
        print("Message is not valid JSON")
        return

    # Process content command
    if message.upper() == "LED_ON":
        led.on()
        print("LED turned ON")
    elif message.upper() == "LED_OFF":
        led.off()
        print("LED turned OFF")
    else:
        print(f"Unknown content command: {message}")


def connect_mqtt():
    try:
        if MQTT_USER and MQTT_PASSWORD:
            client = MQTTClient(
                MQTT_CLIENT_ID,
                MQTT_BROKER,
                port=MQTT_PORT,
                user=MQTT_USER,
                password=MQTT_PASSWORD,
            )
        else:
            client = MQTTClient(MQTT_CLIENT_ID, MQTT_BROKER, port=MQTT_PORT)

        client.set_callback(mqtt_callback)
        print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        client.connect()
        print("Connected to MQTT broker!")

        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")

        return client

    except Exception as e:
        print(f"MQTT connection error: {e}")
        return None


def main():
    print("ESP32 MQTT Subscriber Starting...")

    if not connect_wifi():
        print("Cannot proceed without WiFi connection")
        return

    client = connect_mqtt()
    if not client:
        print("Cannot proceed without MQTT connection")
        return

    try:
        print("Listening for messages... (Press Ctrl+C to stop)")
        while True:
            try:
                client.check_msg()
                time.sleep(0.1)
            except OSError as e:
                print(f"MQTT error: {e}")
                print("Attempting to reconnect...")
                try:
                    client.disconnect()
                except:
                    pass
                time.sleep(5)
                client = connect_mqtt()
                if not client:
                    break
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        try:
            client.disconnect()
            print("Disconnected from MQTT broker")
        except:
            pass


if __name__ == "__main__":
    main()

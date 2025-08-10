import network
import time
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
    """Connect to WiFi network"""
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    
    if not wlan.isconnected():
        print('Connecting to WiFi...')
        wlan.connect(WIFI_SSID, WIFI_PASSWORD)
        
        # Wait for connection
        timeout = 10
        while not wlan.isconnected() and timeout > 0:
            time.sleep(1)
            timeout -= 1
            
        if wlan.isconnected():
            print('WiFi connected!')
            print('Network config:', wlan.ifconfig())
            return True
        else:
            print('WiFi connection failed!')
            return False
    else:
        print('WiFi already connected')
        return True

def mqtt_callback(topic, msg):
    """Callback function called when a message is received"""
    print(f"Received message on topic '{topic.decode()}': {msg.decode()}")
    
    # Blink LED when message received
    led.on()
    time.sleep(0.1)
    led.off()
    
    # Process your message here
    try:
        message = msg.decode()
        # Add your message processing logic here
        # Example: if message == "LED_ON": turn on LED
        if message.upper() == "LED_ON":
            led.on()
            print("LED turned ON")
        elif message.upper() == "LED_OFF":
            led.off()
            print("LED turned OFF")
        else:
            print(f"Processing message: {message}")
            
    except Exception as e:
        print(f"Error processing message: {e}")

def connect_mqtt():
    """Connect to MQTT broker and subscribe to topic"""
    try:
        # Create MQTT client
        if MQTT_USER and MQTT_PASSWORD:
            client = MQTTClient(MQTT_CLIENT_ID, MQTT_BROKER, port=MQTT_PORT, 
                              user=MQTT_USER, password=MQTT_PASSWORD)
        else:
            client = MQTTClient(MQTT_CLIENT_ID, MQTT_BROKER, port=MQTT_PORT)
        
        # Set callback function
        client.set_callback(mqtt_callback)
        
        # Connect to broker
        print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        client.connect()
        print("Connected to MQTT broker!")
        
        # Subscribe to topic
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
        
        return client
        
    except Exception as e:
        print(f"MQTT connection error: {e}")
        return None

def main():
    """Main function"""
    print("ESP32 MQTT Subscriber Starting...")
    
    # Connect to WiFi
    if not connect_wifi():
        print("Cannot proceed without WiFi connection")
        return
    
    # Connect to MQTT
    client = connect_mqtt()
    if not client:
        print("Cannot proceed without MQTT connection")
        return
    
    # Main loop
    try:
        print("Listening for messages... (Press Ctrl+C to stop)")
        while True:
            try:
                # Check for new messages
                client.check_msg()
                
                # Optional: publish a heartbeat message every 30 seconds
                # client.publish("heartbeat", "ESP32 alive")
                
                time.sleep(0.1)  # Small delay to prevent excessive CPU usage
                
            except OSError as e:
                print(f"MQTT error: {e}")
                print("Attempting to reconnect...")
                
                # Try to reconnect
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
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Cleanup
        try:
            client.disconnect()
            print("Disconnected from MQTT broker")
        except:
            pass

# Run the program
if __name__ == "__main__":
    main()
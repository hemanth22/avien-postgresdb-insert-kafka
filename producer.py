from kafka import KafkaProducer
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
import requests
import os
import json

#requesturl = os.environ.get('KAFKA_RECEIVER_SERVERLESS')

# Kafka configuration
bootstrap_servers_sv1 = os.environ.get('BOOTSTRAP_SERVER_NAME')
sasl_mechanism_sv1 = os.environ.get('SASL_MECH')
security_protocol_sv1 = os.environ.get('SSL_SEC')
sasl_plain_username_sv1 = os.environ.get('SASL_USERNAME')
sasl_plain_password_sv1 = os.environ.get('SASL_PASSD')

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers_sv1,
    sasl_mechanism=sasl_mechanism_sv1,
    security_protocol=security_protocol_sv1,
    sasl_plain_username=sasl_plain_username_sv1,
    sasl_plain_password=sasl_plain_password_sv1,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'insert_to_database'

# Function to serialize data to JSON string
def serialize_data(data):
    """
    Serialize the given data to a JSON string.

    Args:
        data (dict): The data to serialize

    Returns:
        str: The serialized JSON string
    """
    return json.dumps(data)

def send_dataToFastAPI(url):
    response = requests.get(url)
    if response.status_code == 200:
        print("Request sent successfully")

    if response.status_code != 200:
        print(f"Failed to send data. Status code: {response.status_code}")


json_message_to_send = {
    "message_date": "17-01-2025",
    "message": "Testing the API with backdate 3"
}

try:
    formatted_output = json.dumps(json_message_to_send, ensure_ascii=False, indent=4)
    print(formatted_output)
    producer.send(topic_name, value=formatted_output)
    print(f"Produced: {formatted_output}")
except Exception as e:
    print(f"Error Message: {e}")

producer.flush()
producer.close()
print("Producer Job Completed")
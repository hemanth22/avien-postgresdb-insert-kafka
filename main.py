from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer
from pydantic import BaseModel
import psycopg2
from psycopg2 import sql
import os
import json
import requests
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
from typing import List
import logging

connection = None
cursor = None
# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

# Define FastAPI app
app = FastAPI()

# Define the request model
class Reminder(BaseModel):
    message_date: str  # Format: DD-MM-YYYY
    message: str

# PostgreSQL connection details
DB_HOST = os.environ.get('postgres_hostname')
DB_NAME = os.environ.get('postgres_database')
DB_PORT = os.environ.get('postgres_port')
DB_USER = os.environ.get('postgres_username')
DB_PASSWORD = os.environ.get('postgres_password')

FASTAPI_WEBHOOK_SERVERLESS = os.environ.get('FASTAPI_WEBHOOK_SERVER')
bootstrap_servers_sv1 = os.environ.get('BOOTSTRAP_SERVER_NAME')
sasl_mechanism_sv1 = os.environ.get('SASL_MECH')
security_protocol_sv1 = os.environ.get('SSL_SEC')
sasl_plain_username_sv1 = os.environ.get('SASL_USERNAME')
sasl_plain_password_sv1 = os.environ.get('SASL_PASSD')


#def send_dataToFastAPI(payload):
#    headers = {
#        'Content-Type': 'application/json',
#        }
#    response = requests.request("POST", url=FASTAPI_WEBHOOK_SERVERLESS, headers=headers, data=payload)
#    print(response.text)
#    if response.status_code == 200:
#        print("Data sent successfully")

#    if response.status_code != 200:
#        print(f"Failed to send data. Status code: {response.status_code}")

def serialize_data(data):
    """
    Serialize the given data to a JSON string.

    Args:
        data (dict): The data to serialize

    Returns:
        str: The serialized JSON string
    """
    return json.dumps(data)

topic_name = 'insert_to_database'

def add_reminder(json_reminder):
    try:
        # Deserialize if input is a string
        if isinstance(json_reminder, str):
            logger.debug("Deserializing input JSON string.")
            json_reminder = json.loads(json_reminder)
        
        message_date = json_reminder.get("message_date")
        message = json_reminder.get("message")

        if not message_date or not message:
            logger.error("Invalid input: 'message_date' or 'message' missing.")
            raise ValueError("Both 'message_date' and 'message' must be provided in the input JSON.")

        # Connect to the PostgreSQL database
        logger.info("Connecting to PostgreSQL database.")
        connection = psycopg2.connect(
            host=DB_HOST, 
            database=DB_NAME,
            user=DB_USER, 
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = connection.cursor()
        logger.debug("Database connection established successfully.")
        
        # Insert the reminder into the database
        insert_query = """
            INSERT INTO remainder_messages (message_date, message) 
            VALUES (TO_DATE(%s, 'DD-MM-YYYY'), %s)
        """
        cursor.execute(insert_query, (message_date, message))
        logger.info("Reminder added to the database.")
        
        # Commit the transaction
        connection.commit()
        logger.debug("Transaction committed successfully.")
        return {"status": "success", "detail": "Reminder added successfully"}
    
    except Exception as e:
        logger.critical(f"Critical error occurred: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
    finally:
        # Close the connection
        if cursor:
            cursor.close()
            logger.debug("Database cursor closed.")
        if connection:
            connection.close()
            logger.debug("Database connection closed.")


@app.get("/messages")
async def get_messages():
    consumer = KafkaConsumer(
        'insert_to_database',
        bootstrap_servers=bootstrap_servers_sv1,
        sasl_mechanism=sasl_mechanism_sv1,
        security_protocol=security_protocol_sv1,
        sasl_plain_username=sasl_plain_username_sv1,
        sasl_plain_password=sasl_plain_password_sv1,
        group_id='$GROUP_NAME',
        auto_offset_reset='earliest',
        consumer_timeout_ms=60000,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    messages = []
    try:
        for message in consumer:
            json_data = message.value
            print(f"Received raw json message: {json_data}")
            add_reminder(json_data)
            messages.append(json_data)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
    return serialize_data(messages)


@app.on_event("startup")
def startup_event():
    logger.debug("Initialization Completed.")

@app.on_event("shutdown")
def shutdown_event():
    logger.debug("Shutting down FastAPI application.")

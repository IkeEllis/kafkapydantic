# Databricks notebook source
# MAGIC %pip install -r "/Workspace/Repos/ike.ellis@indr.com/indr-data-team/spikes/kafka pydantic spike/requirements.txt"
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import logging
import os
import sys

# Allows imports from pythons
base_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get(),
)
path = os.path.normpath(os.path.join(base_dir, ".."))
path = path if path.startswith("/Workspace") else "/Workspace" + path

sys.path.append(path)

scope = "data-ai-kafka"

kafka_api_key = dbutils.secrets.get(scope=scope, key="API_KEY")
kafka_secret = dbutils.secrets.get(scope=scope, key="SECRET")
kafka_bootstrap_server = dbutils.secrets.get(
    scope=scope,
    key="BOOTSTRAP_SERVER",
)

# COMMAND ----------

import re
from abc import ABC, abstractmethod
from typing import Literal, Optional

from pydantic import BaseModel

"""
Base Class for All Kafka Event Handlers
"""


class KafkaEventHandler(ABC, BaseModel):
    """
    Base class for Kafka Event Handlers

    Any event that does not implement resolve will throw a bug when attempted to instantiate
    'TypeError: Can't instantiate abstract class <Class> with abstract method resolve'
    """
    event: str

    @abstractmethod
    def resolve(self):
        pass


class HealthCheckHandler(KafkaEventHandler):
    event: Literal["health"]

    def resolve(self):
        return {
            "status": "Kafka is healthy!",
        }

class SummarizeHandler(KafkaEventHandler):
    event: Literal["summarize"]
    verbose_text: str

    def resolve(self):
        response = "We are summarizing this text: " + self.verbose_text
        return response




class ConcatTwoStringsHandler(KafkaEventHandler):
    event: Literal["concatTwoStrings"]
    first_string: str
    second_string: str

    def resolve(self):
       
        response = self.first_string + " " + self.second_string

        return response


# COMMAND ----------

from typing import Annotated, Union, Any, Optional

from pydantic import Field, TypeAdapter, BaseModel, ValidationError

"""
Compose a Discriminated Union
"""
EventsUnion = Union[
    HealthCheckHandler, 
    SummarizeHandler,
    ConcatTwoStringsHandler,
    ]

EventHandlers = Annotated[
    EventsUnion,
    Field(discriminator="event"),
]

adapter = TypeAdapter(EventHandlers)


# COMMAND ----------




"""
Incoming Kafka Requests - pydantic BaseModel
"""


class KafkaRequest(BaseModel):
    event: str
    body: dict


def parse_request(json_req: str) -> Optional[KafkaRequest]:
    """
    Parse the Value from the request stream, validate it is an Event Request
    """
    try:
        record: KafkaRequest = KafkaRequest.model_validate_json(json_req)

        return record
    except ValidationError as e:
        return e.json()


class Response(BaseModel):
    success: bool
    response: Any = None
    error: Optional[str] = None


def get_response(req: KafkaRequest) -> Response:
    """
    Validate the Request and Return the the Handler
    With the Handler resolve the request
    """
    handler = None

    # Get the handler for the request
    try:
        # Create an adaptor to handle the request
        kafka_event = req.body
        kafka_event.update(event=req.event)

        # This will throw error if the event is not valid
        handler = adapter.validate_python(kafka_event)
    except ValidationError as e:
        return Response(success=False, error=e.json())

    # Resolve the request
    try:
        # Call the resolve method to get the response
        response = handler.resolve()
        return Response(success=True, response=response)
    except Exception as e:
        return Response(success=False, error=f"Error in resolving the request: {e}")


# COMMAND ----------

import json

from kafka import KafkaProducer

conf = {
    "bootstrap_servers": kafka_bootstrap_server,
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": kafka_api_key,
    "sasl_plain_password": kafka_secret,
}
# uses the kafkaProducer for the write
def write_message_to_kafka(topic: str, key: str, value: dict):
    """
    Using Python Kafka write message to topic on kafka
    """
    producer = KafkaProducer(
        value_serializer=lambda m: json.dumps(m).encode(),
        key_serializer=lambda k: str.encode(k),
        **conf,
    )
    producer.send(topic, value, key)
    producer.flush()

# COMMAND ----------

from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StringType


# Kafka consumer
def get_kafka_sdf(topic: str) -> DataStreamReader:
    """
    Get the kafka read stream

    Defaults to Request
    """
    sdf = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_server)
        .option("kafka.security.protocol", "SASL_SSL")
        .option(
            "kafka.sasl.jaas.config",
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_api_key}' password='{kafka_secret}';",
        )
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("subscribe", topic)
        # "latest" , which reads only new data, and "earliest" , which reads from the beginning of the topic.
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Convert Binary to String
    sdf = sdf.withColumn("value", sdf["value"].cast(StringType()))
    sdf = sdf.withColumn("key", sdf["key"].cast(StringType()))

    return sdf

# COMMAND ----------

import os

from pyspark.sql.types import Row

environment = os.environ["ENV"]

response_topic, request_topic =  ("data_ai_response", "dev_request")


def route_request(key, request, request_timestamp) -> None:
    """
    Request Router for Kafka
    """
    parsed_request = parse_request(request)
    print(parsed_request)
    print(type(parsed_request))

    if not isinstance(parsed_request, KafkaRequest):
        # This would happen when the request is not made in the correct format
        message = {
            "error": parsed_request,
            "request": request,
            "request_timestamp": request_timestamp,
        }

        write_message_to_kafka(
            response_topic,
            key,
            message,
        )
        return

    # Add the request to response message
    request_message = parsed_request.model_dump()
    print(request_message)

    # Get the response
    response = get_response(parsed_request)

    # Handle when response is an Error
    if not response.success:
        message = {
            "error": response.error,
            "request": request_message,
            "request_timestamp": request_timestamp,
        }

        write_message_to_kafka(
            response_topic,
            key,
            message,
        )
        return

    # Write the success response to kafka
    message = {
        "request": request_message,
        "response": response.response,
        "request_timestamp": request_timestamp,
    }

    write_message_to_kafka(response_topic, key, message)



# kafka_request_sdf: DataStreamReader = get_kafka_sdf(request_topic)
# kafka_request_stream = (
#     kafka_request_sdf.writeStream.queryName(f"{environment} stream topic {request_topic}")
#     .foreach(route_request)
#     .start()
# )

# COMMAND ----------


"""
Consume Stream
"""
keyOne = "1"
valueOne = "{\"body\": {}, \"task_id\": \"health_check\", \"event\": \"health\"}"
timeStampOne = "2023-09-30 22:03:20.526"
route_request(keyOne, valueOne, timeStampOne)

keyTwo = "2"
valueTwo = "{\"body\":{\"verbose_text\": \"Summarize this please!\"}, \"task_id\":\"summarize\" , \"event\": \"summarize\"}"
timeStampTwo = "2023-09-30 22:03:20.526"
route_request(keyTwo, valueTwo, timeStampTwo)

keyTwo = "3"
valueTwo = "{\"body\":{\"first_string\": \"Brick is the best\", \"second_string\": \"debugger of all time.\"}, \"task_id\":\"concatTwoStrings\" , \"event\": \"concatTwoStrings\"}"
timeStampTwo = "2023-09-30 22:03:20.526"
route_request(keyTwo, valueTwo, timeStampTwo)

print(json.loads(valueOne))
print(json.loads(valueTwo))


# COMMAND ----------


"""
Consume Stream
"""
keyOne = "1"
valueOne = "{\"body\": {}, \"task_id\": \"health_check\", \"event\": \"ike\"}"
timeStampOne = "2023-09-30 22:03:20.526"
route_request(keyOne, valueOne, timeStampOne)

keyTwo = "2"
valueTwo = "{\"body\":{\"verbose_tex\": \"Summarize this please!\"}, \"task_id\":\"summarize\" , \"event\": \"summarize\"}"
timeStampTwo = "2023-09-30 22:03:20.526"
route_request(keyTwo, valueTwo, timeStampTwo)

keyTwo = "3"
valueTwo = "{\"body\":{\"first_string\": \"Brick is the best\"}, \"task_id\":\"concatTwoStrings\" , \"event\": \"concatTwoStrings\"}"
timeStampTwo = "2023-09-30 22:03:20.526"
route_request(keyTwo, valueTwo, timeStampTwo)

print(json.loads(valueOne))
print(json.loads(valueTwo))


import os
from datetime import datetime, timedelta

import pika
from mongoengine.queryset.visitor import Q

from proto.event_pb2 import BusEvent, UTM


def connect_amqp():
    credentials = pika.PlainCredentials(
        os.environ.get('QUEUE_USER', 'guest'),
        os.environ.get('QUEUE_PASS', 'guest')
    )
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            os.environ.get('QUEUE_ADDR', 'localhost'),
            os.environ.get('QUEUE_PORT', 5672),
            '/',
            credentials
        )
    )
    return connection


def from_mongo_to_proto(event):
    e = BusEvent()
    e.unit = event.unit
    e.timestamp = event.timestamp.isoformat()
    e.coordinate.easting = event.coordinate.easting
    e.coordinate.northing = event.coordinate.northing
    return e.SerializeToString()

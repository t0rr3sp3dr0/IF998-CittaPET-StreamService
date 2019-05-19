import os

from mongoengine import *
from mongoengine import connect

class UTM(EmbeddedDocument):
    easting = IntField(required=True)
    northing = IntField(required=True)

class BusEvent(Document):
    unit = StringField(required=True)
    timestamp = DateTimeField(required=True)
    coordinate = EmbeddedDocumentField(UTM, required=True)

def connect_db():
    db_config = {
        'host': os.environ.get('DB_ADDR', 'localhost'),
        'port': os.environ.get('DB_PORT', 27017),
        'db': os.environ.get('DB_NAME', 'test')
    }
    connect(**db_config)

def find_events(start, end):
    c = BusEvent.objects(Q(timestamp__gte=start) & Q(timestamp__lte=end))
    return c
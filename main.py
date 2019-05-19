import os
import sys
from time import sleep
from threading import Thread
from datetime import datetime, timedelta

from loader import read
from db import BusEvent, connect_db, find_events
from amqp import connect_amqp, from_mongo_to_proto

FILES = [
    'input',
    '200118_240118',
]

def main():
    connect_db()

    load_data = len([x for x in sys.argv if x == '--load'])
    if load_data:
        BusEvent.drop_collection()
        x = Thread(target=read,args=(FILES,))
        x.start()
        x.join()
        sleep(10)

    connection = connect_amqp()
    channel = connection.channel()
    channel.queue_declare(queue='events', durable=True)

    start = datetime.strptime('2018-01-20 00:05:14.557000', '%Y-%m-%d %H:%M:%S.%f')
    window = os.environ.get('SEARCH_WINDOW', 30)
    end = start + timedelta(seconds=window)
    while True:
        events = find_events(start, end)
        events = [from_mongo_to_proto(x) for x in events]
        start = end
        end = start + timedelta(seconds=window)
        for e in events:
            channel.basic_publish(exchange='', routing_key='events', body=e)
        sleep(os.environ.get('SEND_INTERVAL', 0))
    connection.close()

if __name__ == "__main__":
    main()
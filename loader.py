import csv
from threading import Thread
from datetime import datetime

from db import BusEvent, UTM, connect

def read_file(name):
    connect()
    current_file = './data/' + name + '.csv'
    with open(current_file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = -1
        for row in csv_reader:
            if line_count > -1:
                try:
                    b = BusEvent(
                        unit=row[0],
                        timestamp=datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S.%f'),
                        coordinate=UTM(easting=float(row[6]), northing=float(row[7]))
                    )
                    b.save()
                except Exception:
                    pass
            line_count += 1

def read(files):
    if not isinstance(files, list):
        files = [files]
    threads = []
    for f in files:
        x = Thread(target=read_file, args=(f,))
        x.start()
    for t in threads:
        t.join()
        
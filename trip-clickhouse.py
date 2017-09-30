#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import threading
from pyclickhouse import Connection
import time

def to_float(str):
	try:
		return float(str)
	except Exception, e:
		return .0

def worker(csv_file):
        con = Connection('localhost', 8123)
        cur = con.cursor()

        nrec = 0
        values = []
        with open(csv_file, 'rb') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            for row in reader:
                    # skip header line
                    if nrec > 0:
                        try:
                            values.append({
                                '_cx':  row[5].split(' ')[0], #time.strftime("%Y/%m/%d"),
                                'medallion':           row[0],
                                'hack_license':        row[1],
                                'vendor_id':           row[2],
                                'rate_code_id':        row[3],
                                'store_and_fwd_flag':  row[4],
                                'pickup_datetime':     row[5],
                                'dropoff_datetime':    row[6],
                                'passenger_count':     int(row[7]),
                                'trip_time_in_secs':   int(row[8]),
                                'trip_distance':       to_float(row[9]),
                                'pickup_longitude':    to_float(row[10]),
                                'pickup_latitude':     to_float(row[11]),
                                'dropoff_longitude':   to_float(row[12]),
                                'dropoff_latitude':    to_float(row[13]),
                                })
                        except Exception, e:
                            print 'values.append(%s:%d): %s, file %s, row \'%s\'' % (csv_file, nrec, str(e), csv_file, row)
                            
                    nrec += 1
                    if nrec % 1024 == 0:
                        print '%s = %d' % (csv_file, nrec)

                        try:
                            cur.bulkinsert('trips', values)
                        except Exception, e:
                            print 'bulkinsert(wts): %s' % str(e)

                        values = []

        print '%s = %d' % (csv_file, nrec)
        cur.bulkinsert('trips', values)
#        cur.close()
        con.close()
        
# create trips table
con = Connection('localhost', 8123)
cur = con.cursor()

cur.ddl("""
    drop table if exists trips
""")

cur.ddl("""
    create table trips (
        _cx                Date,
        medallion          String,
        hack_license       String,
        vendor_id          String,
        rate_code_id       String,
        store_and_fwd_flag String,
        pickup_datetime    DateTime,
        dropoff_datetime   DateTime,
        passenger_count    Int8,
        trip_time_in_secs  Int32,    -- somehow CSV files contain many "-10"
        trip_distance      Float32,
        pickup_longitude   Float32,
        pickup_latitude    Float32,
        dropoff_longitude  Float32,
        dropoff_latitude   Float32
)
engine = ReplacingMergeTree(_cx, (pickup_datetime, medallion, hack_license), 8192, dropoff_datetime)
""")

# get the list of csv files
import glob
csv_files = glob.glob('/mnt/trip/csvfiles//*.csv')
print csv_files
# main
nth = len(csv_files)
threads = []

# fork nth threads to do the work
lock = threading.Lock()
for csv_file in csv_files:
    print csv_file
    if False:
        worker(csv_file)
    else:
        t = threading.Thread(target=worker, args=[csv_file])
        threads.append(t)
        t.start()

for t in threads:
    t.join()

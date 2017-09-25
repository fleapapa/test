#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import threading
import time
import random
import os
import pymapd
from pymapd import connect

# change these per your environment
NWRITER = 2								# number of writer (LOAD   query) threads
NREADER = 5								# number of reader (SELECT query) threads
CSVPATH = '/mnt/trip/csvfiles/*.csv'	# NY taxcab trip CSV file paths

# this signals end of data loading
end_of_writes = False

# get the list of NY taxicab csv files
import glob
csv_files = glob.glob(CSVPATH)
print csv_files

# this is for synchronize writers
lck_csvfile = threading.Lock()
idx_csvfile = 0

def writer(ith):
	con = connect(user=os.environ['MAPD_USERNAME'], password=os.environ['MAPD_PASSWORD'], host="localhost", dbname="mapd")
	
	while True:
		# get a csv file to load
		global idx_csvfile
		lck_csvfile.acquire()
		fi = idx_csvfile
		idx_csvfile += 1
		lck_csvfile.release()
		
		if fi >= len(csv_files): break;
		
		print "writer[%d]: loading %s" % (ith, csv_files[fi])
		cur = con.cursor()
		cur.execute("COPY trip1 FROM '%s' WITH (header='true');" % csv_files[fi])
		cur.close()

	con.close()

def reader(ith):
	con = connect(user=os.environ['MAPD_USERNAME'], password=os.environ['MAPD_PASSWORD'], host="localhost", dbname="mapd")
	
	while not end_of_writes:
		time.sleep(random.random());
		cur = con.cursor()
		cur.execute("select count(*) from (select max(trip_time_in_secs) from trip1 where trip_time_in_secs > 10 group by trip_time_in_secs);")
		print "reader[%d]: result = " % ith, list(cur)
		cur.close()
		
	con.close()

# drop table trip1 if exists
con = connect(user=os.environ['MAPD_USERNAME'], password=os.environ['MAPD_PASSWORD'], host="localhost", dbname="mapd")
cur = con.cursor()

try:
	print "drop table trip1 if exists..."
	cur.execute("drop table trip1;")
except pymapd.exceptions.Error as e:
	#print e
	pass

print "create table trip1..."
cur.execute(
'''CREATE TABLE trip1 (
    medallion               TEXT ENCODING DICT,
    hack_license            TEXT ENCODING DICT,
    vendor_id               TEXT ENCODING DICT,
    rate_code_id            SMALLINT,
    store_and_fwd_flag      TEXT ENCODING DICT,
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    passenger_count         SMALLINT,
    trip_time_in_secs       INTEGER,
    trip_distance           DECIMAL(14,2),
    pickup_longitude        DECIMAL(14,2),
    pickup_latitude         DECIMAL(14,2),
    dropoff_longitude       DECIMAL(14,2),
    dropoff_latitude        DECIMAL(14,2)
    ) WITH (FRAGMENT_SIZE=75000000);
'''
)
cur.close()
con.close()

# one thread for each csv to simulate concurrent writes and reads
nth = len(csv_files)
wthreads = []
rthreads = []

# fork some reader threads to test concurrency
print "launch %d reader threads..." % NREADER
for i in range(0, NREADER):
	t = threading.Thread(target=reader, args=[i])
	rthreads.append(t)
	t.start()

# fork nth writer threads
print "launch %d writer threads..." % NWRITER
for i in range(0, NWRITER):
	t = threading.Thread(target=writer, args=[i])
	wthreads.append(t)
	t.start()

# wait for writers to finish
for t in wthreads: t.join()

end_of_writes = True

# wait for readers to finish
for t in rthreads: t.join()


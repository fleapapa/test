#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, getopt
import csv
import threading
import time
import random
import os
import pymapd
from pymapd import connect

# change these per your environment
# could make csv file path and table name also configurable but not a priority for now:) 
CSVPATH = '/mnt/trip/csvfiles/trip*.csv'# NY taxcab trip CSV file paths
re_create_table = False                 # -c to remove and create table before begin test
nwriter = 0                             # -w number of writer (LOAD   query) threads
nreader = 1                             # -r number of reader (SELECT query) threads
ninterf = 0                             # -i number of seconds to interleave forking of threads 

def usage():
	print sys.argv[0], ' cmd args error :)'
	print "usage: %s [-h][-c][-w nw][-r nr][-i ns]" % os.path.basename(sys.argv[0])
	print "\t -h          show this help"
	print "\t -c          delete and re-create table"
	print "\t -r nr       fork nr readers"
	print "\t -w nw       fork nw writers"
	print "\t -i ns       interleave forking threads with ns seconds"
	sys.exit(-1)
	
# parse args
try:
	opts, args = getopt.getopt(sys.argv[1:], "w:r:i:ch")
except getopt.GetoptError:
	usage()

for opt, arg in opts:
	if opt == '-c':
		re_create_table = True
	elif opt == '-h':
		usage()
	elif opt == '-r':
		nreader = int(arg)
	elif opt == '-w':
		nwriter = int(arg)
	elif opt == '-i':
		ninterf = int(arg)

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
		cur.execute("COPY trips FROM '%s' WITH (header='true');" % csv_files[fi])
		cur.close()
	con.close()

def reader(ith):
	con = connect(user=os.environ['MAPD_USERNAME'], password=os.environ['MAPD_PASSWORD'], host="localhost", dbname="mapd")
	while not end_of_writes:
		time.sleep(random.random());
		cur = con.cursor()
		try:
			cur.execute("select count(*) from (select max(trip_time_in_secs) from trips where trip_time_in_secs > 10 group by trip_time_in_secs);")
			print "reader[%d]: result = " % ith, list(cur)
		except pymapd.exceptions.Error as e:
			print "reader[%d]: error = " % ith, e		
		cur.close()
	con.close()

if re_create_table:
	# drop table trips if exists
	gcon = connect(user=os.environ['MAPD_USERNAME'], password=os.environ['MAPD_PASSWORD'], host="localhost", dbname="mapd")
	gcur = gcon.cursor()
	
	try:
		print "drop table trips if exists..."
		gcur.execute("drop table trips;")
	except pymapd.exceptions.Error as e:
		#print e
		pass
	
	print "create table trips..."
	gcur.execute(
	'''CREATE TABLE trips (
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
	gcur.close()
	gcon.close()

# one thread for each csv to simulate concurrent writes and reads
nth = len(csv_files)
wthreads = []
rthreads = []

# fork nth writer threads
print "launch %d writer threads..." % nwriter
for i in range(0, nwriter):
	t = threading.Thread(target=writer, args=[i])
	wthreads.append(t)
	t.start()
	print "\twriter %d forked" % i
	time.sleep(ninterf)

# fork some reader threads to test concurrency
print "launch %d reader threads..." % nreader
for i in range(0, nreader):
	t = threading.Thread(target=reader, args=[i])
	rthreads.append(t)
	t.start()
	print "\treader %d forked" % i
	time.sleep(ninterf)

# wait for writers to finish
for t in wthreads: t.join()

# when nwriter==0, if we set end_of_writes to True immediately, no reader will
# has any chance to send SELECT query to mapd, so sleep a while for SELECT to be sent.
# TODO: for now keep readers on reading
if True:
	time.sleep(2)
	end_of_writes = True

# wait for readers to finish
for t in rthreads: t.join()


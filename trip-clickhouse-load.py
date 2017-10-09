#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, getopt
import csv
import threading
import time
import random
import os
import datetime
import ciso8601
import glob
from dateutil import parser
from pyclickhouse import Connection

# change these per your environment
# could make csv file path and table name also configurable but not a priority for now:) 
CSVPATH = '/mnt/trip/csvfiles/*.csv'    # NY taxcab trip CSV file paths
csv_files = glob.glob(CSVPATH)
re_create_table = False                 # -c to remove and create table before begin test
nwriter = len(csv_files)                # -w number of writer (LOAD   query) threads
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

# this is for synchronize writers
lck_csvfile = threading.Lock()
idx_csvfile = 0

def print_used_time(head, tsec):
	tmin = tsec // 60
	tsec = tsec %  60
	thrs = tmin // 60
	tmin = tmin %  60
	print '%s: %s' % (head, '%d:%d:%d' % (thrs, tmin, tsec))

def to_float(str):
	try:
		return float(str)
	except Exception, e:
		return .0

# for benchmark statistics
nreads = nwrites = 0
nread  = nwrite  = 0
lck_read  = threading.Lock()
lck_write = threading.Lock()

random.seed()

def writer(ith):
	con = Connection('localhost', 8123)
	twrites = 0
	while True:
		# get a csv file to load
		global idx_csvfile
		lck_csvfile.acquire()
		fi = idx_csvfile
		idx_csvfile += 1
		lck_csvfile.release()
		
		if fi >= len(csv_files): break;
		csv_file = csv_files[fi]
		print "writer[%d]: loading %s" % (ith, csv_file)

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
								# !! this field acts as 'version' of ReplacingMergedTable, so it must 
								# !! be unique across different runs; otherwise will be merged to one
								# !!! this is very slow
								#	'dropoff_datetime':    parser.parse(row[6]) + datetime.timedelta(random.randint(0, 1000)),
								# !!! this is 'x60' faster
								'dropoff_datetime':    ciso8601.parse_datetime(row[6]) + datetime.timedelta(random.randint(0, 1000)),
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
				if nrec % 4096 == 0:
#					print '%s = %d' % (csv_file, nrec)
					twrite = time.time()
					try:
						cur.bulkinsert('trips', values)
					except Exception, e:
						print 'bulkinsert(wts): %s' % str(e)
					values = []

					# avg insert rate
					global nwrites, nwrite
					lck_write.acquire()
					nwrite += 1
					twrites += time.time() - twrite
					# show nwrite rate (per 1024 * 4096 rows)
					if nwrite >= 1024: 
						print 'writer[%d]: avg %f bulkinserts/s' % (ith, nwrite / twrites)
						nwrites += nwrite
						nwrite = 0
						twrites = 0
					lck_write.release()
	
	# flush
	cur.bulkinsert('trips', values)
	con.close()
	print '%s = %d' % (csv_file, nrec)

def reader(ith):
	con = Connection('localhost', 8123)
	treads = 0
	while not end_of_writes:
		# different readers aggregate different columns to saturrate cpu ram
		max_columns = ['pickup_datetime', 'dropoff_datetime', 'trip_time_in_secs', 'trip_distance', 
					   'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',
					   'medallion', 'hack_license', 'vendor_id', 'rate_code_id', 'store_and_fwd_flag', 'passenger_count',
					   ]
		grp_columns = ['medallion', 'hack_license']
		# time.sleep(random.random());
		tread = time.time()
		cur = con.cursor()
		try:
			max_column = max_columns[ith % len(max_columns)]
			grp_column = grp_columns[ith % len(grp_columns)]
			cur.select("select count(*) from (select max(%s) from trips group by %s)" % (max_column, grp_column))
			print "reader[%d]: result(%s, %s) = " % (ith, max_column, grp_column), cur.fetchall()
		except Exception, e:
			print 'reader[%d]: select(): %s' % (ith, str(e))
			
		# aggregate time of each select
		global nreads, nread
		lck_read.acquire()
		nread += 1
		treads += time.time() - tread
		# show read rate (per 1k times)
		if nread > 100: 
			print 'reader[%d]: avg %f selects/s' % (ith, nread / treads)
			nreads += nread
			nread = 0
			treads = 0
		lck_read.release()

	con.close()

if re_create_table:
	# create trips table
	con = Connection('localhost', 8123)
	cur = con.cursor()
	
	print "drop table trips if exists..."
	cur.ddl("drop table if exists trips")
	
	print "create table trips..."
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
	""")	# use 'dropoff_datetime' as version to avoid being replaced

# benchmark total time
tstart = time.time()

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
#time.sleep(2)
#end_of_writes = True

# wait for readers to finish
for t in rthreads: t.join()

tused = time.time() - tstart
print_used_time('used time', tused)
print_used_time('avg read', tused / nreads)
print_used_time('avg wrtie', tused / nwrites)


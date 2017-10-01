#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, getopt
import csv
import threading
import time
import random
import os
import pymapd
import datetime
import ciso8601
import rwlock
from dateutil import parser
from pymapd import connect

# change these per your environment
# could make csv file path and table name also configurable but not a priority for now:) 
CSVPATH = '/mnt/trip/csvfiles/*.csv'    # NY taxcab trip CSV file paths
re_create_table = False                 # -c to remove and create table before begin test
nwriter = 0                             # -w number of writer (LOAD   query) threads
nreader = 1                             # -r number of reader (SELECT query) threads
ninterf = 0                             # -i number of seconds to interleave forking of threads
interleave_reads_and_writes = False		# -I this will run write and read in sequence instead of concurrently

def usage():
	print sys.argv[0], ' cmd args error :)'
	print "usage: %s [-h][-c][-w nw][-r nr][-i ns]" % os.path.basename(sys.argv[0])
	print "\t -h          show this help"
	print "\t -c          delete and re-create table"
	print "\t -r nr       fork nr readers"
	print "\t -w nw       fork nw writers"
	print "\t -i ns       interleave forking threads with ns seconds"
	print "\t -I          interleave readers and writers for checking a racing condition"
	sys.exit(-1)
	
# parse args
try:
	opts, args = getopt.getopt(sys.argv[1:], "w:r:i:chI")
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
	elif opt == '-h':
		interleave_reads_and_writes = True

# this signals end of data loading
end_of_writes = False

# get the list of NY taxicab csv files
import glob
csv_files = glob.glob(CSVPATH)
print csv_files

# this is for synchronize writers
lck_csvfile = threading.Lock()
idx_csvfile = 0

# this lock is for synchronize readers and writers
# this is for debugging a racing condition
lck_reads_and_writes = rwlock.RWLock()

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
nread = 0
tread = 0
nwrite = 0
twrite = 0
lck_read  = threading.Lock()
lck_write = threading.Lock()

random.seed()

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
		csv_file = csv_files[fi]
		print "writer[%d]: loading %s" % (ith, csv_file)

		tstart = time.time()
		
		cur = con.cursor()
		nrec = 0
		values = []
		with open(csv_file, 'rb') as f:
			reader = csv.reader(f, delimiter=',', quotechar='"')
			for row in reader:
				# skip header line
				if nrec > 0:
					try:
						values.append([
								row[0],					# medallion 
								row[1],					# hack_license
								row[2],					# vendor_id
								row[3],					# rate_code_id
								row[4],					# store_and_fwd_flag
								row[5],					# pickup_datetime
								# !! this field acts as 'version' of ReplacingMergedTable, so it must 
								# !! be unique across different runs; otherwise will be merged to one
								# !!! this is very slow
								#	'dropoff_datetime':    parser.parse(row[6]) + datetime.timedelta(random.randint(0, 1000)),
								# !!! this is 'x60' faster
								ciso8601.parse_datetime(row[6]) + datetime.timedelta(random.randint(0, 1000)),
								int(row[7]),			# passenger_count
								int(row[8]),			# trip_time_in_secs
								to_float(row[9]),		# trip_distance
								to_float(row[10]),		# pickup_longitude
								to_float(row[11]),		# pickup_latitude
								to_float(row[12]),		# dropoff_longitude
								to_float(row[13]),		# dropoff_latitude
								])
					except Exception, e:
						print 'values.append(%s:%d): %s, file %s, row \'%s\'' % (csv_file, nrec, str(e), csv_file, row)

				nrec += 1
				if nrec % 4096 == 0:
					# if mutual exclusive with readers
					if interleave_reads_and_writes:
						lck_reads_and_writes.acquire_write()

					print '%s = %d' % (csv_file, nrec)
					try:
						con.load_table('trips', values)
					except Exception, e:
						print 'load_table(trips): %s' % str(e)
					values = []
					
					# if mutual exclusive with readers
					if interleave_reads_and_writes:
						lck_reads_and_writes.relase()


		# flush
		print '%s = %d' % (csv_file, nrec)
		con.load_table('trips', values)

		# avg time of each file
		global nwrite, twrite
		lck_write.acquire()
		nwrite += 1
		twrite += time.time() - tstart
		lck_write.release()

	con.close()

def reader(ith):
	# different readers aggregate different columns to saturrate cpu ram
	columns = ['pickup_datetime', 'dropoff_datetime', 'trip_time_in_secs', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude']

	con = connect(user=os.environ['MAPD_USERNAME'], password=os.environ['MAPD_PASSWORD'], host="localhost", dbname="mapd")
	while not end_of_writes:
		time.sleep(random.random());
					
		# if mutual exclusive with readers
		if interleave_reads_and_writes:
			lck_reads_and_writes.acquire_read()

		cur = con.cursor()
		try:
			column = columns[ith % len(columns)]
			cur.execute("select count(*) from (select max(%s) from trips group by %s);" % (column, column))
			print "reader[%d]: result = " % ith, list(cur)
		except pymapd.exceptions.Error as e:
			print "reader[%d]: error = " % ith, e		
		cur.close()
					
		# if mutual exclusive with readers
		if interleave_reads_and_writes:
			lck_reads_and_writes.release()
			
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
time.sleep(2)

# TODO: stop readers after writers
# end_of_writes = True

# wait for readers to finish
for t in rthreads: t.join()

tend = time.time()
print_used_time('total time', tstart - tend)
print_used_time('avg read', tread / nread)
print_used_time('avg wrtie', twrite / nwrite)

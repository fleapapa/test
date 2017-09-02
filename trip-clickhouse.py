#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import threading
from pyclickhouse import Connection
import time

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
                                '_cx':  time.strftime("%Y/%m/%d"),
                                '_c0':  row[0],
                                '_c1':  row[1],
                                '_c2':  row[2],
                                '_c3':  row[3],
                                '_c4':  row[4],
                                '_c5':  row[5],
                                '_c6':  row[6],
                                '_c7':  int(row[7]),
                                '_c8':  int(row[8]),
                                '_c9':  float(row[9]),
                                '_c10':  float(row[10]),
                                '_c11':  float(row[11]),
                                '_c12':  float(row[12]),
                                '_c13':  float(row[13]),
                                })
                        except Exception, e:
                            print 'values.append(%s:%d): %s' % (csv_file, nrec, str(e))
                            
                    nrec += 1
                    if nrec % 1024 == 0:
                        print '%s = %d' % (csv_file, nrec)

                        try:
                            cur.bulkinsert('trip', values)
                        except Exception, e:
                            print 'bulkinsert(wts): %s' % str(e)

                        values = []

        print '%s = %d' % (csv_file, nrec)
        cur.bulkinsert('trip', values)
        cur.close()
        con.close()
        
# create trip table
con = Connection('localhost', 8123)
cur = con.cursor()

cur.ddl("""
    drop table if exists trip
""")

cur.ddl("""
    create table trip (
        _cx    Date,
        _c0    String,
        _c1    String,
        _c2    String,
        _c3    String,
        _c4    String,
        _c5    DateTime,
        _c6    DateTime,
        _c7    Int8,
        _c8    UInt16,
        _c9    Float32,
        _c10   Float32,
        _c11   Float32,
        _c12   Float32,
        _c13   Float32
)
engine=MergeTree(_cx, (_c5, _c6, _c10, _c11), 8192)
""")

# get the list of csv files
import glob
csv_files = glob.glob('/data/tmp/parquet/trip_data/*.csv')
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

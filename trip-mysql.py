#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import threading
import MySQLdb as mdb

def worker(csv_file):
        con = mdb.connect('localhost', 'test', 'test123', 'test');
        cur = con.cursor(mdb.cursors.DictCursor)

        nrec = 0
        with open(csv_file, 'rb') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            for row in reader:
                try:
                    # skip header line
                    if nrec > 0:
                        cur.execute("""INSERT INTO trip 
                                     VALUES(%s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s)
                                    """, 
                                row)
                    nrec += 1
                    if nrec % 1024 == 0:
                        con.commit()

                except mdb.Error, e: 
                    print >> sys.stderr, "mysql error %d: %s" % (e.args[0], e.args[1])

        print '%s = %d' % (csv_file, nrec)
        con.commit()
        cur.close()
        con.close()
        

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
    t = threading.Thread(target=worker, args=[csv_file])
    threads.append(t)
    t.start()

for t in threads:
    t.join()

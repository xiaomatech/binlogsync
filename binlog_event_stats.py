#!/usr/bin/env python
# -*- coding: utf-8 -*-

# by Emanuel Calvo "3manuek"
#
# Dump all replication events from a remote mysql server
# and generates statistics per given interval.
#
# Useful for a logical stream metrics for debugging application
# writes causing unnecesary writes or affecting replication.
#
# Currently only compatible with binlog_format = ROW
# For statement, instead iterate over rows, we should do:
#
#    for binlogevent in stream:
#        if isinstance(binlogevent, QueryEvent):
#            print binlogevent.query
#

import re, sys, signal  #pprint
from itertools import groupby
from operator import itemgetter
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,
                                          UpdateRowsEvent,
                                          WriteRowsEvent, )

from pymysqlreplication.event import (
    QueryEvent,
    RotateEvent,
    BeginLoadQueryEvent,
    ExecuteLoadQueryEvent
    #, FormatDescriptionEvent,
    #XidEvent, GtidEvent, StopEvent,
    #,NotImplementedEvent
)

from datetime import datetime
import time
from pudb import set_trace

#from collections import defaultDict()

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 22695,
    "user": "msandbox",
    "passwd": "msandbox"
}

# CAUTION: The larger the interval, the more memory needed
# for stats generator. Test and put a limit.
# Possible incremental stats would be more efficient, or
# using a database in memory manageable datastore.

OPTIONS = {
    "interval": 5,  # in seconds
    "serverid": 3,
    "log_file": "mysql-bin.000002",
    "log_pos": 48989184
    #"pattern": "http?:\/\/(.*)\s?" # data pattern to search
}


def signal_handler(signal, frame):
    #stream.close()
    sys.exit(0)


def printStats(opsGeneralCollector, patternGeneralCollector, binLogEventSizes,
               binLogReadBytes, queryStats, rotateStats, loadQueryStats):
    sorted_input = sorted(opsGeneralCollector, key=itemgetter(0, 1))
    for event, table in groupby(sorted_input, lambda x: x[0]):
        print "Event: %s " % (event)
        for table_ in table:
            table = table_[1]
            print "       %s : %s " % (table,
                                       opsGeneralCollector[event, table])

    sorted_input = sorted(patternGeneralCollector, key=itemgetter(1))

    for event, groupEvent in groupby(sorted_input, lambda x: x[0]):
        print "Event: %s " % (event)
        for table, groupTables in groupby(groupEvent, lambda x: x[1]):
            for record in groupTables:
                print "      %s pattern: %s: %s" % (
                    table, record[2], patternGeneralCollector[record])

    print "Total Ops: %s , W/D/I collected: %s, Pattern matches: %s " % (
        totalOps, len(opsGeneralCollector), len(patternGeneralCollector))
    print "Sum binlogevent size %s, read bytes: %s" % (binLogEventSizes,
                                                       binLogReadBytes)
    print "Queries: %s  Rotate: %s (Size: %s) Load Queries: %s" % (
        queryStats['count'], rotateStats['count'], rotateStats['size'],
        loadQueryStats['count'])


"""
t={}
t['s','t','f'] = 1
for a,b in groupby(t, lambda x: x[0]):
        for c,d in groupby(b, lambda x: x[1]):
            for e in d:
                print a,c,t[e],e[1]
"""


def main():
    signal.signal(signal.SIGINT, signal_handler)
    global patternGeneralCollector
    global opsGeneralCollector
    global totalOps  # Reset on each interval
    global countOps
    global countPatterns
    global patternC
    global binLogEventSizes
    global binLogReadBytes

    patternGeneralCollector = {}
    opsGeneralCollector = {}
    queryStats = {}
    rotateStats = {}
    loadQueryStats = {}
    totalOps = 0  # Reset on each interval
    countOps = 0
    countPatterns = 0
    prevTimeFlag = 0
    binLogEventSizes = 0
    binLogReadBytes = 0
    queryStats['count'] = 0
    rotateStats['count'] = 0
    rotateStats['size'] = 0
    loadQueryStats['count'] = 0
    firstRun = 1

    # Great resource http://blog.mattheworiordan.com/post/13174566389/url-regular-expression-for-links-with-or-without
    patternC = re.compile(r'[A-Za-z]{3,9}:\/\/(?:[A-Za-z0-9\.\-]+)')

    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    # If event is bigger, sets it to now() and prints stats.
    timeCheck = time.time()

    # Only events help us to keep the stream shorter as we can.
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=OPTIONS["serverid"],
                                log_file=OPTIONS["log_file"],
                                log_pos=OPTIONS["log_pos"],
                                resume_stream= True, # If no log_pos, set to False (from the beginning)
                                #auto_position=1,
                                blocking=True,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent,QueryEvent, RotateEvent,BeginLoadQueryEvent, ExecuteLoadQueryEvent])

    for binlogevent in stream:
        patternCollector = None
        occurrence = None

        if firstRun == 1:
            print "Binlog: %s Position: %s First Event TS: %s " % (
                OPTIONS["log_file"], OPTIONS["log_pos"],
                datetime.fromtimestamp(timeCheck))
            firstRun = 0

        if isinstance(binlogevent, QueryEvent):
            queryStats['count'] += 1
            continue
        elif isinstance(binlogevent, RotateEvent):
            rotateStats['count'] += 1
            rotateStats['size'] += binlogevent.event_size
            continue
        elif isinstance(binlogevent, ExecuteLoadQueryEvent) or isinstance(
                binlogevent, BeginLoadQueryEvent):
            loadQueryStats['count'] += 1
            continue

        for row in binlogevent.rows:
            if isinstance(binlogevent, DeleteRowsEvent):
                vals = row["values"]
                eventType = "delete"
            elif isinstance(binlogevent, UpdateRowsEvent):
                vals = row["after_values"]
                eventType = "update"
            elif isinstance(binlogevent, WriteRowsEvent):
                vals = row["values"]
                eventType = "insert"

            prefix = "%s.%s" % (binlogevent.schema, binlogevent.table)
            if (eventType, prefix) in opsGeneralCollector:
                opsGeneralCollector[eventType, prefix] += 1
            else:
                opsGeneralCollector[eventType, prefix] = 1

            occurrence = re.search(patternC, str(vals))
            if occurrence:
                occurrence = re.sub('[A-Za-z]{3,9}:\/\/', '',
                                    occurrence.group())
                if (eventType, prefix, occurrence
                    ) in patternGeneralCollector.keys():
                    patternGeneralCollector[eventType, prefix, occurrence] += 1
                else:
                    patternGeneralCollector[eventType, prefix, occurrence] = 1

            totalOps += 1

        binLogEventSizes += binlogevent.event_size
        binLogReadBytes += binlogevent.packet.read_bytes

        # If interval has been committed, print stats and reset everything
        if (timeCheck + OPTIONS["interval"]) < time.time():
            #or txTimeCheck + OPTIONS["interval"] > time.time():
            printStats(opsGeneralCollector, \
                       patternGeneralCollector, \
                       binLogEventSizes, \
                       binLogReadBytes, \
                       queryStats,\
                       rotateStats,\
                       loadQueryStats)
            # Reset everything to release memory
            totalOps = 0
            patternGeneralCollector = {}
            opsGeneralCollector = {}
            queryStats['count'] = 0
            rotateStats['count'] = 0
            rotateStats['size'] = 0
            loadQueryStats['count'] = 0
            timeCheck = time.time()
            prevTimeFlag = 0
            binLogEventSizes = 0
            binLogReadBytes = 0
            firstRun = 0

    stream.close()


if __name__ == "__main__":
    main()

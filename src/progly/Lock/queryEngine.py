#!/usr/bin/python
import os
import subprocess
import Queue
import struct
import time
import random
import sys

from collections import namedtuple


# HELP: PATH expected is ../../../build/

#Define isolatin Levels

#0 - Read Uncommitted
#1 - Read Committed
#2 - Repeatable read
#3 - Serializable

canProcess = Queue.Queue()
waitQueue = Queue.Queue()
MyStruct = namedtuple("MyStruct", "IOType table isolation tid timestamp")
freeTime = 0.0
processedTid = ()
PATH = sys.argv[1]
def isTableBusy(table_name, table_group ):
    #os.system("bin/run-query --num-objs 2 --pool tpchdata --table-name lineitem --lock-op --get-lock-obj abcd.lock")
    #table_name="lineitem"
    #table_group="abcd.lock"
    cmd = "bin/run-query --num-objs 1 --pool tpchdata --table-name " + table_name +" --lock-op --get-lock-obj " + table_group

    output = subprocess.check_output(cmd, shell=True)
    #print output
    print output
    # parse output to get value of table busy
    for row in  output.split('\n'):
        if ":" in row:
            key, value  = row.split(':')
    
    return value

def acquireLock(table_name, table_group ):
    #os.system("bin/run-query --num-objs 2 --pool tpchdata --table-name lineitem --lock-op --get-lock-obj abcd.lock")
    #table_name="lineitem"
    #table_group="abcd.lock"
    cmd = PATH + "bin/run-query --num-objs 1 --pool tpchdata --table-name " + table_name +" --lock-op --acquire-lock-obj " + table_group
    output = subprocess.check_output(cmd, shell=True)
    #print output
    # parse output to get value of table busy

    # Acquired the lock
    if "print_data" in output:
        return 1

    for row in  output.split('\n'):
        if ":" in row:
            key, value  = row.split(':')
    
    return value

def freeLock(table_name, table_group ):
    cmd = PATH + "bin/run-query --num-objs 1 --pool tpchdata --table-name " + table_name +" --lock-op --free-lock-obj " + table_group
    output = subprocess.check_output(cmd, shell=True)



def queueOps():

    q= Queue.Queue()

    for i in range(5):
        q.put(i)

    while not q.empty():
        print q.get()

def processRequests(requests):

    table_group='public.lock'
    for r in requests:

        if r.isolation == 0 or r.isolation == 1:
            canProcess.put(r)
            continue
        ret=acquireLock( r.table, table_group )
        
        # Managed to acquire the lock
        if int(ret) is 1:
            canProcess.put(r)
        else:
            waitQueue.put(r)

def processWaitQueue():

    table_group='public.lock'

    cnt = waitQueue.qsize()
    while cnt!=0:
        req=waitQueue.get()

        ret = acquireLock( req.table, table_group)
        print req.table
        if int(ret) is 1:
            canProcess.put(req)
        else:
            waitQueue.put(req)
        cnt-=1

def execRequest(m):

    oidPrefix="public"

    cmd = PATH + "bin/run-query --num-objs 1 --start-obj 0 --pool tpchdata --table-name " + m.table + " --oid-prefix " + oidPrefix + " --select " + "linenumber,geq,6;"
    output = subprocess.check_output(cmd, shell=True)
    print "Tid of Request processed: ", m.tid
    print output


def generateRequests():

    m = MyStruct(1, 'S', 2, 1 , 12)

    n = MyStruct(1, 'S', 3, 3, 24)

    o = MyStruct(1, 'T', 0, 2, 36)

    p = MyStruct(1, 'S', 3, 4, 48)
    requests=[]

    requests.append(m)
    requests.append(n)
    requests.append(o)
    requests.append(p)


    print 'Tid:', m.tid, 'Table-name:', m.table
    print '\n'
    print 'Tid:', n.tid, 'Table-name:', n.table
    print '\n'
    print 'Tid:', o.tid, 'Table-name:', o.table
    print '\n'
    print 'Tid:', p.tid, 'Table-name:', p.table
    print '\n'
    return requests
# table_group
def checkTimer(freeTimes):

    for k,v in freeTimes.items():
        if time.time() > v:
            freeLock(k, "public.lock")



def main():
    #table_busy = isTableBusy("lineitem", "abcd.lock")


    print 'List of requests to be processed:\n'

    requests = generateRequests()


    processRequests(requests)

    freeTimes=dict()

    while canProcess.empty() is not True or waitQueue.empty() is not True:

        print 'List of requests in canProcess Queue:',list(canProcess.queue)
        print 'List of requests in waiting Queue',list(waitQueue.queue)

        if canProcess.empty() is not True:
            request=canProcess.get()
            execRequest(request)
            freeTimes[request.table] = time.time() + random.randint(1,5)
        processWaitQueue()
        checkTimer(freeTimes)


if __name__ == "__main__":
    main()

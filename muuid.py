#!/usr/bin/env python
#encoding:utf-8

import os
import uuid
import time
import threading

_LOCK_ = threading.Lock()

_UUID_NODE_ = uuid.getnode()

_PROCESS_ID_ = os.getpid()

_THREADING_ID_ = 0

_CURRENT_TIME_ = int(time.time() * 1000000)

_INDEX_ = 0

_MAX_INDEX_ = 9999999999

# 20 uuid_node 5 process_id 5 threading_id 18 microtime 10 index
_UUID_TPL_ = '%020d%05d%05d%018d%010d'

def getuuid():
    if _LOCK_.acquire():
        global _INDEX_, _CURRENT_TIME_
        _id = ''
        try:
            _id = _UUID_TPL_ % (_UUID_NODE_, _PROCESS_ID_, _THREADING_ID_, _CURRENT_TIME_, _INDEX_)
            _INDEX_ += 1
            if _INDEX_ > _MAX_INDEX_:
                _INDEX_ = 0
                _CURRENT_TIME_ = int(time.time() * 1000000)
        except BaseException, e:
            print e
        finally:
            _LOCK_.release()

            return _id

if __name__ == '__main__':
    import Queue
    queue = Queue.Queue()
    threading_cnt = 10
    threading_uuid_cnt = 1000000
    def test():
        for i in xrange(threading_uuid_cnt):
            #getuuid()
            #uuid.uuid1()
            queue.put(getuuid())
    t = time.time()
    ths = []
    for i in xrange(threading_cnt):
        th = threading.Thread(target=test)
        th.setDaemon(True)
        th.start()
        ths.append(th)
    while 1:
        _hasAlive = False
        for th in ths:
            if th.isAlive():
                _hasAlive = True
                break
        if not _hasAlive:
            break
        time.sleep(2)
    ids = []
    while 1:
        try:
            r = queue.get(block=False)
            ids.append(r)
        except:
            break

    print time.time() - t
    print threading_cnt * threading_uuid_cnt
    print len(ids)
    print len(set(ids))
    print time.time() - t


#!/usr/bin/env python
#encoding: utf-8
import time
import json

import config
from redisclient import RedisClient

class Dispatcher(object):


    def __init__(self, script):
        self._client = RedisClient(host=config.REDIS_HOST, \
                                   port=config.REDIS_PORT
                                   )
        self._script = script
        self._redis_key = '%s:%s' % (config.QUEUE_PREFIX, self._script)

    def run(self):
        prev_time = int(time.time())
        cnt = 0
        while 1:
            _task = self._client.get(self._redis_key)
            if _task is None:
               time.sleep(0.05)
               continue
            ctime = time.time()
            if int(ctime) != prev_time:
               prev_time = int(ctime)
               print cnt
               cnt = 0
            else:
               cnt += 1
            continue
            script = _task.get('script')
            kwargs =  _task.get('kwargs')
            print '%s:%s:%s' % (time.time() - float(kwargs.get('time')), script, kwargs)



if __name__ == '__main__':
    Dispatcher().run()

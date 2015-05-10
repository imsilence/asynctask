#!/usr/bin/env python
#encoding: utf-8

import logging
import traceback

import uuid

import config
from redisclient import RedisQueueClient

logger = logging.getLogger('root.publisher')

class Publisher(object):
    

    def __init__(self):
        self._client = RedisQueueClient(host=config.REDIS_HOST, \
                                   port=config.REDIS_PORT
                                   )

    def publish(self, script, script_args={}, task_args={}):
        key = '%s:in' % (config.QUEUE_PREFIX)
        msg = {'id': str(uuid.uuid1()), 'script': script, 'script_args': script_args, 'task_args': task_args}
        logger.debug('put %s to key %s', msg, key)
        self._client.put(msg, key)


if __name__ == '__main__':
    p = Publisher()
    import random
    import time
    prev_time = int(time.time())
    cnt = 0
    while 1:
        script = chr(65)
        args = {'time': time.time()}
        p.publish(script, script_args=args)
        ctime = int(time.time())        
        if prev_time == ctime:
            cnt += 1
        else:
            print '%s:%s' % (ctime, cnt)
            cnt = 0
            prev_time = ctime 
        #time.sleep(0.5)

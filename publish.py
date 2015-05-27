#!/usr/bin/env python
#encoding: utf-8

import threading
import logging
import traceback
import time

import muuid

import config
from redisclient import RedisQueueClient, RedisMapClient
from models import Task, TaskMapper

logger = logging.getLogger('root.publisher')

class ResultMonitor(threading.Thread):

    def __init__(self, *args, **kwargs):
        super(ResultMonitor, self).__init__(*args, **kwargs)
        self.setDaemon(True)
        self._tasks = {}
        self._result_client = RedisMapClient(host=config.REDIS_HOST, \
                                             port=config.REDIS_PORT
                                             )

        self._redis_key_out = '%s:%%s:out' % (config.QUEUE_PREFIX,)

    
    def add_task(self, task):
        if task.is_ignore_result():
            return
        self._tasks[task.get_id()] = task

    def run(self):
        while 1:
            _return_ids = []
            for _id, _task in self._tasks.items():
                _key = self._redis_key_out % _task.get_script()
                _result = self._result_client.get(_id, _key)
                if _result is not None:
                    _return_ids.append(_id)
                    _task.set_result(Task(**_result).get_result())
 
            for _id in _return_ids:
                del self._tasks[_id]
            time.sleep(1)
    

class Publisher(object):
    

    def __init__(self):
        self._client = RedisQueueClient(host=config.REDIS_HOST, \
                                   port=config.REDIS_PORT
                                   )
        self._result_monitor = ResultMonitor()
	self._result_monitor.start()

    def publish(self, script, script_args={}, ignore_result=True, task_args={}):
        key = '%s:%s:in' % (config.QUEUE_PREFIX, script)
	_task = Task(muuid.getuuid(), script, script_args=script_args, ignore_result=ignore_result, task_args=task_args)
        self._result_monitor.add_task(_task) 
        logger.debug('put %s to key %s', TaskMapper(_task).as_dict(), key)
        self._client.put(TaskMapper(_task).as_dict(), key)
        return _task

if __name__ == '__main__':
    p = Publisher()
    import random
    import time
    prev_time = int(time.time())
    cnt = 0
    _tasks = {} 
    while 1:
        script = chr(65)
        args = {'time': time.time(), 'x' : random.randint(0, 10), 'y': random.randint(0, 10)}
        _task = p.publish(script, script_args=args, ignore_result=False)
        _tasks[_task.get_id()] = _task
        ctime = int(time.time())        
        cnt += 1
        if prev_time != ctime:
            _ok_ids = []
            for _id, _task in _tasks.items():
                if _task.is_ok():
                    _ok_ids.append(_id)
            print '%s:%s:%s' % (ctime, cnt, len(_ok_ids))
            for _id in _ok_ids:
                del _tasks[_id]
            cnt = 0
            prev_time = ctime 

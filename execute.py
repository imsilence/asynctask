#!/usr/bin/env python
#encoding: utf-8

import os
import sys
import time
import json

import config
from redisclient import RedisQueueClient, RedisMapClient

EMPTY_METHOD = lambda *args, **kwargs : None

class Dispatcher(object):
    
    _dispatchers = {}
    
    @staticmethod
    def get(script):
        _dispatcher = Dispatcher._dispatchers.get(script, None)
        if _dispatcher is None:
        
            _dir_task = os.path.normpath(os.path.join(config.BASE_PATH, 'tasks'))
            if _dir_task not in sys.path:
                sys.path.insert(0, _dir_task)
            if os.path.exists(os.path.join(_dir_task, '%s.py' % script)):
                _module = __import__(script) 
                getattr(_module, 'init', EMPTY_METHOD)()
                _dispatcher = getattr(_module, 'dispatch', EMPTY_METHOD) 
                Dispatcher._dispatchers[script] = _dispatcher
        return _dispatcher


class Executor(object):


    def __init__(self, script):
        self._task_client = RedisQueueClient(host=config.REDIS_HOST, \
                                   port=config.REDIS_PORT
                                   )
        self._execing_client = RedisMapClient(host=config.REDIS_HOST,\
                                           port=config.REDIS_PORT
                                           )
        self._result_client = RedisMapClient(host=config.REDIS_HOST, \
                                             port=config.REDIS_PORT
                                             )

        self._script = script
        self._redis_key_in = '%s:%s:in' % (config.QUEUE_PREFIX, self._script)
        self._redis_key_execing = '%s:%s:execing' % (config.QUEUE_PREFIX, self._script) 
        self._redis_key_out = '%s:%s:out' % (config.QUEUE_PREFIX, self._script)

    def run(self):
        prev_time = int(time.time())
        cnt = 0
        _pid = os.getpid()
        while 1:
            _task = self._task_client.get(self._redis_key_in)
            if _task is None:
               time.sleep(0.05)
               continue

            script = _task.get('script')
            script_args =  _task.get('script_args', {})
            task_args = _task.get('task_args', {})

            _dispatcher = Dispatcher.get(script)

            _result = None
            if _dispatcher is None:
                continue
            try:
                self._execing_client.set(_task['id'], _task, self._redis_key_execing)
                _result = _dispatcher(**script_args)
                if _result is None:
                    continue
                _task['result'] = _result
                self._result_client.set(_task['id'], _task, self._redis_key_out)          
            except BaseException, e:
	        print e
                self._task_client.put(_task, self._redis_key_in)
            finally:
                self._execing_client.get(_task['id'], self._redis_key_execing)
                ctime = int(time.time())
                if prev_time == ctime:
                    cnt += 1
		else:
		    print '%s:%s:%s' % (_pid, ctime, cnt)
		    cnt = 0
		    prev_time = ctime

if __name__ == '__main__':
    import getopt 
    script = None
    opts, args = {}, []
    try:
        opts, args = getopt.getopt(sys.argv[1:], 's:')
        opts = dict(opts)
    except Exception, e:
        pass

    script = opts.get('-s', None)
    if script is None:
        def usage():
            print 'Usage:%s -s task' % sys.argv[0]
        usage()
        sys.exit(-1)
    Executor(script).run()

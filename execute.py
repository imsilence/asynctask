#!/usr/bin/env python
#encoding: utf-8

import os
import sys
import time
import json

import config

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


    def __init__(self, script, inqueue, outqueue):
        print 'create executor:%s' % script
        self._script = script
        self._inqueue = inqueue
	self._outqueue = outqueue

    def run(self):
        prev_time = int(time.time())
        cnt = 0
        _pid = os.getpid()
        while 1:
            _task = self._inqueue.get(block=True)
            script = _task.get('script')
            script_args =  _task.get('script_args', {})
            task_args = _task.get('task_args', {})

            _dispatcher = Dispatcher.get(script)

            _result = None
            if _dispatcher is None:
                continue
            try:
                _result = _dispatcher(**script_args)
                _task['result'] = _result
            except BaseException, e:
	        _task['exception'] = e
	        print e
            finally:
		self._outqueue.put(_task)
                ctime = int(time.time())
                if prev_time == ctime:
                    cnt += 1
		else:
		    print '%s:%s:%s' % (_pid, ctime, cnt)
		    cnt = 0
		    prev_time = ctime

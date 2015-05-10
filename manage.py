#!/usr/bin/evn python
#encoding: utf-8

import time
from multiprocessing import Process, Queue
import threading

import config
from redisclient import RedisQueueClient, RedisMapClient
from execute import Executor

class TaskManager(object):


    def __init__(self):
        self._processes = {}
        self._executors = {}
	self._queues = {}

        self._inqueue_key = '%s:in' % (config.QUEUE_PREFIX)
	self._dispatchedmap_key = '%s:dispatched' % (config.QUEUE_PREFIX)
	self._outmap_key = '%s:out' % (config.QUEUE_PREFIX)

        self._inqueue_client = RedisQueueClient(host=config.REDIS_HOST, \
	                                        port=config.REDIS_PORT)
        self._dispatchedmap_client = RedisMapClient(host=config.REDIS_HOST, \
	                                     port=config.REDIS_PORT)
        self._outmap_client = RedisMapClient(host=config.REDIS_HOST, \
	                                     port=config.REDIS_PORT)

    def _create_process(self, work_name, executor=None):
        if executor is None:
	    if self._queues.get(work_name) is None: 
		self._queues[work_name] = {'inqueue' : Queue(), 'outqueue' : Queue()}
            executor = Executor(work_name, self._queues[work_name]['inqueue'],  self._queues[work_name]['outqueue'])

        _process = Process(target=executor.run)
	_process.daemon = True
        _process.start()
	self._processes[_process.pid] = {'name' : work_name, 'handler' : _process}
	print 'create process, %s:%s' % (_process.pid, work_name)
	return _process

    def _dispath(self):
        prev_time = int(time.time())
        cnt = 0

        while 1:
	    _task = self._inqueue_client.get(self._inqueue_key)
	    if _task is None:
	        time.sleep(0.05)
		continue
	    _script = _task.get('script', 'default')
	    _inqueue = self._queues.get(_script, {}).get('inqueue', None)
	    if _inqueue is None:
	        print 'script is not config:%s' % _script
	        continue
	    self._dispatchedmap_client.set(_task['id'], _task, self._dispatchedmap_key)
	    _inqueue.put(_task)

	    ctime = int(time.time())
	    if prev_time == ctime:
	        cnt += 1
	    else:
	        print '%s:%s:%s' % ('dispatch', ctime, cnt)
	        cnt = 0
	        prev_time = ctime


    def _collect(self):
        prev_time = int(time.time())
        cnt = 0

        _task_cnt = len(self._queues)
        while 1:
	    _empty_cnt = 0
	    for _item in self._queues.values():
	        for i in xrange(1000):
		    _outqueue = _item.get('outqueue')
		    if 0 == _outqueue.qsize():
		        _empty_cnt += 1
		        break
	            _result = _outqueue.get(block=True)
	            self._outmap_client.set(_result['id'], _result, self._outmap_key)
	            self._dispatchedmap_client.get(_result['id'], self._dispatchedmap_key)

                    ctime = int(time.time())
                    if prev_time == ctime:
                        cnt += 1
                    else:
                        print '%s:%s:%s' % ('collect', ctime, cnt)
                        cnt = 0
                        prev_time = ctime

            if _task_cnt == _empty_cnt:
	        time.sleep(0.05)

    def _monitor(self):
        return
        while 1:
	    _dead_pids = []
            for _pid, _info in self._processes.items():
	        if _info['handler'].is_alive():
		    continue
		_work_name = _info.get('name')
		print 'process is dead, %s:%s' % (_pid, _work_name)
	        _process = self._create_process(_work_name, _info.get('handler'))
		_dead_pids.append(_pid)
            for _pid in _dead_pids:
	        del self._processes[_pid]
            time.sleep(1)

    def run(self):
        for _work_name, _work_options in config.WORKS.items():
	    _work_num = int(_work_options.get('number', 1))
	    for _idx in xrange(_work_num):
	        _process = self._create_process(_work_name)

	_th_monitor = threading.Thread(target=self._monitor)
	_th_monitor.setDaemon(True)
	_th_monitor.start()
         
	_th_dispatch = threading.Thread(target=self._dispath)
	_th_dispatch.setDaemon(True)
	_th_dispatch.start()

	_th_collect = threading.Thread(target=self._collect)
	_th_collect.setDaemon(True)
	_th_collect.start()


if __name__ == '__main__':
    TaskManager().run()
    while 1:
        time.sleep(1)

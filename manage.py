#!/usr/bin/evn python
#encoding: utf-8

import time
from multiprocessing import Process
import threading

import config
from execute import Executor

class TaskManager(object):


    def __init__(self):
        self._processes = {}
        self._executors = {}

    def _create_process(self, work_name, executor=None):
        _executor = executor or Executor(work_name)
        _process = Process(target=_executor.run)
	_process.daemon = True
        _process.start()
	self._processes[_process.pid] = {'name' : work_name, 'handler' : _process}
	print 'create process, %s:%s' % (_process.pid, work_name)
	return _process

    def _monitor(self):
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

	_th = threading.Thread(target=self._monitor)
	_th.setDaemon(True)
	_th.start()
	    


if __name__ == '__main__':
    TaskManager().run()
    while 1:
        time.sleep(1)

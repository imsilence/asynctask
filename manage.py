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

    def _monitor(self):
        pass

    def run(self):
        for _work_name, _work_options in config.WORKS.items():
	    _work_num = int(_work_options.get('number', 1))
	    for _idx in xrange(_work_num):
	        _process = Process(target=Executor(_work_name).run)
		_process.daemon = True
		_process.start()
		self._processes[_process.pid] = {'name' : _work_name, 'handler' : _process}

	_th = threading.Thread(target=self._monitor)
	_th.setDaemon(True)
	_th.start()
	    


if __name__ == '__main__':
    TaskManager().run()
    while 1:
        time.sleep(1)

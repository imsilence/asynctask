#!/usr/bin/env python
#encoding:utf-8

from bpmappers import Mapper, RawField

class Task(object):
    STATUS_WAITING = 1
    STATUS_RUNNING = 2
    STATUS_OK = 3 

    def __init__(self, uid, script, **kwargs):
        self._uid = uid
        self._script = script
	self._script_args = kwargs.get('script_args', {})
        self._ignore_result = kwargs.get('ignore_result', True)
	self._task_args = kwargs.get('task_args', {})
	self._result = kwargs.get('result', None)
	self._status = kwargs.get('status', Task.STATUS_WAITING)

    def get_id(self):
        return self._uid

    def get_script(self):
        return self._script

    def is_ignore_result(self):
        return self._ignore_result

    def get_script_args(self):
        return self._script_args

    def get_task_args(self):
        return self._task_args

    def get_result(self):
        return self._result

    def set_result(self, result):
        self._result = result
        self.set_status(Task.STATUS_OK)

    def set_status(self, status):
        self._status = status
    
    def is_ok(self):
        return self._status == Task.STATUS_OK

class TaskMapper(Mapper):
    uid = RawField('_uid')
    script = RawField('_script')
    script_args = RawField('_script_args')
    ignore_result = RawField('_ignore_result')
    task_args = RawField('_task_args')
    result = RawField('_result')
    status = RawField('_status')

#!/usr/bin/env python
#encoding: utf-8

import os

BASE_PATH = ''

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

QUEUE_PREFIX = 'uk:silence:asynctask' 

LOADED = False

def _load_base_config():
    global BASE_PATH
    BASE_PATH = os.path.normpath(os.path.dirname(os.path.abspath(__file__)))

def _load_asynctask_config():
    global REDIS_HOST, REDIS_PORT, QUEUE_PREFIX
    from ConfigParser import ConfigParser

    _parser = ConfigParser()
    _parser.read(os.path.join(BASE_PATH, 'asynctask.conf'))

    REDIS_HOST = _parser.get('redis', 'host')
    REDIS_PORT = _parser.getint('redis', 'port')

    QUEUE_PREFIX = _parser.get('queue', 'prefix')


def _load_work_config():
    pass


def _load_config():
    if LOADED:
        return

    global LOADED

    _load_base_config()
    _load_asynctask_config() 
    _load_work_config()

    LOADED = True


_load_config()

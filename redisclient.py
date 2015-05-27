#!/usr/bin/env python
#encoding: utf-8
import json
import time
import logging 
import traceback

import redis

class RedisClientException(BaseException):

    def __init__(self, msg):
        super(RedisClientException, self).__init__()
        self._msg = msg


class RedisClient(object):
    

    def __init__(self, host='localhost', db=0, port=6379):
        self._retries = 3
        self._connect = self._create_connect(host, port, db)

    def _create_connect(self, host, port, db):
        _connect = None
        for i in xrange(self._retries):
            try:
                _connect = redis.StrictRedis(host=host, port=port, db=db, socket_connect_timeout=3, socket_timeout=5)
		_connect.ping()
            except Exception, e:
	        _connect = None
		time.sleep(1)

            if _connect is not None:
                break
        if _connect is None:
            raise RedisClientException('not connect the redis[%s:%s:%s]' % (host, port, db))
        return _connect


class RedisQueueClient(RedisClient):
    
    def put(self, msg, key, pickle=json):
        return self._connect.lpush(key, pickle.dumps(msg))

    def get(self, key, block=False, timeout=0, pickle=json):
        _result = None
        if block:
            _result = self._connect.brpop(key, timeout)
            if _result is not None:
                _result = _result[1]
        else:
            _result = self._connect.rpop(key)
        return _result and pickle.loads(_result) or _result

    def size(self, key):
        return self._connect.llen(key)

class RedisMapClient(RedisClient):

    def set(self, name, msg, key, pickle=json):
        return self._connect.hset(key, name, pickle.dumps(msg))

    def get(self, name, key, pickle=json):
        _result = self._connect.hget(key, name)
        self._connect.hdel(key, name)
        return _result and pickle.loads(_result) or _result
         
    def size(self, key):
        return self._connect.hlen(key)

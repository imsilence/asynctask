#!/usr/bin/env python
#encoding:utf-8
import time as mtime

def dispatch(time):
    ctime = mtime.time()
    return {'ctime': ctime, 'diff': (ctime - time)}

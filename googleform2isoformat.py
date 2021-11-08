#!/usr/bin/env python

from datetime import datetime

def googleform2isoformat(formTime):
    try:
        dt = datetime.strptime(formTime, '%m/%d/%Y %H:%M:%S.%f')
    except ValueError:
        dt = datetime.strptime(formTime, '%m/%d/%Y %H:%M:%S')
    iso = dt.isoformat()
    return iso

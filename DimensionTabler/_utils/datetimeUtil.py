#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from datetime import datetime
import pytz

def getUtcNowSeconds():
    return utcToUnixtime(datetime.utcnow())

def utcToUnixtime(utcDatetime):
    epochStart = pytz.utc.localize(datetime(1970, 1, 1))
    if utcDatetime.tzinfo == None:
        utcDatetime = pytz.utc.localize(utcDatetime)
    timestamp = (utcDatetime - epochStart).total_seconds()
    return timestamp

def unixtimeToUtc(timestampUTC):
    utc = pytz.utc.localize(datetime.utcfromtimestamp(timestampUTC))
    return utc

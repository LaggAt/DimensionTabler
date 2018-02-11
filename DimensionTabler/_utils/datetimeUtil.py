#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from datetime import *

def getUtcNowSeconds():
    return getSecondsFromDateTime(datetime.utcnow())

def getSecondsFromDateTime(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())



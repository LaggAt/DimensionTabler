#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from MyIterBase import *
from DimensionTabler.DimTabConfig import DimTabConfig
from more_itertools import one
from DimensionTabler._utils import datetimeUtil

class Dimensions(MyIterBase):
    def __init__(self, dimensionsConfig, cbDoJumpBack):
        self._dimensions = {}
        super(Dimensions, self).__init__(self._dimensions)
        self._dimensionsConfig = dimensionsConfig
        self._cbDoJumpBack = cbDoJumpBack #returns TimeSec, we need to start before that

    def UpdateDimensions(self, timeSecSnapshot):
        debugDtSnapshot = datetimeUtil.unixtimeToUtc(timeSecSnapshot)
        timeSecJumpback = None  # timeSec we need to jump back to
        # new structure based on current time_sec
        newDimensions = {}
        # past dimension: settings before any other dimension
        pastDim = one([dim for dim in self._dimensionsConfig if dim.IsPast])
        pastDim._timeSec = -timeSecSnapshot # past time sec to min datetime
        newDimensions[0] = pastDim
        # dimension 1 to n
        dimensionsOrdered = sorted(
            [dim for dim in self._dimensionsConfig if not dim.IsPast],
            key=lambda dim: dim.TimeSec)
        for dim in dimensionsOrdered:
            # we want the same ranges within a timebox, so get start of timebox:
            start = self._getDimStartSec(timeSecSnapshot, dim)
            debugDtDimStart = datetimeUtil.unixtimeToUtc(start)
            newDimensions[start] = dim
        # mark newest dimension
        newDimensions[max(newDimensions.keys())]._isNewest = True
        # do we need to jump back?
        if len(self._dimensions):
            oldTimeSecLst = sorted(self._dimensions.keys())
            newTimeSecLst = sorted(newDimensions.keys())
            for old, new in zip(oldTimeSecLst, newTimeSecLst):
                if old != new:
                    timeSecJumpback = old
                    break
        # write altered dimensions
        self._dimensions = newDimensions
        self._theDict = self._dimensions # needed not to break the MyIterBase.
        # dimensions are ready, jump back if necessary
        if timeSecJumpback:
            self._cbDoJumpBack(timeSecJumpback)

    def _getDimStartSec(self, timeSecSnapshot, dim):
        start = timeSecSnapshot
        if dim.TimeSec:
            start = (start // -dim.TimeSec) * -dim.TimeSec
        return start

    @property
    def DebugDimensionStartDeltas(self):
        return " | ".join([d.TimeSecReadable for d in self])

    def GetDimensionAndTimeSecSlotStartAndEndForTimeSec(self, timeSec):
        debugDt = datetimeUtil.unixtimeToUtc(timeSec)
        nextDimensionStart = None
        for key in self.keys():
            debugDimStartDT = datetimeUtil.unixtimeToUtc(key)
            if timeSec < key:
                nextDimensionStart = key
                break
            dim = self[key]
        # if Granularity > 0: get first timeSec
        timeSecStart = timeSec
        if dim.GranularitySec:
            timeSecStart = (timeSecStart // dim.GranularitySec) * dim.GranularitySec
        # end of time slot
        timeSecEnd = timeSecStart + dim.GranularitySec
        # is the time slot overlapping with the next dimension start?
        if not dim.IsNewest and (nextDimensionStart and timeSecEnd > nextDimensionStart):
            timeSecEnd = nextDimensionStart # so use "<"
        # return Tuple with Dimension, time slot start and end seconds.
        debugDtBlockStart = datetimeUtil.unixtimeToUtc(timeSecStart)
        debugDtBlockEnd = datetimeUtil.unixtimeToUtc(timeSecEnd)
        return dim, timeSecStart, timeSecEnd
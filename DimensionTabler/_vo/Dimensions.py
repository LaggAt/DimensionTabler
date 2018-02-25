#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from MyIterBase import MyIterBase
from DimensionTabler.DimTabConfig import DimTabConfig
from more_itertools import one

class Dimensions(MyIterBase):
    def __init__(self, dimensionsConfig, cbDoJumpBack):
        self._dimensions = {}
        super(Dimensions, self).__init__(self._dimensions)
        self._dimensionsConfig = dimensionsConfig
        self._cbDoJumpBack = cbDoJumpBack #returns TimeSec, we need to start before that

    def UpdateDimensions(self, timeSecSnapshot):
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
            newDimensions[start] = dim
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
        self._theDict = self._dimensions # needed to not break the MyIterBase.
        # dimensions are ready, jump back if necessary
        if timeSecJumpback:
            self._cbDoJumpBack(timeSecJumpback)

    def _getDimStartSec(self, timeSecSnapshot, dim):
        start = timeSecSnapshot
        if dim.TimeSec:
            start = (start // -dim.TimeSec) * -dim.TimeSec
        return start

    def GetDimensionAndTimeSecSlotStartAndEndForTimeSec(self, timeSec):
        nextDimensionStart = None
        for key in self.keys():
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
        if nextDimensionStart and timeSecEnd >= nextDimensionStart:
            timeSecEnd = nextDimensionStart # so use "<"
        # return Tuple with Dimension, time slot start and end seconds.
        return dim, timeSecStart, timeSecEnd
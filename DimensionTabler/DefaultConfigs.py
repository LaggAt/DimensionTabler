#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler import DimTabConfig, DimTab
from DimensionTabler.DimTabEvArgs import *
from DimensionTabler._utils.datetimeUtil import *
import decimal

class ConfDefault(DimTabConfig):
    def __init__(self, tableName):
        super(ConfDefault, self).__init__(tableName)
        self._setDefaults()

    def _setDefaults(self):
        self.Dimensions = [
            DimTabConfig.DimensionConfig(" aktuell",         -15*60,       60),
            DimTabConfig.DimensionConfig("last  1h",         -60*60,     5*60),
            DimTabConfig.DimensionConfig("last 12h",      -12*60*60,    15*60),
            DimTabConfig.DimensionConfig("last 24h",      -24*60*60,    30*60),
            DimTabConfig.DimensionConfig("last  7d",    -7*24*60*60,    60*60),
            DimTabConfig.DimensionConfig("last 30d",   -30*24*60*60,  4*60*60),
            DimTabConfig.DimensionConfig("  before",
                                DimTabConfig.DIMENSION_TIMESEC_PAST, 12*60*60)]
        self.FillGapsWithPreviousResult = True
        self.WaitSecondsBeforeCumulating = 15

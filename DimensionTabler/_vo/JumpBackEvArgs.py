#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from EvArgsBase import EvArgsBase

class JumpBackEvArgs(object):
    def __init__(self, sRowStartPoint, jumpBackBeforeSec, wasOnSec):
        super(JumpBackEvArgs, self).__init__()
        self._sRowStartPoint = sRowStartPoint
        self._jumpBackBeforeSec = jumpBackBeforeSec
        self._wasOnSec = wasOnSec

    @property
    def SRowStartPoint(self):
        return self._sRowStartPoint
    @property
    def JumpBackBeforeSec(self):
        return self._jumpBackBeforeSec
    @property
    def WasOnSec(self):
        return self._wasOnSec
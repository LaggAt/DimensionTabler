#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from EvArgsBase import EvArgsBase

class DtDeleteEvArgs(object):
    def __init__(self, block, count, isBlockEmpty):
        super(DtDeleteEvArgs, self).__init__()
        self._block = block
        self._count = count
        self._isBlockEmpty = isBlockEmpty

    @property
    def Block(self):
        return self._block
    @property
    def Count(self):
        return self._count
    @property
    def IsBlockEmpty(self):
        return self._isBlockEmpty
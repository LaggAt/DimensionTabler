#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler._vo.EvArgsBase import EvArgsBase
from DimensionTabler._utils import datetimeUtil

class GetDataEvArgs(EvArgsBase):
    def __init__(self, sqlLst, count):
        super(GetDataEvArgs, self).__init__()
        self._sqlLst = sqlLst
        self._count = count

    @property
    def SqlLst(self):
        return self._sqlLst

    @property
    def Count(self):
        return self._count

class SourceRowEvArgs(EvArgsBase):
    def __init__(self, sRow, dim, timeSecStart, timeSecEnd):
        super(SourceRowEvArgs, self).__init__()
        self._sRow = sRow
        self._dim = dim
        self._timeSecStart = timeSecStart
        self._timeSecEnd = timeSecEnd

    @property
    def SourceRow(self):
        return self._sRow
    @property
    def Dimension(self):
        return self._dim
    @property
    def TimeSecStart(self):
        return self._timeSecStart
    @property
    def TimeSecEnd(self):
        return self._timeSecEnd
    @property
    def UtcDateStart(self):
        return datetimeUtil.unixtimeToUtc(self._timeSecStart)
    @property
    def UtcDateEnd(self):
        return datetimeUtil.unixtimeToUtc(self._timeSecEnd)

class JumpBackEvArgs(EvArgsBase):
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


class DtInsertEvArgs(EvArgsBase):
    def __init__(self, block, id, sql, params, interDeleteCnt, interInsertCnt):
        super(DtInsertEvArgs, self).__init__()
        self._block = block
        self._id = id
        self._sql = sql
        self._params = params
        self._interDeleteCnt = interDeleteCnt
        self._interInsertCnt = interInsertCnt

    @property
    def Block(self):
        return self._block

    @property
    def Id(self):
        return self._id

    @property
    def Sql(self):
        return self._sql

    @property
    def SqlParams(self):
        return self._params

    @property
    def InterDeleteCnt(self):
        return self._interDeleteCnt

    @property
    def InterInsertCnt(self):
        return self._interInsertCnt


class DtUpdateEvArgs(EvArgsBase):
    def __init__(self, block, id, sql, params, interDeleteCnt, interInsertCnt):
        super(DtUpdateEvArgs, self).__init__()
        self._block = block
        self._id = id
        self._sql = sql
        self._params = params
        self._interDeleteCnt = interDeleteCnt
        self._interInsertCnt = interInsertCnt

    @property
    def Block(self):
        return self._block

    @property
    def Id(self):
        return self._id

    @property
    def Sql(self):
        return self._sql

    @property
    def SqlParams(self):
        return self._params

    @property
    def InterDeleteCnt(self):
        return self._interDeleteCnt

    @property
    def InterInsertCnt(self):
        return self._interInsertCnt


class DtDeleteEvArgs(EvArgsBase):
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

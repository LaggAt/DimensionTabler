#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from MyIterBase import *

class GroupedRows(MyDict):
    def __init__(self, worker):
        ### Dict over TimeSec groups
        super(GroupedRows, self).__init__()
        self._worker = worker

    @property
    def Worker(self):
        return self._worker

    def AddOrGetTS(self, dim, timeSecStart, timeSecEnd):
        ts = self.GetTS(timeSecStart)
        if ts is None:
            ts = TimeSecGroup(dim, timeSecStart, timeSecEnd, self)
            self[timeSecStart] = ts
        return ts

    def GetTS(self, timeSecStart):
        if timeSecStart in self.keys():
            return self[timeSecStart]
        return None

    def GetTSBefore(self, timeSec):
        return self.GetPrev(timeSec)

    def GetTSAfter(self, timeSec):
        return self.GetNext(timeSec)

    # GroupedRows: TimeSecGroup dirty blocks
    def GetDirtyBlocks(self, clearDirty = False):
        for tsGroup in self:
            if tsGroup.Dirty:
                # clear dirty first, so yielded blocks may set this again
                if clearDirty:
                    tsGroup._dirty = False
                for g in tsGroup.GetDirtyBlocks(clearDirty = clearDirty):
                    yield g

    def RemoveOldBlocks(self):
        keepBeforeDirty = 3
        # find last non-dirty
        tsStack = []
        for tsObj in self:
            if tsObj.Dirty:
                break
            tsStack.append(tsObj.TimeSecStart)
        if len(tsStack):
            # remove some elements if we still need parents
            tsStack = sorted([ts for ts in tsStack if ts <= tsObj.MinParentTsWithFillGaps])
            # remove all but last keepBeforeDirty items
            removeKeys = tsStack[:-keepBeforeDirty]
            for k in removeKeys:
                self.pop(k)

class TimeSecGroup(MyDict):
    def __init__(self, dim, timeSecStart, timeSecEnd, groupedRowsObj):
        ### Dict over Group hashes
        super(TimeSecGroup, self).__init__()
        self._dim = dim
        self._timeSecStart = timeSecStart
        self._timeSecEnd = timeSecEnd
        self._groupedRowsObj = groupedRowsObj
        self._dirty = True

    @property
    def TimeSecStart(self):
        return self._timeSecStart

    @property
    def TimeSecEnd(self):
        return self._timeSecEnd

    @property
    def TotalSeconds(self):
        return self.TimeSecEnd - self.TimeSecStart

    @property
    def Dimension(self):
        return self._dim

    @property
    def Dirty(self):
        return self._dirty

    def _setDirty(self):
        self._dirty = True

    @property
    def GroupedRowsObj(self):
        return self._groupedRowsObj

    @property
    def MinParentTsWithFillGaps(self):
        retTS = self.TimeSecStart
        for g in self:
            parentTS = g.GParentWithFillGaps.TimeSecObj.TimeSecStart
            retTS = min(retTS, parentTS)
        return retTS

    def AddOrGetG(self, groupHash):
        g = self.GetG(groupHash)
        if g is None:
            g = GroupingGroup(groupHash, self)
            self[groupHash] = g
        return g

    def GetG(self, groupHash):
        if groupHash in self.keys():
            return self[groupHash]
        return None

    # TimeSecGroup: GroupingGroup dirty blocks
    def GetDirtyBlocks(self, clearDirty = False):
        for gGroup in self:
            if gGroup.Dirty:
                if clearDirty:
                    gGroup._dirty = False
                yield gGroup

class GroupingGroup(MyDict):
    def __init__(self, groupHash, timeSecObj):
        ### dict over source rows
        super(GroupingGroup, self).__init__()
        self._groupHash = groupHash
        self._dirty = False
        self._timeSecObj = timeSecObj
        self._worker = self.TimeSecObj.GroupedRowsObj.Worker
        self._setDirty()
        self._dimTableRow = None
        self._dimTableRowID = None

    @property
    def GroupHash(self):
        return self._groupHash

    @property
    def TimeSecObj(self):
        return self._timeSecObj

    @property
    def Dirty(self):
        return self._dirty

    @property
    def DimTableRow(self):
        return self._dimTableRow
    @DimTableRow.setter
    def DimTableRow(self, value):
        self._dimTableRow = value

    @property
    def DimTableRowID(self):
        return self._dimTableRowID
    @DimTableRowID.setter
    def DimTableRowID(self, value):
        self._dimTableRowID = value

    def _setDirty(self):
        if not self._dirty:
            self._dirty = True
            # next group is related to this?
            if self._worker is not None and self._worker.Config.FillGapsWithPreviousResult:
                g = self.NextGroupingGroup
                if g is not None and not len(g.Rows):
                    g._setDirty()
            # also set time sec dirty
            self._timeSecObj._setDirty()

    @property
    def Rows(self):
        return self

    @property
    def GParentWithFillGaps(self):
        retG = self
        if not len(self.Rows) and self._worker is not None and self._worker.Config.FillGapsWithPreviousResult:
            g = self.PrevGroupingGroup
            if g is not None:
                retG = g.GParentWithFillGaps
        return retG

    @property
    def RowsWithFillGaps(self):
        return self.GParentWithFillGaps.Rows

    @property
    def PrevGroupingGroup(self):
        prevTS = self.TimeSecObj.GroupedRowsObj.GetTSBefore(self.TimeSecObj.TimeSecStart)
        if prevTS:
            return prevTS.GetG(self.GroupHash)
        return None

    @property
    def NextGroupingGroup(self):
        nextTS = self.TimeSecObj.GroupedRowsObj.GetTSAfter(self.TimeSecObj.TimeSecStart)
        if nextTS:
            return nextTS.GetG(self.GroupHash)
        return None

    def AddRow(self, sRow):
        if sRow.Id in self:
            # source row changed, update it
            if self[sRow.Id] != sRow:
                self[sRow.Id] = sRow
                self._setDirty()
        else:
            self[sRow.Id] = sRow
            self._setDirty()
        return sRow

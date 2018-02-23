#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from MyIterBase import MyIterBase


class GroupedRows(MyIterBase):
    def __init__(self, worker):
        self._timeSecGroups = {}
        super(GroupedRows, self).__init__(self._timeSecGroups)
        self._worker = worker

    @property
    def Worker(self):
        return self._worker

    def AddOrGetTS(self, dim, timeSecStart, timeSecEnd):
        ts = self.GetTS(timeSecStart)
        if not ts:
            ts = TimeSecGroup(dim, timeSecStart, timeSecEnd, self)
            self._timeSecGroups[timeSecStart] = ts
        return ts

    def GetTS(self, timeSecStart):
        if timeSecStart in self.keys():
            return self[timeSecStart]
        return None

    def GetTSBefore(self, timeSec):
        keysSmaller = [k for k in self.keys() if k < timeSec]
        if keysSmaller:
            return self[max(keysSmaller)]
        return None

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
        keyStack = []
        for obj in self:
            keyStack.append(obj.TimeSecStart)
            if obj.Dirty:
                break
        # we reached end of list, or first dirty item
        removeKeys = keyStack[:-1-keepBeforeDirty]
        for k in removeKeys:
            self._timeSecGroups.pop(k)

class TimeSecGroup(MyIterBase):
    def __init__(self, dim, timeSecStart, timeSecEnd, groupedRowsObj):
        self._groupings = {}
        super(TimeSecGroup, self).__init__(self._groupings)
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

    def AddOrGetG(self, groupHash):
        g = self.GetG(groupHash)
        if not g:
            g = GroupingGroup(groupHash, self)
            self._groupings[groupHash] = g
        return g

    def GetG(self, groupHash):
        if groupHash in self.keys():
            return self._groupings[groupHash]
        return None

    # TimeSecGroup: GroupingGroup dirty blocks
    def GetDirtyBlocks(self, clearDirty = False):
        for gGroup in self:
            if gGroup.Dirty:
                if clearDirty:
                    gGroup._dirty = False
                yield gGroup


class GroupingGroup(MyIterBase):
    def __init__(self, groupHash, timeSecObj):
        self._rows = {}
        super(GroupingGroup, self).__init__(self._rows)
        self._groupHash = groupHash
        self._timeSecObj = timeSecObj
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
        self._dirty = True
        self._timeSecObj._setDirty()

    @property
    def Rows(self):
        return self._rows

    @property
    def RowsWithFillGaps(self):
        myRows = self.Rows
        if not len(myRows) and self.TimeSecObj.GroupedRowsObj.Worker.Config:
            g = self.PrevGroupingGroup
            if len(g):
                myRows = g.RowsWithFillGaps
        return myRows

    @property
    def PrevGroupingGroup(self):
        prevTS = self.TimeSecObj.GroupedRowsObj.GetTSBefore(self.TimeSecObj.TimeSecStart)
        if prevTS:
            return prevTS.GetG(self.GroupHash)
        return None

    def AddRow(self, sRow):
        if sRow.Id in self._rows:
            # source row changed, update it
            if self._rows[sRow.Id] != sRow:
                self._rows[sRow.Id] = sRow
                self._setDirty()
        else:
            self._rows[sRow.Id] = sRow
            self._setDirty()
        return sRow

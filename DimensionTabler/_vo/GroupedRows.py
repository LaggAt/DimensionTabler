#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from MyIterBase import MyIterBase


class GroupedRows(MyIterBase):
    def __init__(self):
        self._timeSecGroups = {}
        super(GroupedRows, self).__init__(self._timeSecGroups)

    def AddOrGetTS(self, timeSec):
        ts = self.GetTS(timeSec)
        if not ts:
            ts = TimeSecGroup(timeSec, self)
            self._timeSecGroups[timeSec] = ts
        return ts

    def GetTS(self, timeSec):
        if timeSec in self._timeSecGroups:
            return self._timeSecGroups[timeSec]
        return None

    def GetTSBefore(self, timeSec):
        keysSmaller = [k for k in self.keys() if k < timeSec]
        if keysSmaller:
            return self[max(keysSmaller)]
        return None

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
            keyStack.append(obj.TimeSec)
            if obj.Dirty:
                break
        # we reached end of list, or first dirty item
        removeKeys = keyStack[:-1-keepBeforeDirty]
        for k in removeKeys:
            self._timeSecGroups.pop(k)

class TimeSecGroup(MyIterBase):
    def __init__(self, timeSec, groupedRowsObj):
        self._groupings = {}
        super(TimeSecGroup, self).__init__(self._groupings)
        self._timeSec = timeSec
        self._groupedRowsObj = groupedRowsObj
        self._dirty = True

    @property
    def TimeSec(self):
        return self._timeSec

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
        if groupHash in self._groupings:
            return self._groupings[groupHash]
        return None

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

    def _setDirty(self):
        self._dirty = True
        self._timeSecObj._setDirty()

    def AddRow(self, sRow):
        #TODO: possibly update a row if values changed, for now we ignore dupes
        if sRow.Id in self._rows:
            pass
        else:
            self._rows[sRow.Id] = sRow
            self._setDirty()


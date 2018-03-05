#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

class MyDict(dict):
    def __init__(self, *args, **kwargs):
        self._sortedList = None
        super(MyDict, self).__init__(*args, **kwargs)
    def __setitem__(self, key, value):
        self._sortedList = None
        return super(MyDict, self).__setitem__(key, value)
    def __delitem__(self, key):
        self._sortedList = None
        return super(MyDict, self).__delitem__(key)
    def pop(self, key):
        self._sortedList = None
        return super(MyDict, self).pop(key)
    # get a list of values
    def values(self):
        # use the sorted order, build a list out of the dict.
        lst = []
        for e in self:
            lst.append(e)
        return lst
    # get an iterator of sorted objects
    def __iter__(self):
        for k in self.Sorted:
            yield self[k]
    # get an iterator of sorted keys
    def keys(self):
        return self.Sorted

    @property
    def Sorted(self):
        if self._sortedList is None:
            self._sortedList = sorted(super(MyDict, self).__iter__())
        return self._sortedList

    def GetPrevKey(self, key):
        idx = self.Sorted.index(key)
        if idx == 0:
            return None
        else:
            return self.Sorted[idx-1]

    def GetNextKey(self, key):
        idx = self.Sorted.index(key)
        if idx+1 == len(self.Sorted):
            return None
        else:
            return self.Sorted[idx + 1]

    def _getKeyOrNone(self, key):
        if key is None:
            return None
        return self[key]

    def GetPrev(self, key):
        return self._getKeyOrNone(self.GetPrevKey(key))

    def GetNext(self, key):
        return self._getKeyOrNone(self.GetNextKey(key))
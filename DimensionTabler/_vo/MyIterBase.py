#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler._utils.myDict import MyDict

#TODO maybe use MyDict directly instead of this class?
class MyIterBase(object):
    def __init__(self, theDict):
        super(MyIterBase, self).__init__()
        #if type(theDict) is not MyDict:
        #    raise Exception("Use MyDict() instead of dict!")
        self._theDict = theDict

    # get an iterator of sorted objects
    def __iter__(self):
        for k in sorted(self._theDict):
            yield self._theDict[k]

    # get an iterator of sorted keys
    def keys(self):
        for k in sorted(self._theDict):
            yield k

    # get a specific item with instance[index]
    def __getitem__(self, index):
        return self._theDict[index]

    # get count of items
    def __len__(self):
        return len(self._theDict)

    # get a list of values
    def values(self):
        # use the sorted order, build a list out of the dict.
        lst = []
        for e in self:
            lst.append(e)
        return lst

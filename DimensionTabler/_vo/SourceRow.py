#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

import urllib
import hashlib
from datetime import *

class SourceRow(object):
    def __init__(self, nameLst, row):
        super(SourceRow, self).__init__()
        self._timeSec = None
        self._groups = {}
        self._vars = {}
        self._fx = {}
        fieldLst = zip(nameLst, row)
        self._idName, self._id = fieldLst[0]
        # default fx for id
        for field in [
            "fx_first_%s_first" % (self._idName),
            "fx_last_%s_last" % (self._idName)
        ]:
            self._fx[field] = self._id
        for field, value in fieldLst[1:]:
            if field == "time_sec":
                self._timeSec = value
            elif field.startswith("group_"):
                self._groups[field] = value
            elif field.startswith("var_"):
                varName = "@" + field
                self._vars[varName] = value
            elif field.startswith("fx_"):
                self._fx[field] = value
        if not self._timeSec:
            raise Exception("We need a time_sec column (this will probably change in further versions).")
        self._groupHash = hashlib.sha256(urllib.urlencode(self._groups)).hexdigest()
        self._fullHash =  hashlib.sha256(urllib.urlencode(fieldLst)).hexdigest()

    @property
    def Id(self):
        return self._id
    @property
    def TimeSec(self):
        return self._timeSec
    @property
    def GroupHash(self):
        return self._groupHash
    @property
    def Grouping(self):
        return self._groups
    @property
    def UtcDate(self):
        return datetime.utcfromtimestamp(self._timeSec)
    @property
    def Vars(self):
        return self._vars
    @property
    def Fx(self):
        return self._fx

    def __eq__(self, other):
        return self._fullHash == other._fullHash
    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "ID %s = '%s'. time_sec = %s, equality hash %s, grouping = %s, vars = %s" % (
            self._idName, self._id, self._timeSec, self._groupHash, repr(self._groups), repr(self._vars))


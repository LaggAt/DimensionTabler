#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

import fx
from callbackHandler import _callback

class FxHandler(object):
    def __init__(self, worker):
        self._groupResults = {}
        self._worker = worker
        self._postProcDict = {}
        if worker:
            # no post processing for SchemaUpdater (where worker = None)
            self._postProcDict = worker._config.PostProcessorDict
    
    def AggregateGroupResults(self, group):
        sourceRowLst = group.values()
        if len(sourceRowLst):
            # timeSec & Grouping
            for f in sourceRowLst[0].Fx:
                ignore, method, name = f.split("_", 2)
                valueLst = [e.Fx[f] for e in sourceRowLst]
                fxFunc = fx.__dict__[method]
                self._callFunc(fxFunc, name, valueLst)
            for g in sourceRowLst[0].Grouping:
                # just pass it thro
                self._callFunc(fx.forward, g, sourceRowLst[0].Grouping[g])
            for v in sourceRowLst[0].Vars:
                valueLst = [e.Vars[v] for e in sourceRowLst]
                # for vars persist most current value
                self._callFunc(fx.last, v[1:], valueLst)
        else:
            return None
        return self._groupResults

    def _callFunc(self, fxFunc, name, valueLst):
        # call function
        result = fxFunc(valueLst)
        # post processing
        if name in self._postProcDict.keys():
            # we do not want to write unexpected data, so we fail here if custom fx fails!
            ppFx = self._postProcDict[name]
            result = ppFx(self, result)
        # return
        self._groupResults[name] = result

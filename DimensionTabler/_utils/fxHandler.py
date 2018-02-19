#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

import fx

def AggregateGroupResults(group):
    groupResults = {}
    sourceRowLst = group.values()
    if len(sourceRowLst):
        # timeSec & Grouping
        for f in sourceRowLst[0].Fx:
            ignore, method, name = f.split("_", 2)
            valueLst = [e.Fx[f] for e in sourceRowLst]
            fxFunc = fx.__dict__[method]
            groupResults[name] = fxFunc(valueLst)
        for g in sourceRowLst[0].Grouping:
            groupResults[g] = sourceRowLst[0].Grouping[g]
        for v in sourceRowLst[0].Vars:
            valueLst = [e.Vars[v] for e in sourceRowLst]
            # for vars persist most current value
            groupResults[v[1:]] = fx.last(valueLst)
    else:
        return None
    return groupResults

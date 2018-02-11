#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler.DimTabConfig import DimTabConfig
from more_itertools import one
from DimensionTabler._utils import fx, iterUtil
import urllib

class Cumulator(object):
    def __init__(self, timeSecSnapshot, config):
        super(Cumulator, self).__init__()
        self._groupedRows = {}
        self._dimTableBlock = {}
        self._config = config
        self._createDimensions(timeSecSnapshot, config.Dimensions)

    def _createDimensions(self, timeSecSnapshot, dimensions):
        self._dimensionPast = one(
            [dim for dim in dimensions if dim.TimeSec == DimTabConfig.DIMENSION_TIMESEC_PAST])
        self._dimensionStartingTimeSec = {}
        dimensionsOrdered = sorted(
            [dim for dim in dimensions if dim.TimeSec <> DimTabConfig.DIMENSION_TIMESEC_PAST],
            key = lambda dim: dim.TimeSec)
        for dim in dimensionsOrdered:
            # we want the same ranges within a timebox, so get start of timebox:
            start = timeSecSnapshot + dim.TimeSec # past is negative, so +
            startRounded = (start // dim.GranularitySec) * dim.GranularitySec
            self._dimensionStartingTimeSec[startRounded] = dim

    def AddRow(self, row):
        timeSecStart = 0
        dim = self._dimensionPast
        for timeSecStart in self._dimensionStartingTimeSec:
            if row.TimeSec >= timeSecStart:
                dim = self._dimensionStartingTimeSec[timeSecStart]
            else:
                break
        timeSecGroup = (row.TimeSec // dim.GranularitySec) * dim.GranularitySec
        # create structure self._groupedRows[timeSecGroup]['groups']['hash'][row,row,...]
        if not timeSecGroup in self._groupedRows:
            self._groupedRows[timeSecGroup] = {
                'dirty': True, # changed since cumulation
                'dimension': dim,
                'groups': {}
            }
        if not row.GroupHash in self._groupedRows[timeSecGroup]['groups']:
            self._groupedRows[timeSecGroup]['groups'][row.GroupHash] = []
        #duplicate check & add
        if not any(r.Id == row.Id for r in self._groupedRows[timeSecGroup]['groups'][row.GroupHash]):
            self._groupedRows[timeSecGroup]['groups'][row.GroupHash].append(row)
            self._groupedRows[timeSecGroup]['dirty'] = True #not already persisted
        #TODO: eventually cumulate and clean in blocks, not on every row, but often enough to show almost real time data.
        # Maybe time based if no cumulation happend for more than 30s?
        #cumulate & delete old data
        timeSecObsolete = timeSecGroup - dim.GranularitySec
        for timeSecBlock in self._groupedRows.keys():
            rowsBlock = self._groupedRows[timeSecBlock]
            if rowsBlock['dirty']:
                blockResult = self._cumulateBlock(rowsBlock)
                # TODO: sync self._dimTableBlock with time_sec = timeSecGroup and all fields to db table
                for blockHash in blockResult:
                    if (not timeSecGroup in self._dimTableBlock.keys()) or (not blockHash in self._dimTableBlock[timeSecGroup].keys()) \
                    or (urllib.urlencode(blockResult[blockHash]) != urllib.urlencode(self._dimTableBlock[timeSecGroup][blockHash])):
                        self._updateDimensionTableRow(timeSecGroup, blockResult[blockHash])
                # these are written, so remember and clear dirty flag
                self._dimTableBlock[timeSecGroup] = blockResult
                rowsBlock['dirty'] = False
            if timeSecBlock <= timeSecObsolete and not rowsBlock['dirty']:
                self._groupedRows.pop(timeSecBlock)
        pass

    def _cumulateBlock(self, block):
        blockResults = {}
        for groupHash in block['groups']:
            group = block['groups'][groupHash]
            groupResults = {}
            if len(group):
                #timeSec & Grouping
                for f in group[0].Fx:
                    ignore, method, name = f.split("_", 2)
                    valueLst = [e.Fx[f] for e in group]
                    groupResults[name] = fx.__dict__[method](valueLst)
                for g in group[0].Grouping:
                    groupResults[g] = group[0].Grouping[g]
            else:
                pass #group is empty
            blockResults[groupHash] = groupResults
        return blockResults

    def _updateDimensionTableRow(self, timeSecGroup, dtRow):
        fields = [e for e in dtRow if not e.startswith("group_")]
        fieldsValues = [dtRow[f] for f in fields]
        groups = [e for e in dtRow if     e.startswith("group_")]
        groupsValues = [dtRow[f] for f in groups]
        groups.append('time_sec')
        groupsValues.append(timeSecGroup)

        db = self._config.Db
        # update or insert?
        sql = "SELECT " + ", ".join(groups) + \
            " FROM " + self._config.Name + \
            " WHERE " + " and ".join([e + " = %s" for e in groups]) + ";"
        params = groupsValues
        try:
            cur = db.cursor()
            cur.execute(sql, params)
            dbRow = cur.fetchone()
        except db.Error as e:
            raise e

        # try update
        if dbRow:
            sql = "UPDATE " + self._config.Name + " SET " + ", ".join([e + " = %s" for e in fields]) + " " + \
                  "WHERE " + " and ".join([e + " = %s" for e in groups]) + ";"
            params = fieldsValues + \
                 groupsValues
        else:
            #insert
            sql = "INSERT " + self._config.Name + " (" + ", ".join(fields + groups) + ") " + \
                  "VALUES(" + ", ".join(["%s" for e in fields + groups]) + ");"
            params = fieldsValues + groupsValues
        try:
            cur = db.cursor()
            cur.execute(sql, params)
            db.commit()
        except db.Error as e:
            raise e


#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler.DimTabConfig import DimTabConfig
from more_itertools import one
from DimensionTabler._utils import fxHandler, iterUtil
import urllib
from DimensionTabler._vo.DimensionTableRow import DimensionTableRow

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
            sourceRowLst = block['groups'][groupHash]
            groupResults =  fxHandler.AggregateGroupResults(sourceRowLst)
            blockResults[groupHash] = groupResults
        return blockResults

    def _updateDimensionTableRow(self, timeSecGroup, sourceRow):
        dimT = DimensionTableRow(timeSecGroup, sourceRow)

        db = self._config.Db
        # update or insert?
        sql = "SELECT " + ", ".join(dimT.Groups) + \
            " FROM " + self._config.Name + \
            " WHERE " + " and ".join([e + " = %s" for e in dimT.Groups]) + ";"
        params = dimT.GroupsValues
        try:
            cur = db.cursor()
            cur.execute(sql, params)
            dbRow = cur.fetchone()
            cur.close()
        except db.Error as e:
            raise e

        # try update
        if dbRow:
            sql = "UPDATE " + self._config.Name + \
                  " SET " + ", ".join([e + " = %s" for e in dimT.Fields]) + " " + \
                  "WHERE " + " and ".join([e + " = %s" for e in dimT.Groups]) + ";"
            params = dimT.FieldsValues + \
                     dimT.GroupsValues
        else:
            #insert
            sql = "INSERT " + self._config.Name + " (" + ", ".join(dimT.Fields + dimT.Groups) + ") " + \
                  "VALUES(" + ", ".join(["%s" for e in dimT.Fields + dimT.Groups]) + ");"
            params = dimT.FieldsValues + dimT.GroupsValues
        try:
            cur = db.cursor()
            cur.execute(sql, params)
            db.commit()
            cur.close()
        except db.Error as e:
            raise e


#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler.DimTabConfig import DimTabConfig
from more_itertools import one
from DimensionTabler._utils import fxHandler
import urllib
from DimensionTabler._vo.DimensionTableRow import DimensionTableRow
from _utils import datetimeUtil


class Cumulator(object):
    def __init__(self, timeSecSnapshot, config):
        super(Cumulator, self).__init__()
        self._groupedRows = {}
        self._dimTableBlock = {}
        self._config = config
        self._currentRow = None
        self._currentTimeSec = 0
        self._lastUpdateSeconds = timeSecSnapshot
        self._createDimensions(timeSecSnapshot, config.Dimensions)

    @property
    def CurrentRow(self):
        return self._currentRow
    @property
    def CurrentTimeSec(self):
        return self._currentTimeSec

    @property
    def CurrentDimension(self):
        dim = self._dimensionPast
        for key in sorted(self._dimensionStartingTimeSec):
            if key <= self.CurrentTimeSec:
                dim = self._dimensionStartingTimeSec[key]
                break
        if not dim:
            return ""
        return dim

    @property
    def NextDimension(self):
        dim = self._dimensionPast
        for key in sorted(self._dimensionStartingTimeSec):
            if key > self.CurrentTimeSec:
                dim = self._dimensionStartingTimeSec[key]
                break
        if not dim:
            return ""
        return dim

    def _createDimensions(self, timeSecSnapshot, dimensions):
        self._dimensionPast = one(
            [dim for dim in dimensions if dim.TimeSec == DimTabConfig.DIMENSION_TIMESEC_PAST])
        self._dimensionStartingTimeSec = {}
        dimensionsOrdered = sorted(
            [dim for dim in dimensions if dim.TimeSec <> DimTabConfig.DIMENSION_TIMESEC_PAST],
            key = lambda dim: dim.TimeSec)
        for dim in dimensionsOrdered:
            # we want the same ranges within a timebox, so get start of timebox:
            start = self._getDimStartSec(timeSecSnapshot, dim)
            self._dimensionStartingTimeSec[start] = dim

    """ shifts dimensions to new start times, 
        and if it shifted a dimension returns the old start time (if more than one, the first) """
    def FirstTimeOfShiftedDimension(self, timeSecSnapshot):
        firstTimeSec = None
        newDimensionStartingTimeSec = {}
        dim = self._dimensionPast
        for oldSec in sorted((self._dimensionStartingTimeSec).keys()):
            oldDim = dim
            dim = self._dimensionStartingTimeSec[oldSec]
            newSec = self._getDimStartSec(timeSecSnapshot, dim)
            newDimensionStartingTimeSec[newSec] = dim
            # start granularity of previous dimension earlier (or current granularity if in this curious case we have worse resolution in newer data)
            singleGranularity = max(oldDim.GranularitySec, dim.GranularitySec)
            startBeforeTimeSec = oldSec
            if singleGranularity:
                startBeforeTimeSec = (startBeforeTimeSec // singleGranularity) * singleGranularity
            # did we move forward with this time box?
            if oldSec < newSec:
                # there is no earlier time box we will work on
                if firstTimeSec is None:
                    # are we already working that early
                    if startBeforeTimeSec < self._currentTimeSec:
                        self._currentTimeSec = firstTimeSec = startBeforeTimeSec
        if firstTimeSec:
            self._dimensionStartingTimeSec = newDimensionStartingTimeSec
        return firstTimeSec

    def _getDimStartSec(self, timeSecSnapshot, dim):
        start = timeSecSnapshot
        if dim.TimeSec:
            start = (start // -dim.TimeSec) * -dim.TimeSec
        return start

    def AddRow(self, row):
        # make them available for callbacks
        self._currentTimeSec = row.TimeSec
        self._currentRow = row
        # get the dimension this row is in
        timeSecGroup = row.TimeSec
        dim = self._getDimensionForTimeSec(timeSecGroup)
        # we are in dimension dim, determine group time_sec this row is in
        if dim.GranularitySec:
            timeSecGroup = (timeSecGroup // dim.GranularitySec) * dim.GranularitySec
        # create structure self._groupedRows[timeSecGroup]['groups']['hash'][row,row,...]
        #we have no row in that group, so create it (and all missing in between the previous one)
        self._addGroupedRows(dim, timeSecGroup)
        # if we havn't seen this grouping, this is also missing
        if not row.GroupHash in self._groupedRows[timeSecGroup]['groups']:
            self._groupedRows[timeSecGroup]['groups'][row.GroupHash] = []
        #duplicate check for row & add
        if not any(r.Id == row.Id for r in self._groupedRows[timeSecGroup]['groups'][row.GroupHash]):
            self._groupedRows[timeSecGroup]['groups'][row.GroupHash].append(row)
            self._groupedRows[timeSecGroup]['dirty'] = True #not already persisted
        # Maybe time based if no cumulation happend for more than 30s?
        #cumulate & delete old data
        timeSecObsolete = timeSecGroup - dim.GranularitySec
        self.CumulateTimeSec()
        for timeSecBlock in sorted(self._groupedRows.keys()):
            rowsBlock = self._groupedRows[timeSecBlock]
            if timeSecBlock <= timeSecObsolete and not rowsBlock['dirty']:
                self._groupedRows.pop(timeSecBlock)

    def _getDimensionForTimeSec(self, checkTimeSec):
        dim, timeSecDim = self._getDimensionAndTimeSecForTimeSec(checkTimeSec)
        return dim
    def _getDimensionAndTimeSecForTimeSec(self, checkTimeSec):
        timeSecDim = 0
        dim = self._dimensionPast
        for timeSecKey in sorted((self._dimensionStartingTimeSec).keys()):
            if checkTimeSec >= timeSecKey:
                dim = self._dimensionStartingTimeSec[timeSecKey]
                timeSecDim = timeSecKey
            else:
                break
        return dim, timeSecDim

    def _addGroupedRows(self, dim, timeSecGroup):
        # create this dimension group
        if not timeSecGroup in self._groupedRows:
            self._groupedRows[timeSecGroup] = {
                'dirty': True,  # changed since cumulation
                'dimension': dim,
                'groups': {}
            }
        # is there an earlier time_sec?
        if [k for k in self._groupedRows.keys() if k < timeSecGroup]:
            # get earlier dimension group
            earlierTimeSec = timeSecGroup - 1
            earlierDim, earlierTimeSecDim = self._getDimensionAndTimeSecForTimeSec(earlierTimeSec)
            if earlierDim.GranularitySec:
                earlierTimeSec = (earlierTimeSec // earlierDim.GranularitySec) * earlierDim.GranularitySec
            # create that earlier group (and all earliers to the next existing
            self._addGroupedRows(earlierDim, earlierTimeSec)
            # add groups (hashes) for earlier groups, needed for deletion of old lines or generation of missing data
            for earlierGroupHash in self._groupedRows[earlierTimeSec]['groups']:
                if not earlierGroupHash in self._groupedRows[timeSecGroup]['groups']:
                    self._groupedRows[timeSecGroup]['groups'][earlierGroupHash] = []
                    self._groupedRows[timeSecGroup]['dirty'] = True
        pass

    def CumulateTimeSec(self):
        # cumulate and update only every 10 seconds
        now = datetimeUtil.getUtcNowSeconds()
        if now >= self._lastUpdateSeconds + 1:
            self._lastUpdateSeconds = now
            for timeSecBlock in sorted(self._groupedRows.keys()):
                rowsBlock = self._groupedRows[timeSecBlock]
                if rowsBlock['dirty']:
                    # all cumulation is done here
                    blockResult = self._cumulateBlock(rowsBlock)
                    # TODO: sync self._dimTableBlock with time_sec = timeSecGroup and all fields to db table
                    for blockHash in blockResult:
                        # just update database if changed
                        if (not timeSecBlock in self._dimTableBlock.keys()) or (
                        not blockHash in self._dimTableBlock[timeSecBlock].keys()) \
                                or (urllib.urlencode(blockResult[blockHash]) != urllib.urlencode(
                            self._dimTableBlock[timeSecBlock][blockHash])):
                            self._updateDimensionTableRow(timeSecBlock, blockResult[blockHash], rowsBlock['groups'][blockHash])
                    # these are written, so remember and clear dirty flag
                    self._dimTableBlock[timeSecBlock] = blockResult
                    rowsBlock['dirty'] = False

    def _cumulateBlock(self, block):
        blockResults = {}
        for groupHash in block['groups']:
            sourceRowLst = block['groups'][groupHash]
            if len(sourceRowLst):
                groupResults = fxHandler.AggregateGroupResults(sourceRowLst)
                blockResults[groupHash] = groupResults
            else:
                # Fill gaps with previous time_sec result?
                if self._config.FillGapsWithPreviousResult:
                    pass #TODO: find previous result and set it as result for blockResults[groupHash]
                else:
                    pass # leave the gaps
        return blockResults

    def _updateDimensionTableRow(self, timeSecGroup, cumulatedRow, sourceRowLst):
        #TODO per config co linking to source rows, see sourceRowLst
        dimT = DimensionTableRow(timeSecGroup, cumulatedRow)
        db = self._config.Db
        dbRow = None

        # update or insert?
        sql = "SELECT " + ", ".join(dimT.Groups) + \
            " FROM " + self._config.Name + \
            " WHERE " + " and ".join([e + " = %s" for e in dimT.Groups]) + ";"
        params = dimT.GroupsValues
        with db as cur:
            cur.execute(sql, params)
            dbRow = cur.fetchone()
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
        with db as cur:
            cur.execute(sql, params)

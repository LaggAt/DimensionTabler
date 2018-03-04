#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from copy import copy
from DimensionTabler.DimTabConfig import DimTabConfig
from more_itertools import one
import urllib
from DimensionTabler._utils.fxHandler import FxHandler
from DimensionTabler._vo.DimensionTableRow import DimensionTableRow
from DimensionTabler._vo.GroupedRows import GroupedRows
from DimensionTabler._utils import datetimeUtil
from DimensionTabler._utils.callbackHandler import _callback
from DimensionTabler.DimTabEvArgs import *

class Cumulator(object):
    def __init__(self, worker):
        super(Cumulator, self).__init__()
        self._groupedRows = GroupedRows(worker)
        #self._dimTableBlock = {}
        self._worker = worker
        self._config = worker._config
        self._lastCumulationSec = datetimeUtil.getUtcNowSeconds()
        self._lastMemCleanupSec = datetimeUtil.getUtcNowSeconds()

    def AddRow(self, row):
        # get the dimension this row is in
        dim, timeSecStart, timeSecEnd = \
            self._worker.Dimensions.GetDimensionAndTimeSecSlotStartAndEndForTimeSec(row.TimeSec)
        _callback(self._worker, self._config.OnSourceRow,
              SourceRowEvArgs(row, dim, timeSecStart, timeSecEnd))
        # create structure self._groupedRows...
        tempTsG = self._addTSGroup(dim, timeSecStart, timeSecEnd)
        tempG =   tempTsG.AddOrGetG(row.GroupHash)
        tempRow = tempG.AddRow(row)
        #cumulate
        self.DoCumulate()

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

    def _addTSGroup(self, dim, timeSecStart, timeSecEnd):
        # create this dimension group
        ts = self._groupedRows.AddOrGetTS(dim, timeSecStart, timeSecEnd)
        # is there an earlier time_sec?
        tsBefore = self._groupedRows.GetTSBefore(timeSecStart)
        if tsBefore:
            # create that earlier group (and all earliers to the next existing
            earlierTS = self._addTSGroup(tsBefore, tsBefore.TimeSecStart, tsBefore.TimeSecEnd)
            # add groups (hashes) for earlier groups, needed for deletion of old lines or generation of missing data
            for earlierG in earlierTS:
                g = ts.AddOrGetG(earlierG.GroupHash)
        return ts

    def DoCumulate(self, force = False):
        # cumulate and update only every seconds
        now = datetimeUtil.getUtcNowSeconds()
        if force or (now >= self._lastCumulationSec + self._config.WaitSecondsBeforeCumulating):
            dirtyBlocks = self._groupedRows.GetDirtyBlocks(clearDirty=True)
            for block in dirtyBlocks:
                # cumulate block and create dimension table row
                agregatedSRow = FxHandler(self._worker).AggregateGroupResults(block)
                if agregatedSRow:
                    block.DimTableRow = DimensionTableRow(block.TimeSecObj.TimeSecStart, agregatedSRow)
                else:
                    if self._config.FillGapsWithPreviousResult:
                        gBefore = block.TimeSecObj.GroupedRowsObj\
                            .GetTSBefore(block.TimeSecObj.TimeSecStart) \
                            .GetG(block.GroupHash)
                        if gBefore is None or gBefore.DimTableRow is None:
                            block.DimTableRow = None
                        else:
                            block.DimTableRow = DimensionTableRow(block.TimeSecObj.TimeSecStart, gBefore.DimTableRow.SourceRow)
                    else:
                        #no row for that!
                        block.DimTableRow = None
                self._updateDimensionTableRow(block)
            if now >= self._lastMemCleanupSec + self._config.WaitSecondsBeforeMemCleanup:
                self._groupedRows.RemoveOldBlocks()
                self._lastMemCleanupSec = datetimeUtil.getUtcNowSeconds()
            # set the stopwatch again
            self._lastCumulationSec = datetimeUtil.getUtcNowSeconds()

    def _updateDimensionTableRow(self, block):
        dimT = block.DimTableRow
        db = self._config.Db

        # delete eventually unneeded rows
        isBlockEmpty = dimT is None
        sqlDelete = "DELETE FROM " + self._config.Name + \
            " WHERE grp_hash = %s" + \
            " AND time_sec " + (">=" if isBlockEmpty else ">") + " %s" + \
            " AND time_sec < %s"
        paramsDelete = (
            block.GroupHash,
            block.TimeSecObj.TimeSecStart,
            block.TimeSecObj.TimeSecEnd
        )
        with db as cur:
            cur.execute(sqlDelete, paramsDelete)
            deleteCount = cur.rowcount
        if deleteCount:
            deleteEvArgs = DtDeleteEvArgs(
                block=block, count=deleteCount, isBlockEmpty=isBlockEmpty)
            _callback(self._worker, self._config.OnDtDelete, deleteEvArgs)

        # if no dimension table entry: delete only
        if isBlockEmpty:
            return

        # update or insert?
        id = 0
        sql = "SELECT id, " + ", ".join(block.DimTableRow.Fields) + \
            " FROM " + self._config.Name + \
            " WHERE time_sec = %s and grp_hash = %s"
        params = (block.TimeSecObj.TimeSecStart, block.GroupHash)
        with db as cur:
            cur.execute(sql, params)
            dbRow = cur.fetchone()
            isUpdate = False
            if dbRow:
                block.DimTableRowID = dbRow[0] # from select
                isUpdateNeeded = False
                for i in range(1, len(block.DimTableRow.Fields)):
                    fromSelect = dbRow[i+1]
                    fromVo = block.DimTableRow.FieldsValues[i]
                    if fromSelect != fromVo:
                        isUpdateNeeded = True
                        break
        # try update
        if dbRow and isUpdateNeeded:
            sql = "UPDATE " + self._config.Name + \
                  " SET " + ", ".join([e + " = %s" for e in dimT.Fields]) + \
                  "     , grp_hash = %s, time_sec_update = %s " + \
                  " WHERE time_sec = %s and grp_hash = %s;"
            params = dimT.FieldsValues + \
                     [block.GroupHash, datetimeUtil.getUtcNowSeconds()] + \
                     [block.TimeSecObj.TimeSecStart, block.GroupHash]
            with db as cur:
                cur.execute(sql, params)
                interDeleteCnt, interInsertCnt = self._updateIntermediateTable(cur, block)
            _callback(self._worker, self._config.OnDtUpdate,
                DtUpdateEvArgs(block, id, sql, params, interDeleteCnt, interInsertCnt))
        elif not dbRow:
            #insert
            sql = "INSERT " + self._config.Name + " (" + ", ".join(dimT.Fields + dimT.Groups) + \
                  "    , grp_hash, time_sec_insert) " + \
                  "VALUES(" + ", ".join(["%s" for e in dimT.Fields + dimT.Groups]) + \
                  "    , %s, %s);"
            params = dimT.FieldsValues + dimT.GroupsValues + \
                     [block.GroupHash, datetimeUtil.getUtcNowSeconds()]
            with db as cur:
                cur.execute(sql, params)
                block.DimTableRowID = cur.lastrowid # get id from insert
                interDeleteCnt, interInsertCnt = self._updateIntermediateTable(cur, block)
            _callback(self._worker, self._config.OnDtInsert,
                DtInsertEvArgs(block, id, sql, params, interDeleteCnt, interInsertCnt ))
        else:
            with db as cur:
                interDeleteCnt, interInsertCnt = self._updateIntermediateTable(cur, block)
        pass

    def _updateIntermediateTable(self, cur, block):
        deleteCount = 0
        insertCount = 0
        #link dim table row "id" to the source rows "block.Rows"
        if self._config.IntermediateTable:
            sourceRowIDLst = block.RowsWithFillGaps.keys()
            id = block.DimTableRowID

            # remove rows not in set
            cIT = self._config.IntermediateTable
            sqlDelete = "DELETE FROM " + cIT.TableName + \
                        " WHERE " + cIT.DimTableID + " = %s "
            if not len(sourceRowIDLst):
                # remove all rows with dimension table id
                paramsDelete = [id]
            else:
                sqlDelete += " AND " + cIT.SourceID + " NOT IN (" + ", ".join(["%s" for e in sourceRowIDLst]) + ");"
                paramsDelete = [id] + sourceRowIDLst
            cur.execute(sqlDelete, paramsDelete)
            deleteCount = cur.rowcount

            # add row links
            insertCount = 0
            if len(sourceRowIDLst):
                # add rows not in list
                sqlInsert = "INSERT INTO " + cIT.TableName + " (" + cIT.DimTableID + ", " + cIT.SourceID + ")" + \
                    " SELECT %s as leftid, " + \
                    "     righttab.rightid " + \
                    " FROM (" + " UNION ".join(["SELECT %s as rightid" for e in sourceRowIDLst]) + ") as righttab" + \
                    " LEFT JOIN " + cIT.TableName + " as inter_table " + \
                    "     ON  inter_table." + cIT.DimTableID + " = %s " + \
                    "     AND inter_table." + cIT.SourceID + " = righttab.rightid " + \
                    " WHERE inter_table." + cIT.SourceID + " is null;"
                paramsInsert = [id] + sourceRowIDLst + [id]
                cur.execute(sqlInsert, paramsInsert)
                insertCount = cur.rowcount
        return deleteCount, insertCount
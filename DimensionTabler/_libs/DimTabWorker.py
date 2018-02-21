#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from Cumulator import Cumulator
from DimensionTabler._utils import datetimeUtil
from DimensionTabler._utils.callbackHandler import _callback
from DimensionTabler._vo.SourceRow import SourceRow
from DimensionTabler._vo.Dimensions import Dimensions
from _libs.SchemaUpdater import SchemaUpdater
from _vo.JumpBackEvArgs import JumpBackEvArgs


class DimTabWorker(object):
    def __init__(self, config):
        super(DimTabWorker, self).__init__()
        self._config = config
        self._currentSourceRow = None
        self._isSchemaOK = False
        self._jumpBackBeforeSec = 0
        self._currentTimeSec = 0
        self._dimensions = Dimensions(self._config.Dimensions, self._cbJumpbackNeeded)
        self._cumulator = Cumulator(self)

    @property
    def Dimensions(self):
        return self._dimensions

    @property
    def CurrentTimeSec(self):
        return self._currentTimeSec
    def _setGetCurrentTimeSec(self):
        self._currentTimeSec = datetimeUtil.getUtcNowSeconds()
        self._dimensions.UpdateDimensions(self._currentTimeSec)
        return self._currentTimeSec

    def _prepareSqlLst(self):
        sqlLst = []
        for varConfig in self._config.VariableConfigLst:
            val = str(varConfig.Value)
            sqlLst.append(varConfig.Sql.replace("VALUE", val))
        sqlLst.append(self._config.SqlMain)
        return sqlLst

    def _getData(self):
        db = self._config.Db
        with db as cur:
            for sql in self._prepareSqlLst():
                cur.execute(sql)
            nameLst = [x[0] for x in cur.description]
            rows = cur.fetchall()
        for row in rows:
            sRow = SourceRow(nameLst, row)
            if not self._isSchemaOK:
                SchemaUpdater(self, cur, sRow)
                self._isSchemaOK = True
            yield sRow

    def _updateVars(self, lastRow):
        for varConfig in self._config.VariableConfigLst:
            if lastRow is None: #back to defaults
                varConfig.Value = varConfig.ValueDefault
            else:
                varConfig.Value = lastRow.Vars[varConfig.Name]

    @property
    def Config(self):
        return self._config
    @property
    def CurrentSourceRow(self):
        return self._currentSourceRow
    @property
    def Cumulator(self):
        return self._cumulator

    def _cbJumpbackNeeded(self, timeSec):
        # we need to work on older data to match dimension table again
        self._jumpBackBeforeSec = timeSec

    def Work(self):
        #TODO: is this right? dont we need the time from the data rows for jumpback handling?
        self._setGetCurrentTimeSec()
        # run most current batch (beginning with last run time max)
        self._workBatch()

        # jump back if Dimensions tell us to do so, OR if we get data from the future
        now = datetimeUtil.getUtcNowSeconds()
        if self._jumpBackBeforeSec or self.CurrentTimeSec > now:
            if not self._jumpBackBeforeSec:
                # this is the case we got data from future, step back to current
                self._jumpBackBeforeSec = now
            if self.CurrentTimeSec > self._jumpBackBeforeSec:
                #find dimension table row earlier fromTimeSec, create/update/delete all rows from that
                sRowStartPoint = self._setOldStartPoint(self._jumpBackBeforeSec)
                _callback(self, self._config.OnJumpBack,
                    JumpBackEvArgs(sRowStartPoint, self._jumpBackBeforeSec, self.CurrentTimeSec))
                # reset jump back time
                self._jumpBackBeforeSec = 0
                # we are probably up-to-date, to save time, start immediatly:
                self._workBatch()

    def _setOldStartPoint(self, beforeTimeSec):
        db = self._config.Db
        sRow = None
        with db as cur:
            sql = "select * from %s WHERE time_sec <= %s ORDER BY time_sec desc LIMIT 1" % (
                self.Config.Name, beforeTimeSec)
            cur.execute(sql)
            row = cur.fetchone()
            if row:
                nameLst = [x[0] for x in cur.description]
                sRow = SourceRow(nameLst, row)
        self._updateVars(sRow)
        return sRow

    def _workBatch(self):
        batchHasData = True
        while batchHasData:
            batchHasData = False
            for row in self._getData():
                batchHasData = True
                self._currentSourceRow = row
                _callback(self, self._config.OnSourceRow)
                self._cumulator.AddRow(row)
            if batchHasData:
                self._updateVars(row)
        self._cumulator.DoCumulate()
        _callback(self, self._config.OnBatchCurrent)
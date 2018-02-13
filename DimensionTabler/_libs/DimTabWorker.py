#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from Cumulator import Cumulator
from DimensionTabler._utils import datetimeUtil
from DimensionTabler._utils.callbackHandler import _callback
from DimensionTabler._vo.SourceRow import SourceRow
from _libs.SchemaUpdater import SchemaUpdater


class DimTabWorker(object):
    def __init__(self, config):
        super(DimTabWorker, self).__init__()
        self._config = config
        self._currentSourceRow = None
        self._isSchemaOK = False

    def _prepareSqlLst(self):
        sqlLst = []
        for varConfig in self._config.VariableConfigLst:
            val = str(varConfig.Value)
            sqlLst.append(varConfig.Sql.replace("VALUE", val))
        sqlLst.append(self._config.SqlMain)
        return sqlLst # ";\n".join(sqlLst)

    def _getData(self):
        db = self._config.Db
        try:
            cur = db.cursor()
            for sql in self._prepareSqlLst():
                cur.execute(sql)
            nameLst = [x[0] for x in cur.description]
            rows = cur.fetchall()
            for row in rows:
                sRow = SourceRow(nameLst, row)
                if not self._isSchemaOK:
                    SchemaUpdater(self._config, cur, sRow)
                    self._isSchemaOK = True
                yield sRow
        except db.Error as e:
            raise e
        finally:
            cur.close()

    def _updateVars(self, lastRow):
        for varConfig in self._config.VariableConfigLst:
            varConfig.Value = lastRow.Vars[varConfig.Name]

    @property
    def Config(self):
        return self._config
    @property
    def CurrentSourceRow(self):
        return self._currentSourceRow

    def Work(self):
        cumulator = Cumulator(datetimeUtil.getUtcNowSeconds(), self._config)
        batchHasData = True
        while batchHasData:
            batchHasData = False
            for row in self._getData():
                batchHasData = True
                self._currentSourceRow = row
                _callback(self, self._config._onSourceRow)
                cumulator.AddRow(row)
            if batchHasData:
                self._updateVars(row)
        _callback(self, self._config._onBatchCurrent)
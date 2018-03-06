#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler._utils.dbHandler import DbHandler
from DimensionTabler._utils import datetimeUtil
from datetime import timedelta

class DimTabConfig(object):
    def __init__(self, tableName):
        super(DimTabConfig, self).__init__()
        if not tableName:
            raise Exception("Init the config with a name.")
        self._name = tableName
        self._intermediateTable = None
        self._db = None
        self._sqlMain = ""
        self._variableConfigLst = []
        self._startAtEnd = False
        self._postProcessorDict = {}
        self._dimensions = []
        self._fillGapsWithPreviousResult = False
        self._waitSecondsBeforeCumulating = 10
        self._waitSecondsBeforeMemCleanup = 0
        self._onGetData = None
        self._onSourceRow = None
        self._onBatchCurrent = None
        self._onRedoPastRows = None
        self._onJumpBack = None
        self._onDtInsert = None
        self._onDtUpdate = None
        self._onDtDelete = None

    @property
    def Name(self):
        return self._name

    class IntermediateTableConfig(object):
        def __init__(self, tableName, sourceID, dimTableID):
            self._tableName = tableName
            self._sourceID = sourceID
            self._dimTableID = dimTableID
        @property
        def TableName(self):
            return self._tableName
        @property
        def SourceID(self):
            return self._sourceID
        @property
        def DimTableID(self):
            return self._dimTableID

    @property
    def IntermediateTable(self):
        return self._intermediateTable
    @IntermediateTable.setter
    def IntermediateTable(self, value):
        if not type(value) is DimTabConfig.IntermediateTableConfig and value is not None:
            raise Exception("Set only if you want a intermediate Table. If, set a IntermediateTableConfig")
        self._intermediateTable = value

    @property
    def Db(self):
        return DbHandler(self._db)
    @Db.setter
    def Db(self, value):
        self._db = value

    @property
    def SqlMain(self):
        return self._sqlMain
    @SqlMain.setter
    def SqlMain(self, value):
        """ set this to an sql which will gather data.
            first column will be used as identifier, name of first column will be used as name for that id.
            Column 'time_sec' is a unix timestamp for that line (currently we only support time box)
            Columns like 'group_%' will be used to group data in a time box
            Columns like 'var_%' contain variables, next sql uses last content as value. Init them in InitTuple
            Columns like 'fx_%' are aggregated by the named function
            supported functions see utils/fx.py. Currently first, last, min, max, avg, sum, count
            TODO: support calculations, which need more than one value. e.g. weighted avg using fx_wavg-value_NAME, fx_wavg-weight_NAME
            """
        self._sqlMain = value

    class VariableConfig(object):
        def __init__(self, var_NAME, sql, defaultValue):
            if not var_NAME:
                raise Exception("var_NAME must be specified.")
            if not unicode(sql).find("VALUE"):
                raise Exception("sql needs to be like: SET @var_iter = VALUE")
            self._varName = "@" + var_NAME
            self._sql = sql # must contain VALUE which will be replaced by the current value
            self._value = self._valueDefault = defaultValue
        @property
        def Name(self):
            return self._varName
        @property
        def Sql(self):
            return self._sql
        @property
        def Value(self):
            return self._value
        @Value.setter
        def Value(self, value):
            self._value = value
        @property
        def ValueDefault(self):
            return self._valueDefault
    @property
    def VariableConfigLst(self):
        return self._variableConfigLst
    @VariableConfigLst.setter
    def VariableConfigLst(self, value):
        if (type(value) is list) and (all(type(element) is DimTabConfig.VariableConfig for element in value)):
            self._variableConfigLst = value
        else:
            raise Exception("Value must be a list of DimTabConfig.VariableConfig.")

    @property
    def StartAtEnd(self):
        return self._startAtEnd

    @StartAtEnd.setter
    def StartAtEnd(self, value):
        if type(value) is bool:
            self._startAtEnd = value
        else:
            raise Exception("If you want to start at the last dt-line written set this True. Possibly misses jump back's.")

    @property
    def PostProcessorDict(self):
        return self._postProcessorDict
    @PostProcessorDict.setter
    def PostProcessorDict(self, value):
        if not type(value) is dict:
            raise Exception("Pass a dictionary with field names as key.")
        for key in value:
            self._postProcessor = self._validCallback(value[key], 2, "<FxHandler_Instance>, <Result from FX>. Check key %s" % (key))
        self._postProcessorDict = value

    DIMENSION_TIMESEC_PAST   = "PAST"
    class DimensionConfig(object):
        def __init__(self, description, timeSec, granularitySec):
            self._description = description
            if (not type(timeSec) is int) and (not timeSec == DimTabConfig.DIMENSION_TIMESEC_PAST):
                raise Exception("timeSec must be number of seconds or a DIMENSION_TIMESEC_* constant.")
            self._timeSec = timeSec
            self._isPast = False
            self._isNewest = False
            if timeSec == DimTabConfig.DIMENSION_TIMESEC_PAST:
                self._isPast = True
            if not type(granularitySec) is int:
                raise Exception("granularitySec needs to be the wanted granularity in seconds.")
            self._granularitySec = granularitySec #dont div/0
        @property
        def Description(self):
            return self._description
        @property
        def TimeSec(self):
            return self._timeSec

        @property
        def TimeSecReadable(self):
            if self.TimeSec > 0:
                return "in %s" % (timedelta(seconds=self.TimeSec),)
            elif self.TimeSec < 0:
                return "%s ago" % (timedelta(seconds=-self.TimeSec),)
            return "now"
        @property
        def IsPast(self):
            return self._isPast
        @property
        def IsNewest(self):
            return self._isNewest
        @property
        def GranularitySec(self):
            return self._granularitySec
    @property
    def Dimensions(self):
        return self._dimensions
    @Dimensions.setter
    def Dimensions(self, value):
        if (type(value) is list) and (all(type(element) is DimTabConfig.DimensionConfig for element in value)):
            self._dimensions = value
        else:
            raise Exception("Value must be a list of DimensionTablerConfig.DimensionConfig.")

    @property
    def FillGapsWithPreviousResult(self):
        return self._fillGapsWithPreviousResult

    @FillGapsWithPreviousResult.setter
    def FillGapsWithPreviousResult(self, value):
        if type(value) is bool:
            self._fillGapsWithPreviousResult = value
        else:
            raise Exception(
                "Value must be a bool. True fills empty time_sec/groups with results from previous time_sec")

    @property
    def WaitSecondsBeforeCumulating(self):
        return self._waitSecondsBeforeCumulating

    @WaitSecondsBeforeCumulating.setter
    def WaitSecondsBeforeCumulating(self, value):
        if type(value) is int:
            self._waitSecondsBeforeCumulating = value
        else:
            raise Exception(
                "Value must be seconds.")

    @property
    def WaitSecondsBeforeMemCleanup(self):
        if self._waitSecondsBeforeMemCleanup:
            return self._waitSecondsBeforeMemCleanup
        else:
            return self._waitSecondsBeforeCumulating * 10

    @WaitSecondsBeforeMemCleanup.setter
    def WaitSecondsBeforeMemCleanup(self, value):
        if type(value) is int:
            self._waitSecondsBeforeMemCleanup = value
        else:
            raise Exception(
                "Value must be seconds. Defaults to WaitSecondsBeforeCumulating * 10.")

    @property
    def OnGetData(self):
        return self._onGetData

    @OnGetData.setter
    def OnGetData(self, callback):
        self._onGetData = self._validCallback(callback, 2, "<DimTabWorker instance>, <GetDataEvArgs>")

    @property
    def OnSourceRow(self):
        return self._onSourceRow

    @OnSourceRow.setter
    def OnSourceRow(self, callback):
        self._onSourceRow = self._validCallback(callback, 2, "<DimTabWorker instance>, <evArgs>")

    @property
    def OnBatchCurrent(self):
        return self._onBatchCurrent
    @OnBatchCurrent.setter
    def OnBatchCurrent(self, callback):
        self._onBatchCurrent = self._validCallback(callback, 2, "<DimTabWorker instance>, <evArgs>")

    @property
    def OnJumpBack(self):
        return self._onJumpBack
    @OnJumpBack.setter
    def OnJumpBack(self, callback):
        self._onJumpBack = self._validCallback(callback, 2, "<DimTabWorker instance>, <evArgs>")

    @property
    def OnDtInsert(self):
        return self._onDtInsert
    @OnDtInsert.setter
    def OnDtInsert(self, callback):
        self._onDtInsert = self._validCallback(callback, 2, "<Cumulator instance>, <evArgs>")

    @property
    def OnDtUpdate(self):
        return self._onDtUpdate
    @OnDtUpdate.setter
    def OnDtUpdate(self, callback):
        self._onDtUpdate = self._validCallback(callback, 2, "<Cumulator instance>, <evArgs>")

    @property
    def OnDtDelete(self):
        return self._onDtDelete
    @OnDtDelete.setter
    def OnDtDelete(self, callback):
        self._onDtDelete = self._validCallback(callback, 2, "<Cumulator instance>, <evArgs>")

    def _validCallback(self, callback, argCount, argumentHelpText):
        if callback.func_code.co_argcount != argCount:
            raise Exception("Wrong parameter count. Syntax: %s(%s)" % (callback.func_code.co_name, argumentHelpText))
        return callback

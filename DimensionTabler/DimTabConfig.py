#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

class DimTabConfig(object):
    def __init__(self, tableName):
        super(DimTabConfig, self).__init__()
        if not tableName:
            raise Exception("Init the config with a name.")
        self._name = tableName
        self._db = None
        self._sqlMain = ""
        self._variableConfigLst = []
        self._dimensions = []
        self._onSourceRow = None
        self._onBatchCurrent = None

    @property
    def Name(self):
        return self._name

    @property
    def Db(self):
        return self._db
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

    DIMENSION_TIMESEC_PAST   = "PAST"
    class DimensionConfig(object):
        def __init__(self, description, timeSec, granularitySec):
            self._description = description
            if (not type(timeSec) is int) and (not timeSec == DimTabConfig.DIMENSION_TIMESEC_PAST):
                raise Exception("timeSec must be number of seconds or a DIMENSION_TIMESEC_* constant.")
            self._timeSec = timeSec
            if not type(granularitySec) is int:
                raise Exception("granularitySec needs to be the wanted granularity in seconds.")
            self._granularitySec = 1 if granularitySec == 0 else granularitySec #dont div/0
        @property
        def Description(self):
            return self._description
        @property
        def TimeSec(self):
            return self._timeSec
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

    # we allow a single callback function whenever we start working on a source row
    @property
    def OnSourceRow(self):
        return self._onSourceRow
    @OnSourceRow.setter
    def OnSourceRow(self, callback):
        self._onSourceRow = self._validCallback(callback, 1, "<DimTabWorker instance>")

    @property
    def OnBatchCurrent(self):
        return self._onBatchCurrent
    @OnSourceRow.setter
    def OnBatchCurrent(self, callback):
        self._onBatchCurrent = self._validCallback(callback, 1, "<DimTabWorker instance>")

    def _validCallback(self, callback, argCount, argumentHelpText):
        if callback.func_code.co_argcount != 1:
            raise Exception("Wrong parameter count. Syntax: %s(%s)" % (callback.func_code.co_name, argumentHelpText))
        return callback

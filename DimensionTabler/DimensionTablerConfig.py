
class DimensionTablerConfig(object):
    def __init__(self, name):
        super(DimensionTablerConfig, self).__init__()
        if not name:
            raise Exception("Init the config with a name.")
        self._name = name
        self._sqlMain = ""
        self._variableConfigLst = []

    @property
    def Name(self):
        return self._name

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
            Columns like 'fx_%' are aggregated by the named function (planned for future releases)
            todo: fx_first, fx_last, fx_min, fx_max
            """
        self._sqlMain = value

    class VariableConfig(object):
        def __init__(self, var_NAME, sql, defaultValue):
            if not var_NAME:
                raise Exception("var_NAME must be specified.")
            if not unicode(sql).find("VALUE"):
                raise Exception("sql needs to be like: SET @var_iter = VALUE;")
            self._varName = ""
            self._sql = "" # must contain VALUE which will be replaced by the current value
            self._value = defaultValue
    @property
    def VariableConfigLst(self):
        return self._initDict
    @VariableConfigLst.setter
    def VariableConfigLst(self, value):
        if (type(value) is list) and (all(type(element) is DimensionTablerConfig.VariableConfig for element in value)):
            self._variableConfigLst = value
        else:
            raise Exception("Value must be a list of DimensionTablerConfig.VariableConfig.")



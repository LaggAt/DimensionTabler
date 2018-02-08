import urllib
import hashlib

class sourceRow(object):
    def __init__(self, nameLst, row):
        super(sourceRow, self).__init__()
        self._timeSec = None
        self._groups = {}
        self._vars = {}
        fieldLst = zip(nameLst, row)
        self._idName, self._id = fieldLst[0]
        for field, value in fieldLst[1:]:
            if field == "time_sec":
                self._timeSec = value
                #TODO: also persist time_sec for time box?
            elif field.startswith("group_"):
                self._groups[field] = value
            elif field.startswith("var_"):
                varName = "@" + field
                self._vars[varName] = value
        if not self._timeSec:
            raise Exception("We need a time_sec column (this will probably change in further versions).")
        self._hash = hashlib.sha256(urllib.urlencode(self._groups)).hexdigest()

    @property
    def Vars(self):
        return self._vars

    def __eq__(self, other):
        return self._hash == other._hash
    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "ID %s = '%s'. time_sec = %s, equality hash %s, grouping = %s, vars = %s" % (
            self._idName, self._id, self._timeSec, self._hash, repr(self._groups), repr(self._vars))

class DimensionTablerWorker(object):
    def __init__(self, config):
        super(DimensionTablerWorker, self).__init__()
        self._config = config

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
                yield sourceRow(nameLst, row)
        except db.Error as e:
            raise e

    def _updateVars(self, lastRow):
        for varConfig in self._config.VariableConfigLst:
            varConfig.Value = lastRow.Vars[varConfig.Name]

    def Work(self):
        batchHasData = True
        while batchHasData:
            batchHasData = False
            for row in self._getData():
                batchHasData = True
                #TODO: work on these items & create dimension table
                print row
            if batchHasData:
                self._updateVars(row)
        print("Batch %s is current." % (self._config.Name,))

        # TODO: also delete outdated dimensions
        pass

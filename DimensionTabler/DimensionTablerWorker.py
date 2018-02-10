from Cumulator import Cumulator
from utils import datetimeUtil
from vo.SourceRow import SourceRow


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
                yield SourceRow(nameLst, row)
        except db.Error as e:
            raise e

    def _updateVars(self, lastRow):
        for varConfig in self._config.VariableConfigLst:
            varConfig.Value = lastRow.Vars[varConfig.Name]

    def Work(self):
        cumulator = Cumulator(datetimeUtil.getUtcNowSeconds(),
                self._config)
        batchHasData = True
        while batchHasData:
            batchHasData = False
            for row in self._getData():
                batchHasData = True
                print row #TODO: remove later, show progress somehow
                cumulator.AddRow(row)
            if batchHasData:
                self._updateVars(row)
        print("Batch %s is current." % (self._config.Name,))

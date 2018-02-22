#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler import *
from DimensionTabler.DimTabEvArgs import *
from DimensionTabler._utils.datetimeUtil import *
import decimal

# DB-Api connection
import MySQLdb as mdb
db = mdb.connect('localhost', 'dimensiontabler_demo', 'demo4711', 'dimensiontabler_demo');

# now we create an instance of DimTabConfig for each dimension table we want:
def getDTTickerConfig():
    # here is the table name
    config = DimTabConfig("dt_ticker")

    #Intermediate table name:
    # Don't set, if you do not want a linking table between the source rows and the dimension table.
    # Here we want to see where data is coming from, so:
    config.IntermediateTable = config.IntermediateTableConfig('ticker__dt_ticker', 'ticker_id', 'dt_ticker_id')

    # database connection to use (currently only mysql is tested, should work with any DB-Api compatible db).
    config.Db = db

    # the sql must be ordered by time_sec, and must contain some fields. That said:
    #   first column must be identifier of the detail table (We might add support for a linking table later).
    #   time_sec is a unix timestamp, we use that for grouping into time windows
    #   group_* fields will group results, here we use it to get a result per time box and currency
    #   var_* are variables. see config.VariableConfigLst below.
    #       in this example we use this feature for paging
    #   fx_<method>_* are methods. Currently we only support methods using only a single input. This will change.
    #       for a list of supported methods see ./DimensionTabler/_utils/fx.py
    config.SqlMain = """
        SELECT 
            ticker.id as wallet_id, 
            CAST(UNIX_TIMESTAMP(ticker.dt) AS SIGNED) AS time_sec,
            currency as group_currency,
            CAST(UNIX_TIMESTAMP(ticker.dt) AS SIGNED) as var_iter,
            price as fx_first_price_open, 
            price as fx_last_price_close, 
            price as fx_min_price_low, 
            price as fx_max_price_high,
            price as fx_avg_price_average
        FROM ticker
        WHERE CAST(UNIX_TIMESTAMP(ticker.dt) AS SIGNED) > @var_iter
        -- order MUST always be time_sec asc
        ORDER BY ticker.dt
        LIMIT 0,500
    """

    #Variables: List of variables used in SQL.
    #   They will be initialized with a initial value. A var_* field updates them on each data row.
    config.VariableConfigLst = [
        # they come with: a name without @, a SQL to initialize them including VALUE, the initial VALUE
        DimTabConfig.VariableConfig("var_iter", "SET @var_iter = VALUE", 0),
    ]

    #PostProcessorDict: Dict with field name and post-processing function.
    # this is a dict with dimension table fields as key, and a function as value.
    # after using the aggregate function, this function is called. Signature see example fxRound8
    # These functions should never fail.
    # you could use fx_forward_* to forward the whole list to the post processing function.
    # TODO: do an example for fx_forward_*
    config.PostProcessorDict = {
        'price_average': fxRound8
    }

    # dimensions config: a human-readable name, a time in past/future in seconds, the granularity
    #   'time in past/future' and 'granularity': for example see the 3rd line:
    #       we want a dimension table entry every 15 minutes (granularity 15*60)
    #       for each source data line which time_sec is between 1 day ago (2nd line: -24*60*60) and 7 days ago.
    #       Got it?
    config.Dimensions = [
        DimTabConfig.DimensionConfig("  future",              0,        0),  # every value from future if any
        DimTabConfig.DimensionConfig("last  1h",         -60*60,        0),  # every value for last hour
        DimTabConfig.DimensionConfig("last  3h",       -3*60*60,       60),  # every minute a value for last 3 hours
        DimTabConfig.DimensionConfig("last  6h",       -6*60*60,     5*60),  # every 5 minutes for last 6 hours
        DimTabConfig.DimensionConfig("last 12h",      -12*60*60,    10*60),  # every 5 minutes for last 12 hours
        DimTabConfig.DimensionConfig("last  7d",    -7*24*60*60,    15*60),  # every 15' for 7 days
        DimTabConfig.DimensionConfig("last 30d",   -30*24*60*60,    60*60),  # every hour this month
        DimTabConfig.DimensionConfig("last 90d",   -90*24*60*60,  6*60*60),  # every 6 hours last 90 days
        DimTabConfig.DimensionConfig("one year",  -361*24*60*60, 12*60*60),  # every 12 hours for a year
        DimTabConfig.DimensionConfig("15' year",  -368*24*60*60,    15*60),  # get 7 days in 15' resolution at about 1 year
        DimTabConfig.DimensionConfig("  before",
                            DimTabConfig.DIMENSION_TIMESEC_PAST, 24*60*60),  # every day for all longer than 1y
    ]

    #Options: Fill Gaps
    # We create a dimension table line for each time_sec and group_*, by default if data is missing we ignore that.
    # with this setting on we fill these gaps with the previous result.
    config.FillGapsWithPreviousResult = True

    #Options: Wait before cumulating rows
    # In each cumulator run we summarize over all Groups of TimeSec-Blocks and Groupings where a source row was added.
    # A lower number means: we do that more often, therefore deliver faster, use more CPU and less RAM.
    # A higher number means: you have to wait for new source lines longer to be considered, therefore you have less
    # cpu usage, less I/O and more RAM usage. Default is 3 Seconds, which should be fine for most cases I think of.
    #for demo we use one second here, so you see more data updating.
    config.WaitSecondsBeforeCumulating = 15

    # keep us informed, pass a callback function. lambda isn't needed, we just wrap it up in a small class instance.
    callbackHandler = CallbackHandler()
    config.OnGetData = lambda worker, evArgs: callbackHandler.GetData(worker, evArgs)
    #config.OnSourceRow = lambda worker, evArgs: callbackHandler.InfoCallback(worker)
    config.OnBatchCurrent = lambda worker, evArgs: callbackHandler.BatchIsCurrent(worker)
    config.OnJumpBack = lambda worker, evArgs: callbackHandler.JumpBack(worker, evArgs)
    config.OnDtInsert = lambda worker, evArgs: callbackHandler.DtInsert(worker)
    config.OnDtUpdate = lambda worker, evArgs: callbackHandler.DtUpdate(worker)
    config.OnDtDelete = lambda worker, evArgs: callbackHandler.DtDelete(worker, evArgs)
    return config

def fxRound8(fxHandler, result):
    try:
        return result.quantize(decimal.Decimal("0.00000001"), decimal.ROUND_HALF_EVEN)
    except:
        return result

# callback examples:
class CallbackHandler(object):
    def __init__(self):
        super(CallbackHandler, self).__init__()
        self.cntSourceRows = 0
        self.cntInserted = 0
        self.cntUpdated = 0
        self.cntDelete = 0

    def GetData(self, worker, getDataEvArgs):
        print getDataEvArgs.Count,

    def InfoCallback(self, worker):
        self.cntSourceRows += 1
        # only output every 10th/100's row:
        if self.cntSourceRows % 10 == 0:
            print ".",
            if self.cntSourceRows % 100 == 0:
                print "Worker %s working on: %s" % (worker.Config.Name, worker.CurrentSourceRow)

    def BatchIsCurrent(self, worker):
        print("Batch %s is current, worked on %s rows. Dimension table rows inserted: %s, updated: %s, delete: %s." % (
            worker._config.Name, self.cntSourceRows, self.cntInserted, self.cntUpdated, self.cntDelete))

    def DtInsert(self, worker):
        self.cntInserted += 1
        if self.cntInserted % 10 == 0:
            print "i",

    def DtUpdate(self, worker):
        self.cntUpdated += 1
        if self.cntUpdated % 10 == 0:
            print "u",

    def DtDelete(self, worker, evArgs):
        """
        give PyCharm some hints of datatypes for AutoComplete.
        :type worker: DimTabWorker
        :type evArgs: DtDeleteEvArgs
        """
        tensBefore = self.cntDelete // 10
        self.cntDelete += evArgs.Count
        tensAfter = self.cntDelete // 10
        for i in range(tensBefore, tensAfter):
            print "d",

    def JumpBack(self, worker, jumpBackEvArgs):
        """
        :type worker: DimTabWorker
        :type jumpBackEvArgs: JumpBackEvArgs
        """
        print("Jumping back from %s to %s." % (
            getDateTimeFromSeconds(jumpBackEvArgs.WasOnSec),
            getDateTimeFromSeconds(jumpBackEvArgs.JumpBackBeforeSec)))

if __name__ == "__main__":
    # you probably want more than one dimension table, so we use a list here
    allConfigs = [
        # get that whole config block from above:
        getDTTickerConfig(),
    ]

    # get a instance of our runner
    runner = DimTab(allConfigs)

    # Dimension Tabler runs in a loop by default. Once it is finished, it will watch for new data every 1 seconds.
    # if you want to use another main loop just call runner._iteration() from it. Beware this could take a long time.
    # Updating every second is not necessary, if you're ok with waiting some seconds for new source rows. So we use 15s.
    runner.MainLoop(seconds=15)

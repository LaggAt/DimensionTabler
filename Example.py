#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler import *

# DB-Api connection
import MySQLdb as mdb
db = mdb.connect('localhost', 'dimensiontabler_demo', 'demo4711', 'dimensiontabler_demo');

# now we create an instance of DimTabConfig for each dimension table we want:
def getDTTickerConfig():
    # here is the table name
    config = DimTabConfig("dt_ticker")

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

    # Variables: List of variables used in SQL.
    #   They will be initialized with a initial value. A var_* field updates them on each data row.
    config.VariableConfigLst = [
        # they come with: a name without @, a SQL to initialize them including VALUE, the initial VALUE
        DimTabConfig.VariableConfig("var_iter", "SET @var_iter = VALUE", 0),
    ]

    # dimensions config: a human-readable name, a time in past/future in seconds, the granularity
    #   'time in past/future' and 'granularity': for example see the 3rd line:
    #       we want a dimension table entry every 15 minutes (granularity 15*60)
    #       for each source data line which time_sec is between 1 day ago (2nd line: -24*60*60) and 7 days ago.
    #       Got it?
    config.Dimensions = [
        DimTabConfig.DimensionConfig("  future",              0,        0),  # every value from future if any
        DimTabConfig.DimensionConfig("last  1h",         -60*60,        0),  # every value for last hour
        DimTabConfig.DimensionConfig("last  3h",       -3*60*60,       60),  # every minute a value for last 3 hours
        DimTabConfig.DimensionConfig("last  5h",       -5*60*60,     3*60),  # every value for last hour
        DimTabConfig.DimensionConfig("last 24h",      -24*60*60,     5*60),  # every minute for last day
        DimTabConfig.DimensionConfig("last  7d",    -7*24*60*60,    15*60),  # every 15' for 7 days
        DimTabConfig.DimensionConfig("last 30d",   -30*24*60*60,    60*60),  # every hour
        DimTabConfig.DimensionConfig("last 90d",   -90*24*60*60,  6*60*60),  # every 6 hours
        DimTabConfig.DimensionConfig("one year",  -361*24*60*60, 12*60*60),  # every 12 hours
        DimTabConfig.DimensionConfig("15' year",  -368*24*60*60,    15*60),  # get 7 days in 15' resolution again!
        DimTabConfig.DimensionConfig("  before",
                            DimTabConfig.DIMENSION_TIMESEC_PAST, 24*60*60),  # every day
    ]
    # keep us informed, pass a callback function. lambda isn't needed, we just wrap it up in a small class instance.
    callbackHandler = CallbackHandler()
    config.OnSourceRow = lambda worker: callbackHandler.InfoCallback(worker)
    config.OnBatchCurrent = lambda worker: callbackHandler.BatchIsCurrent(worker)
    config.OnJumpBack = lambda worker: callbackHandler.JumpBack(worker)
    return config

# callback examples:
class CallbackHandler(object):
    def __init__(self):
        super(CallbackHandler, self).__init__()
        self.cntSourceRows = 0

    def InfoCallback(self, worker):
        self.cntSourceRows += 1
        # only output every 100's row:
        if self.cntSourceRows % 10 == 0:
            print ".",
            if self.cntSourceRows % 100 == 0:
                print "Worker %s working on: %s" % (worker.Config.Name, worker.CurrentSourceRow)

    def BatchIsCurrent(self, worker):
        print("Batch %s is current, worked on %s rows." % (worker._config.Name, self.cntSourceRows))

    def JumpBack(self, worker):
        nextDimText = "-"
        dim = worker.Cumulator.NextDimension
        if dim:
            nextDimText = dim.Description
        print("Old dimensions are outdated, jumping back before dimension %s time_sec %s" % (
            nextDimText, worker.Cumulator.CurrentTimeSec,))

if __name__ == "__main__":
    # you probably want more than one dimension table, so we use a list here
    allConfigs = [
        # get that whole config block from above:
        getDTTickerConfig(),
    ]

    # get a instance of our runner
    runner = DimTab(allConfigs)

    # Dimension Tabler runs in a loop by default. Once it is finished, it will watch for new data every 10 seconds.
    # if you want to use another main loop just call runner._iteration() from it. Beware this could take a long time.
    runner.MainLoop()

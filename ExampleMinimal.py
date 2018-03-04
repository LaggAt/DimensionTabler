#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

# for more help see Example.py, here we show the bare minimum needed.

# import default configs and some modules we use
from DimensionTabler.DefaultConfigs import *
import decimal
import MySQLdb as mdb


def getDTTickerConfig():
    # instance of default config, set dimension table name:
    config = ConfDefault("dt_ticker")
    # connect database:
    config.Db = mdb.connect('localhost', 'dimensiontabler_demo', 'demo4711', 'dimensiontabler_demo')
    # Main-SQL is needed:
    config.SqlMain = """
        SELECT 
            ticker.id as ticker_id, 
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
    # variables for main sql
    config.VariableConfigLst = [
        DimTabConfig.VariableConfig("var_iter", "SET @var_iter = VALUE", 0),
    ]
    # some post processing (rounding to 8 decimal places here)
    config.PostProcessorDict = {
        'price_average': fxRound8
    }
    # return minimal feedback on console
    config.OnBatchCurrent = roundDone
    return config

def fxRound8(fxHandler, result):
    try:
        return result.quantize(decimal.Decimal("0.00000001"), decimal.ROUND_HALF_EVEN)
    except:
        return result

def roundDone(worker, evArgs):
    print ".",

if __name__ == "__main__":
    # get a list of configs per dimension table (here only one)
    allConfigs = [
        getDTTickerConfig(),
    ]
    # run in a loop
    runner = DimTab(allConfigs)
    runner.MainLoop(seconds=15)

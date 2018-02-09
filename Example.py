from DimensionTabler import *

# DB-Api connection
import MySQLdb as mdb
db = mdb.connect('localhost', 'dimensiontabler_demo', 'demo4711', 'dimensiontabler_demo');

def getDTTickerConfig():
    config = DimensionTabler.Config("demo")
    config.Db = db
    config.SqlMain = """
        SELECT 
            -- first column: identifier as identifier_name
            ticker.id as wallet_id, 
            -- column named time_sec is the unix timestamp we use for our time window
            CAST(UNIX_TIMESTAMP(ticker.dt) AS SIGNED) AS time_sec,
            -- columns named group_* group data. for every combination of groups we want exact 1 data line in a given time window
            currency as group_currency,
            -- columns with var_* will be initialized as variables. 
            -- This is needed for iterating over chunks of data (see where & limit) but can be used for anything else.
            -- Variables must be initialized 
            CAST(UNIX_TIMESTAMP(ticker.dt) AS SIGNED) as var_iter,
            -- Functions start with fx_<method>_ followed by a name 
            price as fx_first_price_open, 
            price as fx_last_price_close, 
            price as fx_min_price_low, 
            price as fx_max_price_high,
            price as fx_avg_price_average
        FROM ticker
        WHERE CAST(UNIX_TIMESTAMP(ticker.dt) AS SIGNED) > @var_iter
        ORDER BY ticker.dt
        LIMIT 0,500
    """
    config.VariableConfigLst = [
        DimensionTabler.Config.VariableConfig("var_iter", "SET @var_iter = VALUE", 0),
    ]
    config.Dimensions = [
        #DimensionTabler.Config.DimensionConfig("  future",              0,        0),  # all values from future if any
        #DimensionTabler.Config.DimensionConfig("last 24h",      -24*60*60,        0),  # all values for last day
        #DimensionTabler.Config.DimensionConfig("last  7d",    -7*24*60*60,    15*60),  # every 15' for 7 days
        DimensionTabler.Config.DimensionConfig("last 30d",   -30*24*60*60,    60*60),  # every hour
        DimensionTabler.Config.DimensionConfig("last 90d",   -90*24*60*60,  4*60*60),  # every 4 hours
        DimensionTabler.Config.DimensionConfig("  before",
                            DimensionTabler.Config.DIMENSION_TIMESEC_PAST, 24*60*60),  # every day
    ]
    return config

if __name__ == "__main__":
    allConfigs = [
        getDTTickerConfig(),
    ]
    runner = DimensionTabler(allConfigs)
    runner.MainLoop()

from DimensionTabler import *

def getDTTickerConfig():
    config = DimensionTabler.Config("demo")
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
            ticker.id as var_iter
        FROM ticker
        WHERE ticker.id > @var_iter
        LIMIT 0,50
    """
    config.VariableConfigLst = [
        DimensionTabler.Config.VariableConfig("var_iter", "SET @var_iter = VALUE;", 0),
    ]
    return config

if __name__ == "__main__":
    allConfigs = [
        getDTTickerConfig(),
    ]
    runner = DimensionTabler(allConfigs)
    runner.MainLoop()

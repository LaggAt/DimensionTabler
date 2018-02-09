from datetime import *

def getUtcNowSeconds():
    return getSecondsFromDateTime(datetime.utcnow())

def getSecondsFromDateTime(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())



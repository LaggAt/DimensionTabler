from DimensionTablerConfig import DimensionTablerConfig
from more_itertools import one

class Cumulator(object):
    def __init__(self, timeSecSnapshot, dimensions):
        super(Cumulator, self).__init__()
        self._groupedRows = {}
        self._createDimensions(timeSecSnapshot, dimensions)

    def _createDimensions(self, timeSecSnapshot, dimensions):
        self._dimensionPast = one(
            [dim for dim in dimensions if dim.TimeSec == DimensionTablerConfig.DIMENSION_TIMESEC_PAST])
        self._dimensionStartingTimeSec = {}
        dimensionsOrdered = sorted(
            [dim for dim in dimensions if dim.TimeSec <> DimensionTablerConfig.DIMENSION_TIMESEC_PAST],
            key = lambda dim: dim.TimeSec)
        for dim in dimensionsOrdered:
            # we want the same ranges within a timebox, so get start of timebox:
            start = timeSecSnapshot + dim.TimeSec # past is negative, so +
            startRounded = (start // dim.GranularitySec) * dim.GranularitySec
            self._dimensionStartingTimeSec[startRounded] = dim

    def AddRow(self, row):
        timeSecStart = 0
        dim = self._dimensionPast
        for timeSecStart in self._dimensionStartingTimeSec:
            if row.TimeSec >= timeSecStart:
                dim = self._dimensionStartingTimeSec[timeSecStart]
            else:
                break
        timeSecGroup = (row.TimeSec // dim.GranularitySec) * dim.GranularitySec
        # create structure self._groupedRows[timeSecGroup]['groups']['hash'][row,row,...]
        if not timeSecGroup in self._groupedRows:
            self._groupedRows[timeSecGroup] = {
                'dimension': dim,
                'groups': {}
            }
        if not row.GroupHash in self._groupedRows[timeSecGroup]['groups']:
            self._groupedRows[timeSecGroup]['groups'][row.GroupHash] = []
        #duplicate check & add
        if not any(r.Id == row.Id for r in self._groupedRows[timeSecGroup]['groups'][row.GroupHash]):
            self._groupedRows[timeSecGroup]['groups'][row.GroupHash].append(row)
        #TODO: cumulate
        #TODO: get rid of old data
        pass
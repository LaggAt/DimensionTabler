#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

from DimensionTabler._vo.DimensionTableRow import DimensionTableRow
from DimensionTabler._vo.GroupedRows import GroupedRows
from DimensionTabler._utils import fxHandler

class SchemaUpdater(object):
    def __init__(self, worker, cursor, sRow):
        config = worker._config
        super(SchemaUpdater, self).__init__()
        db = config.Db

        # get columns of existing table
        sql = "SELECT * FROM " + config.Name + " limit 1;"
        with db as cur:
            cur.execute(sql)
            columnMetadataDimTable = self._descriptionToMetadata(cur.description)

        # get needed schema from data row
        #CumulateBlock - pass no worker as we do not want to alter it
        dummyGroupingSchema = GroupedRows(None)
        dummyGroupingSchema\
            .AddOrGetTS(None, 0, 0)\
            .AddOrGetG('')\
            .AddRow(sRow)
        outputExampeRow = fxHandler.AggregateGroupResults(dummyGroupingSchema[0][''])

        # get some more details
        columnMetadataSource = {}
        metadataSource = self._descriptionToMetadata(cursor.description)
        dimT = DimensionTableRow(metadataSource['time_sec'], metadataSource)
        if config._db.__module__.startswith("MySQLdb"):
            import MySQLdb
            myFields = MySQLdb.FIELD_TYPE.__dict__
            typeNoAndTypes = {v: k for k, v in myFields.iteritems() if type(v) is int}
            for column in metadataSource:
                if (column in dimT.Fields) or (column in dimT.Groups):
                    typeName = typeNoAndTypes[metadataSource[column][0]]
                    columnMetadataSource[column] = typeName
        else:
            # we dont have information of data type
            for column in metadataSource:
                columnMetadataSource[column] = "unknown"

        # compare columnMetadataSource (data) against columnMetadataDimTable (dimension table)
        missingColumns = {}
        for colName in outputExampeRow:
            if colName not in columnMetadataDimTable:
                missingColumns[colName] = columnMetadataSource[colName]

        #TODO: ALTER TABLE config.Name ... with predefined mappings for data types
        if missingColumns:
            raise Exception("Dimension table '"+config.Name+"' misses these columns:\n    " +
                    ", ".join([n+" ("+missingColumns[n]+")" for n in missingColumns]))

    def _descriptionToMetadata(self, desc):
        columnMetadata = {}
        for col in desc:
            columnMetadata[col[0]] = col[1:]
        return columnMetadata

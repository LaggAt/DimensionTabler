#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

class DimensionTableRow(object):
    def __init__(self, timeSecGroup, sourceRow):
        fields = [e for e in sourceRow if not e.startswith("group_")]
        fieldsValues = [sourceRow[f] for f in fields]
        groups = [e for e in sourceRow if e.startswith("group_")]
        groupsValues = [sourceRow[f] for f in groups]
        groups.append('time_sec')
        groupsValues.append(timeSecGroup)

        self._fields = fields
        self._fieldsValues = fieldsValues
        self._groups = groups
        self._groupsValues = groupsValues

    @property
    def Fields(self):
        return self._fields
    @property
    def FieldsValues(self):
        return self._fieldsValues
    @property
    def Groups(self):
        return self._groups
    @property
    def GroupsValues(self):
        return self._groupsValues

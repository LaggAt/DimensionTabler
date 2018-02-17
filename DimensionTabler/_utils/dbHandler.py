#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

class DbHandler(object):
    def __init__(self, dbApiConnection):
        super(DbHandler, self).__init__()
        self._cursor = None
        self._db = dbApiConnection

    def __enter__(self):
        self._cursor = self._db.cursor()
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # No exception, so commit
            self._db.commit()
        else:
            # Exception occurred, so rollback.
            self._db.rollback()
            return False
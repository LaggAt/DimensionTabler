#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

import sys
import traceback

def _callback(inst, cb, evArgs = None):
    if cb:
        try:
            cb(inst, evArgs)
        except Exception as ex:
            print("Ignoring Exception %s in Callback for SourceRow:\n    %s" % (repr(ex)))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, file=sys.stdout)

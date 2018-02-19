#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

import time
from _libs.DimTabWorker import DimTabWorker
from DimTabConfig import DimTabConfig as Config
import sys
import traceback

class DimTab(object):

    def __init__(self, configLst):
        super(DimTab, self).__init__()
        self._workerLst = []
        if (type(configLst) is list) and (all(type(element) is Config for element in configLst)):
            for config in configLst:
                self._workerLst.append(
                    DimTabWorker(config)
                )
        else:
            raise Exception("Initialize with list of DimensionTabler.Config's.")

    def _iteration(self):
        for worker in self._workerLst:
            # catch any exception before main loop
            try:
                worker.Work()
            except Exception as ex:
                print("%s: %s" % (worker._config.Name, repr(ex)))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, file=sys.stdout)
                pass

    def MainLoop(self, seconds=1):
        while True:
            self._iteration()
            time.sleep(seconds)

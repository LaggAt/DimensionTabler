import time
from DimensionTablerWorker import DimensionTablerWorker

class DimensionTabler(object):
    from DimensionTablerConfig import DimensionTablerConfig as Config

    def __init__(self, configLst):
        super(DimensionTabler, self).__init__()
        self._workerLst = []
        if (type(configLst) is list) and (all(type(element) is DimensionTabler.Config for element in configLst)):
            for config in configLst:
                self._workerLst.append(
                    DimensionTablerWorker(config)
                )
        else:
            raise Exception("Initialize with list of DimensionTabler.Config's.")

    def _iteration(self):
        for worker in self._workerLst:
            # catch any exception before main loop
            try:
                worker.Work()
            except Exception as ex:
                print("%s: %s" % (worker._config.Name, ex.message))

    def MainLoop(self, seconds=10):
        while True:
            self._iteration()
            time.sleep(seconds)

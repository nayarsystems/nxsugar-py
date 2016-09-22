# -*- coding: utf-8 -*-

import sys
import time
sys.path.insert(0, "../..")
from nxsugarpy import *

if __name__ == "__main__":
    s = Service("root:root@localhost", "test.nxsugar.fibsrv", {"pulls": 4, "pullTimeout": 3600, "maxtThreads": 12})
    s.setLogLevel(DebugLevel)
    s.setStatsPeriod(5)

    # A method that computes fibonacci
    def fib(task):
        # Parse params
        try:
            v = int(task.params["v"])
        except:
            return None, newJsonRpcErr(ErrInvalidParams)
        tout = 0
        try:
            tout = float(task.params["t"])
        except:
            pass

        # Do work
        if tout > 0:
            time.sleep(tout)
        r = []
        i = 0
        j = 1
        while j < v:
            r.append(i)
            oldi = i
            i = i + j
            j = oldi
        return r, None
    s.addMethod("fib", fib)

    # Serve
    s.serve()

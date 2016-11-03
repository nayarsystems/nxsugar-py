# -*- coding: utf-8 -*-

import sys
sys.path.insert(0, "../..")
from nxsugarpy import *

def getConnState(conn, state):
    log(InfoLevel, "connState", "I got connState: ({0}) and conn ({1})", state, conn)
    if state == StateServing:
        err = conn.ping(20)
        if err == None:
            log(InfoLevel, "connState", "I did a ping with conn!")
        else:
            log(InfoLevel, "connState", "Ping failed with conn!")

if __name__ == "__main__":
    s = Service("root:root@localhost", "test.nxsugar.getconnready", {"pulls": 4, "pullTimeout": 3600, "maxtThreads": 12})
    s.setLogLevel(DebugLevel)
    s.setStatsPeriod(5)
    s.connState = getConnState

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
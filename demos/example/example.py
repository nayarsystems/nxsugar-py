# -*- coding: utf-8 -*-

import sys
import time
import threading
sys.path.insert(0, "../..")
from nxsugarpy import *

if __name__ == "__main__":
    s, err = newServiceFromConfig("example")
    if err != None:
        sys.exit(1)

    # A method that returns the service available methods
    def methods(task):
        return s.getMethods(), None
    s.addMethod("methods", replyToWrapper(methods))

    # A method that panics: an ErrInternal error will be returned as a result
    def panic(task):
        raise Exception("¿What if a method panics?")
        return "ok", None
    s.addMethod("panic", panic)

    # A method that calls stop()
    def exit(task):
        worker = threading.Thread(target=s.stop)
        worker.start()
        return "why?", None
    s.addMethod("exit", exit)

    # A method that calls gracefulStop()
    def gracefulExit(task):
        worker = threading.Thread(target=s.gracefulStop)
        worker.start()
        return "meh!", None
    s.addMethod("gracefulexit", gracefulExit)

    # A fibonacci method
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

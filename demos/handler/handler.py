# -*- coding: utf-8 -*-

import sys
sys.path.insert(0, "../..")
from nxsugarpy import *

if __name__ == "__main__":
    s, err = newServiceFromConfig("handler")
    if err != None:
        sys.exit(1)

    # A handler for all methods
    def handler(task):
        if task.method == "hello":
            return "bye", None
        return None, newJsonRpcErr(ErrMethodNotFound)

    s.setHandler(handler)

    # Serve
    s.serve()

# -*- coding: utf-8 -*-
##############################################################################
#
#    nxsugarpy, a Python library for building nexus services with python
#    Copyright (C) 2016 by the nxsugarpy team
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Lesser General Public License as published
#    by the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

# nexus errors
ErrParse                    = -32700
ErrInvalidRequest           = -32600
ErrInternal                 = -32603
ErrInvalidParams            = -32602
ErrMethodNotFound           = -32601
ErrTtlExpired               = -32011
ErrPermissionDenied         = -32010
ErrConnClosed               = -32007
ErrLockNotOwned             = -32006
ErrUserExists               = -32005
ErrInvalidUser              = -32004
ErrInvalidPipe              = -32003
ErrInvalidTask              = -32002
ErrCancel                   = -32001
ErrTimeout                  = -32000
ErrUnknownError             = -32098
ErrNotSupported             = -32099

# nxsugar errors
ErrTestingMethodNotProvided = 20000
ErrPactNotDefined           = 20001

#
ErrStr = {
    # nexus errors
    ErrParse:                    "Parse error",
    ErrInvalidRequest:           "Invalid request",
    ErrMethodNotFound:           "Method not found",
    ErrInvalidParams:            "Invalid params",
    ErrInternal:                 "Internal error",
    ErrTimeout:                  "Timeout",
    ErrCancel:                   "Cancel",
    ErrInvalidTask:              "Invalid task",
    ErrInvalidPipe:              "Invalid pipe",
    ErrInvalidUser:              "Invalid user",
    ErrUserExists:               "User already exists",
    ErrPermissionDenied:         "Permission denied",
    ErrTtlExpired:               "TTL expired",
    ErrLockNotOwned:             "Lock not owned",
    ErrConnClosed:               "Connection is closed",
    ErrUnknownError:             "Unknown error",
    ErrNotSupported:             "Not supported",

    # nxsugar errors
    ErrTestingMethodNotProvided: "Testing method not provided",
    ErrPactNotDefined:           "Pact not defined for provided input",
}

def newJsonRpcErr(code, message="", data=None):
    return {"code": code, "message": message, "data": data}

def formatAsJsonRpcErr(err):
    if not isinstance(err, dict):
        try:
            msg = str(err)
            return {"code": 0, "message": msg, "data": None}
        except:
            return {"code": 0, "message": "", "data": err}
    code = 0
    if "code" in err:
        code = err["code"]
    message = ""
    if "message" in err:
        message = err["message"]
    data = None
    if "data" in err:
        data = err["data"]
    return {"code": code, "message": message, "data": data}

def errToStr(err):
    code = 0
    if "code" in err:
        code = err["code"]
    message = ""
    if "message" in err:
        message = err["message"]
    return "[{0}] {1}".format(code, message)

def isNexusErrCode(err, code):
    err = formatAsJsonRpcErr(err)
    return err["code"] == code

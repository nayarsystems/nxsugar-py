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

from __future__ import print_function
import sys
import strict_rfc3339
import json

PanicLevel  = "panic"
FatalLevel  = "fatal"
ErrorLevel  = "error"
WarnLevel   = "warn"
InfoLevel   = "info"
DebugLevel  = "debug"

_jsonEnabled = False
_level = 0

def _eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def setJSONOutput(enabled):
    global _jsonEnabled
    if enabled:
        _jsonEnabled = True
    else:
        _jsonEnabled = False

def setLogLevel(level):
    global _level
    _level = _getLogLevelNum(level)

def _getLogLevelNum(level):
    if level == PanicLevel:
        return 5
    elif level == FatalLevel:
        return 4
    elif level == ErrorLevel:
        return 3
    elif level == WarnLevel:
        return 2
    elif level == InfoLevel:
        return 1
    else:
        return 0

def getLogLevel():
    if _level == 5:
        return PanicLevel
    elif _level == 4:
        return FatalLevel
    elif _level == 3:
        return ErrorLevel
    elif _level == 2:
        return WarnLevel
    elif _level == 1:
        return InfoLevel
    else:
        return DebugLevel

def log(level, path, message, *args, **kwargs):
    logWithFields(level, path, {}, message, *args, **kwargs)

def logWithFields(level, path, fields, message, *args, **kwargs):
    if _level <= _getLogLevelNum(level):
        try:
            msg = message.format(*args, **kwargs)
        except:
            msg = "<invalid format> msg[ {0} ] args[ {1} ] kwargs[ {2} ]".format(message, args, kwargs)
        if _jsonEnabled:
            jsonData = json.dumps(fields)
            print('{"time": "' + strict_rfc3339.now_to_rfc3339_utcoffset() + '", "level": "' + level + '", "path": "' + path + '", "msg": "' + msg + '", "data": ' + jsonData + '}', file=sys.stderr)
        else:
            level = level[:4].upper()
            print('''[{time}] [{level}] [{path}] {message}'''.format(time=strict_rfc3339.now_to_rfc3339_utcoffset(), level=level, path=path, message=msg), file=sys.stderr)

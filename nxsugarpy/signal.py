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

from __future__ import absolute_import
from .log import *
import signal

def disableSigintHandler():
    signal.signal(signal.SIGINT, _origSignalHandler)

def enableSigintHandler():
    global _origSignalHandler
    _origSignalHandler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _firstSignalHandler)

def _firstSignalHandler(sign, frame):
    signal.signal(signal.SIGINT, _secondSignalHandler)
    logWithFields(DebugLevel, "signal", {"type": "graceful_requested"}, "received SIGINT: stop gracefuly")
    for s in _stoppables:
        s.gracefulStop()

def _secondSignalHandler(sign, frame):
    logWithFields(DebugLevel, "signal", {"type": "stop_requested"}, "received SIGINT again: stop")
    for s in _stoppables:
        s.stop()

def addStoppable(s):
    global _stoppables
    _stoppables.append(s)

def clearStoppables():
    global _stoppables
    _stoppables = []

_stoppables = []
_origSignalHandler = None

enableSigintHandler()


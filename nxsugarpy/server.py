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

from nxsugarpy.log import *
from nxsugarpy.service import *
from nxsugarpy.service import _populateOpts

import threading
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty

class Server(object):
    def __init__(self, url):
        self.url = url
        self._showurl = ""
        self._connurl = ""
        self.user = ""
        self.password = ""
        self.pulls = 1
        self.pullTimeout = 3600
        self.maxThreads = 4
        self.logLevel = InfoLevel
        self.statsPeriod = 300
        self.gracefulExit = 20
        self.testing = False
        self.version = "0.0.0"

        self.connState = None
        self._nc = None
        self._services = {}
        self._addedAsStoppable = False

    def getConn(self):
        return self._nc

    def setUrl(self, url):
        self.url = url

    def setUser(self, user):
        self.user = user

    def setPassword(self, password):
        self.password = password

    def setLogLevel(self, l):
        self.logLevel = l

    def setStatsPeriod(self, t):
        self.statsPeriod = t
        for _, svc in self._services.items():
            svc.setStatsPeriod(t)

    def setGracefulExit(self, t):
        self.gracefulExit = t
        for _, svc in self._services.items():
            svc.setGracefulExit(t)

    def setVersion(self, major, minor, patch):
        self.version = "{0}.{1}.{2}".format(major, minor, patch)
        for _, svc in self._services.items():
            svc.version = self.version

    def setTesting(self, t):
        if t:
            self.testing = True
        else:
            self.testing = False
        for _, svc in self._services.items():
            svc.setTesting(self.testing)

    def isTesting(self):
        return self.testing

    def addService(self, name, path, opts=None):
        svc = Service(self.url, path, {"pulls": self.pulls, "pullTimeout": self.pullTimeout, "maxThreads": self.maxThreads, "testing": self.testing})
        svc.user = self.user
        svc.password = self.password
        svc.name = name
        svc.logLevel = self.logLevel
        svc.statsPeriod = self.statsPeriod
        svc.gracefulExit = self.gracefulExit
        svc.version = self.version
        if opts != None:
            opts = _populateOpts(opts)
            svc.pulls = opts["pulls"]
            svc.pullTimeout = opts["pullTimeout"]
            svc.maxThreads = opts["maxThreads"]
            svc.testing = opts["testing"]
            svc._preaction = opts["preaction"]
            svc._postaction = opts["postaction"]
        self._services[name] = svc
        return svc

    def _setState(self, state):
        if self.connState != None:
            self.connState(self.getConn(), state)

    def serve(self):
        self._setState(StateInitializing)

        # Check server
        if len(self._services) == 0:
            errs = "no services to serve"
            logWithFields(ErrorLevel, "server", {"type": "no_services"}, errs)
            return errs

        # Dial and login
        scheme, user, password, host, port = parseNexusUrl(self.url, self.user, self.password)
        self.user = user
        self.password = password
        self._showurl = "{0}://{1}:{2}".format(scheme, host, port)
        self._connurl = "{0}://{1}:{2}@{3}:{4}".format(scheme, user, password, host, port)
        for _, svc in self._services.items():
            svc.url = self.url
            svc._showurl = self._showurl
            svc._connurl = self._connurl
            svc.user = self.user
            svc.password = self.password

        self._setState(StateConnecting)
        try:
            self._nc = nxpy.Client(self._connurl)
        except Exception as e:
            errs = "can't connect to nexus server ({0}): {1}".format(self._showurl, str(e))
            logWithFields(ErrorLevel, "server", {"type": "connection_error"}, errs)
            return errs
        if not self._nc.is_version_compatible:
            logWithFields(WarnLevel, "server", {"type": "incompatible_version"}, "connecting to an incompatible version of nexus at ({0}): client ({1}) server ({2})", self._showurl, nxpy.__version__, self._nc.nexus_version)
        if not self._nc.is_logged:
            errs = "can't login to nexus server ({0}) as ({1}): {2}".format(self._showurl, self.user, errToStr(self._nc.login_error))
            logWithFields(ErrorLevel, "server", {"type": "login_error"}, errs)
            return errs

        # Configure services
        for _, svc in self._services.items():
            svc.setLogLevel(self.logLevel)
            svc._setConn(self._nc)

        # Serve
        errQ = Queue(len(self._services))
        serviceWorkers = []
        for _, svc in self._services.items():
            worker = threading.Thread(target=svc.serve, kwargs={"errQueue":errQ})
            worker.daemon = True
            serviceWorkers.append(worker)
            worker.start()

        if not self._addedAsStoppable:
            addStoppable(self)
            self._addedAsStoppable = True

        self._setState(StateServing)

        for worker in serviceWorkers:
            worker.join()

        self._nc = None
        self._setState(StateStopped)

        try:
            firstErr = errQ.get_nowait()
        except Empty:
            return None
        else:
            return firstErr

    def gracefulStop(self):
        for _, svc in self._services.items():
            svc.gracefulStop()

    def stop(self):
        for _, svc in self._services.items():
            svc.stop()
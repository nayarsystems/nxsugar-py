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

import pynexus as nxpy
import nxsugarpy.info as info
from nxsugarpy.log import *
from nxsugarpy.errors import *
from nxsugarpy.helpers import *
from nxsugarpy.stats import *
from nxsugarpy.signal import  *
from six import string_types

import time
import threading
import traceback

try:
    from Queue import Queue, Full
except ImportError:
    from queue import Queue, Full

StateInitializing, StateConnecting, StateServing, StateStopped = range(4)
_connStateStr = {
    StateInitializing: "initializing",
    StateConnecting:   "connecting",
    StateServing:      "serving",
    StateStopped:      "stopped",
}

class Method(object):
    def __init__(self, f, testf = None, inSchema=None, resSchema=None, errSchema=None, pacts=[], methodOpts={}):
        _populateMethodOpts(methodOpts)
        self.disablePullLog = methodOpts["disablePullLog"]
        self.enableResponseResultLog = methodOpts["enableResponseResultLog"]
        self.enableResponseErrorLog = methodOpts["enableResponseErrorLog"]
        self.inSchema = inSchema
        self.resSchema = resSchema
        self.errSchema = errSchema
        self.pacts = pacts
        self.f = f
        self.testf = testf

def _populateMethodOpts(opts={}):
    if opts == None:
        opts = {}
    if "disablePullLog" not in opts:
        opts["disablePullLog"] = False
    if "enableResponseResultLog" not in opts:
        opts["enableResponseResultLog"] = False
    if "enableResponseErrorLog" not in opts:
        opts["enableResponseErrorLog"] = False

def _populateOpts(opts={}):
    if opts == None:
        opts = {}
    if "pulls" not in opts or opts["pulls"] <= 0:
        opts["pulls"] = 1
    if "pullTimeout" not in opts:
        opts["pullTimeout"] = 3600
    if opts["pullTimeout"] <= 0:
        opts["pullTimeout"] = 0
    if "maxThreads" not in opts or opts["maxThreads"] <= 0:
        opts["maxThreads"] = 1
    if opts["maxThreads"] < opts["pulls"]:
        opts["maxThreads"] = opts["pulls"]
    if "testing" not in opts:
        opts["testing"] = False
    if "preaction" not in opts:
        opts["preaction"] = None
    if "postaction" not in opts:
        opts["postaction"] = None
    return opts

class Service(object):
    def __init__(self, url, path, opts = {}):
        opts = _populateOpts(opts)

        self.name = "service"
        self.description = ""
        self.url = url
        self._showurl = ""
        self._connurl = ""
        self.user = ""
        self.password = ""
        self.path = path
        self.pulls = opts["pulls"]
        self.pullTimeout = opts["pullTimeout"]
        self.maxThreads = opts["maxThreads"]
        self.statsPeriod = 300
        self.gracefulExit = 20
        self.logLevel = InfoLevel
        self.version = "0.0.0"
        self.testing = opts["testing"]
        self.connState = None

        self._nc = None
        self._methods = {}
        self._handler = None
        self._stats = None

        self._cmdQueue = Queue(self.pulls + 1024)
        self._threadsSem = None
        self._taskWorkers = {}
        self._taskWorkersLock = None
        self._statsTicker = None
        self._stopLock = None
        self._stopping = False
        self._addedAsStoppable = False

        self._debugEnabled = False
        self._sharedConn = False
        self._connid = ""

        # only in nsugar-py
        self._preaction = opts["preaction"]
        self._postaction = opts["postaction"]

    def getConn(self):
        return self._nc

    def _setConn(self, conn):
        self._nc = conn
        self._sharedConn = True
        self._connid = conn.connid

    def addMethod(self, name, f, testf=None, schema=None, methodOpts={}):
        if len(self._methods) == 0:
            self._initMethods()
        method = Method(_defMethodWrapper(f), methodOpts=methodOpts)
        if testf != None:
            method.testf = _defMethodWrapper(testf)
        self._methods[name] = method
        if schema != None:
            err, errM = self._addSchemaToMethod(name, schema)
            if err != None:
                self.logWithFields(ErrorLevel, errM, err)
                return err
        return None

    def _initMethods(self):
        self._methods["@schema"] = Method(self._schemaMethod)
        self._methods["@info"] = Method(self._infoMethod)
        self._methods["@ping"] = Method(self._pingMethod)

    def _schemaMethod(self, task):
        r = {}
        for name, m in self._methods.items():
            d = {}
            if m.inSchema != None:
                d["input"] = m.inSchema.json
            if m.resSchema != None:
                d["result"] = m.resSchema.json
            if m.errSchema != None:
                d["error"] = m.errSchema.result
            if len(m.pacts) != 0:
                d["pacts"] = m.pacts
            r[name] = d
        task.sendResult(r)

    def _infoMethod(self, task):
        task.sendResult({
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "nxcli-version": "not implemented",
            "wan-ips": info.wanIps,
            "lan-ips": info.lanIps,
            "user": info.user,
            "directory": info.directory,
            "uptime": int(time.time() - info.started),
            "testing": self.isTesting(),
            "stats": self._logStatsMap(),
        })

    def _pingMethod(self, task):
        task.sendResult("pong")

    def setHandler(self, h, methodOpts={}):
        self._handler = Method(_defMethodWrapper(h), methodOpts=methodOpts)

    def setDescription(self, description):
        self.description = description

    def setUrl(self, url):
        self.url = url

    def setUser(self, user):
        self.user = user

    def setPassword(self, password):
        self.password = password

    def setPath(self, path):
        self.path = path

    def setPulls(self, pulls):
        self.pulls = pulls

    def setMaxThreads(self, maxThreads):
        self.maxThreads = maxThreads

    def setPullTimeout(self, pullTimeout):
        self.pullTimeout = pullTimeout

    def setLogLevel(self, t):
        t = t.lower()
        setLogLevel(t)
        if getLogLevel() == t:
            self.logLevel = t
            self._debugEnabled = (t == DebugLevel)

    def setStatsPeriod(self, t):
        self.statsPeriod = t

    def setGracefulExit(self, t):
        self.gracefulExit = t

    def setVersion(self, major, minor, patch):
        self.version = "{0}.{1}.{2}".format(major, minor, patch)

    def setTesting(self, t):
        if t:
            self.testing = True
        else:
            self.testing = False

    def isTesting(self):
        return self.testing

    def getMethods(self):
        if self._handler != None:
            return []
        return [x for x in self._methods.keys()]

    def gracefulStop(self):
        if self._cmdQueue != None:
            try:
                self._cmdQueue.put_nowait(("graceful", ""))
            except Full:
                log(PanicLevel, "queue", "cmdQueue is full")
                pass

    def stop(self):
        if self._cmdQueue != None:
            try:
                self._cmdQueue.put_nowait(("stop", ""))
            except Full:
                log(PanicLevel, "queue", "cmdQueue is full")
                pass

    def _setState(self, state):
        if self.connState != None:
            self.connState(self.getConn(), state)

    def serve(self, errQueue=None):
        self._setState(StateInitializing)

        self.setLogLevel(self.logLevel)

        # Check service
        if len(self._methods) == 0 and self._handler == None:
            errs = "no methods to serve"
            self.logWithFields(ErrorLevel, {"type": "no_methods"}, errs)
            return errs

        if self.maxThreads < 0:
            self.maxThreads = 1
        if self.pulls < 0:
            self.pulls = 1
        if self.maxThreads < self.pulls:
            self.maxThreads = self.pulls
        if self.pullTimeout < 0:
            self.pullTimeout = 0
        if self.statsPeriod < 0.1:
            if self.statsPeriod < 0:
                self.statsPeriod = 0
            else:
                self.statsPeriod = 0.1
        if self.gracefulExit < 1:
            self.gracefulExit = 1
        if self.version == "":
            self.version = "0.0.0"

        if not self._sharedConn:
            self._setState(StateConnecting)

            # Dial and login
            scheme, user, password, host, port = parseNexusUrl(self.url, self.user, self.password)
            self.user = user
            self.password = password
            self._showurl = "{0}://{1}:{2}".format(scheme, host, port)
            self._connurl = "{0}://{1}:{2}@{3}:{4}".format(scheme, user, password, host, port)

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
            self._connid = self._nc.connid

        self._setState(StateServing)

        # Output
        self.logWithFields(InfoLevel, self._logMap(), str(self))

        # Serve
        self._stats = Stats()
        self._stopping = False
        self._stopLock = threading.Lock()
        if self._threadsSem == None:
            self._threadsSem = threading.Semaphore(self.maxThreads)
        self._taskWorkers = {}
        self._taskWorkersLock = threading.Lock()

        pullWorkers = []
        for i in range(self.pulls):
            worker = threading.Thread(target=self._taskPull, args=(i+1,))
            worker.daemon = True
            pullWorkers.append(worker)
            worker.start()

        if not self._sharedConn and not self._addedAsStoppable:
            addStoppable(self)
            self._addedAsStoppable = True

        gracefulTimeout = None
        if self.statsPeriod > 0:
            self._statsTicker = threading.Timer(self.statsPeriod, self._statsTickerHandler)
            self._statsTicker.daemon = True
            self._statsTicker.start()

        graceful = False
        errs = None
        while True:
            cmd, reason = self._cmdQueue.get(block=True, timeout=None)
            if cmd == "stats_ticker":
                if self._debugEnabled:
                    self.logWithFields(DebugLevel, self._logStatsMap(), self._logStatsMsg())
            elif cmd == "graceful" or cmd == "stop":
                if cmd == "stop":
                    graceful = False
                    self._setStopping()
                    self._nc.close()
                    gracefulTimeout = threading.Timer(1.0, self._gracefulTimeoutHandler)
                    gracefulTimeout.daemon = True
                    gracefulTimeout.start()
                    continue
                graceful = True
                if not self._isStopping():
                    self._setStopping()
                    gracefulTimeout = threading.Timer(self.gracefulExit, self._gracefulTimeoutHandler)
                    gracefulTimeout.daemon = True
                    gracefulTimeout.start()
                    waitWorkers = threading.Thread(target=self._waitWorkers)
                    waitWorkers.daemon = True
                    waitWorkers.start()
            elif cmd == "task_workers_done":
                self._nc.close()
                waitPullers = threading.Thread(target=self._waitPullers, args=(pullWorkers,))
                waitPullers.daemon = True
                waitPullers.start()
                continue
            elif cmd == "graceful_timeout":
                if not graceful:
                    self.logWithFields(DebugLevel, {"type": "stop"}, "stop: done")
                    break
                self._nc.close()
                errs = "graceful: timeout after {0}".format(secondsToStr(self.gracefulExit))
                self.logWithFields(ErrorLevel, {"type": "graceful_timeout"}, errs)
                break
            elif cmd == "connection_ended":
                if self._isStopping():
                    if graceful:
                        self.logWithFields(DebugLevel, {"type": "graceful"}, "graceful: done")
                    else:
                        self.logWithFields(DebugLevel, {"type": "stop"}, "stop: done")
                    break
                errs = "stop: nexus connection ended: {0}".format(reason)
                self.logWithFields(ErrorLevel, {"type": "connection_ended"}, errs)
                break

        if gracefulTimeout != None:
            gracefulTimeout.cancel()
        if self._statsTicker != None:
            self._statsTicker.cancel()
        self._nc = None
        self._setState(StateStopped)

        if errQueue != None:
            try:
                errQueue.put_nowait(errs)
            except Full:
                log(PanicLevel, "queue", "errQueue is full")
                pass

        return errs

    def _taskPull(self, i):
        while True:
            if self._isStopping():
                return
            self._threadsSem.acquire()
            self._stats.addThreadsUsed(1)
            if self._isStopping():
                self._threadsSem.release()
                self._stats.addThreadsUsed(-1)
                return

            # Make a task pull
            self._stats.addTaskPullsDone(1)
            task, err = self._nc.taskPull(self.path, self.pullTimeout)
            if err != None:
                if isNexusErrCode(err, ErrTimeout):
                    self._stats.addTaskPullsTimeouts(1)
                    self._threadsSem.release()
                    self._stats.addThreadsUsed(-1)
                    continue

                if not self._isStopping() or not (isNexusErrCode(err, ErrCancel) or isNexusErrCode(err, ErrConnClosed)):
                    errReason = errToStr(err)
                    self.logWithFields(ErrorLevel, {"type": "pull_error"}, "pull {0}: pulling task: {1}", i, errReason)
                    try:
                        self._cmdQueue.put_nowait(("connection_ended", errReason))
                    except Full:
                        log(PanicLevel, "queue", "cmdQueue is full")
                        pass
                self._nc.close()
                self._threadsSem.release()
                self._stats.addThreadsUsed(-1)
                return

            # A task has been pulled
            self._stats.addTasksPulled(1)
            
            # Get method or global handler
            method = self._handler
            if method == None:
                if task.method not in self._methods:
                    task.sendError(ErrMethodNotFound, "", None)
                    self._stats.addTasksMethodNotFound(1)
                    self._threadsSem.release()
                    self._stats.addThreadsUsed(-1)
                    continue
                method = self._methods[task.method]

            # Log the task
            if not method.disablePullLog:
                self.logWithFields(InfoLevel, {"type": "pull", "path": task.path, "method": task.method, "params": task.params, "tags": task.tags}, "pull {0}: task[ path={1} method={2} params={3} tags={4} ]", i, task.path, task.method, task.params, task.tags)

            # Execute the task
            taskExecute = threading.Thread(target=self._taskExecute, args=(i, method, task))
            taskExecute.daemon = True
            taskExecute.start()

    def _taskExecute(self, n, method, task):
        self._taskWorkersLock.acquire()
        th = threading.current_thread()
        self._taskWorkers[th] = True
        self._taskWorkersLock.release()
        self._stats.addTasksRunning(1)

        try:
            metadata = {}
            if isinstance(task.params, dict) and "@metadata" in task.params:
                metadata = task.params["@metadata"]

            if self._preaction != None:
                try:
                    self._preaction(task)
                except Exception:
                    tbck = traceback.format_exc()
                    self.log(ErrorLevel, {"type": "task_exception"}, "pull {0}: panic serving task on preaction: {1}", n, tbck)

            # Pact: return mock
            if "pact" in metadata and metadata["pact"]:
                pactOk = False
                for pact in method.pacts:
                    if isinstance(pact.input, dict):
                        pact.input["@metadata"] = metadata
                        if pact.input == task.params:
                            pactOk = True
                            task.sendResult(pact.output)
                            break
                if not pactOk:
                    task.SendError(ErrPactNotDefined, ErrStr[ErrPactNotDefined], None)
            else:
                errReturned = False
                # Validate input schema
                if method.inSchema != None:
                    self.log(InfoLevel, "not implemented: we should check the following input schema here: {0}", method.inSchema)
                    # Implement json schema validation!

                # Execute the task
                if not errReturned:
                    if "testing" in metadata and metadata["testing"]:
                        if method.testf == None:
                            task.sendError(ErrTestingMethodNotProvided, ErrStr[ErrTestingMethodNotProvided], None)
                            errReturned = True
                        else:
                            method.testf(task)
                    else:
                        method.f(task)

                # Log response
                if method.enableResponseResultLog and task.tags and "@local-response-result" in task.tags and task.tags["@local-response-result"] != None:
                    self.logWithFields(InfoLevel, {"type": "response_result", "path": task.path, "method": task.method}, "pull {0}: task[ path={1} method={2} result={3} ]", n, task.path, task.method, task.tags["@local-response-result"])
                if method.enableResponseErrorLog and task.tags and "@local-response-error" in task.tags and task.tags["@local-response-error"] != None:
                    self.logWithFields(InfoLevel, {"type": "response_error", "path": task.path, "method": task.method}, "pull {0}: task[ path={1} method={2} error={3} ]", n, task.path, task.method, task.tags["@local-response-error"])

                # Validate result and error schema
                if not errReturned:
                    if method.resSchema != None and "@local-response-result" in task.tags and task.tags["@local-response-result"] != None:
                        self.log(InfoLevel, "not implemented: we should check the following result schema here: {0}", method.resSchema)
                        # Implement jscon schema validation!
                    if method.errSchema != None and "@local-response-error" in task.tags and task.tags["@local-response-error"] != None:
                        self.log(InfoLevel, "not implemented: we should check the following error schema here: {0}", method.errSchema)
                        # Implement jscon schema validation!

            if self._postaction != None:
                try:
                    self._postaction(task)
                except Exception:
                    tbck = traceback.format_exc()
                    self.log(ErrorLevel, {"type": "task_exception"}, "pull {0}: panic serving task on postaction: {1}", n, tbck)

            self._stats.addTasksServed(1)

        except Exception:
            self._stats.addTaskPanic(1)
            tbck = traceback.format_exc()
            self.logWithFields(ErrorLevel, {"type": "task_exception"}, "pull {0}: panic serving task: {1}", n, tbck)
            task.sendError(ErrInternal, tbck, None)

        self._taskWorkersLock.acquire()
        th = threading.current_thread()
        if th in self._taskWorkers:
            self._taskWorkers.pop(th, None)
        self._taskWorkersLock.release()
        self._threadsSem.release()
        self._stats.addThreadsUsed(-1)
        self._stats.addTasksRunning(-1)

    def _waitWorkers(self):
        while True:
            self._taskWorkersLock.acquire()
            wlist = list(self._taskWorkers.keys())
            self._taskWorkersLock.release()
            if len(wlist) == 0:
                break
            for worker in wlist:
                worker.join()
        try:
            self._cmdQueue.put_nowait(("task_workers_done", ""))
        except Full:
            log(PanicLevel, "queue", "cmdQueue is full")
            pass

    def _waitPullers(self, pullers):
        for worker in pullers:
            worker.join()
        try:
            self._cmdQueue.put_nowait(("connection_ended", "closed by service"))
        except Full:
            log(PanicLevel, "queue", "cmdQueue is full")
            pass

    def _gracefulTimeoutHandler(self):
        try:
            self._cmdQueue.put_nowait(("graceful_timeout", ""))
        except Full:
            log(PanicLevel, "queue", "cmdQueue is full")
            pass

    def _statsTickerHandler(self):
        try:
            self._cmdQueue.put_nowait(("stats_ticker", ""))
        except Full:
            log(PanicLevel, "queue", "cmdQueue is full")
            pass
        self._statsTicker = threading.Timer(self.statsPeriod, self._statsTickerHandler)
        self._statsTicker.daemon = True
        self._statsTicker.start()

    def _setStopping(self):
        self._stopLock.acquire()
        self._stopping = True
        self._stopLock.release()

    def _isStopping(self):
        self._stopLock.acquire()
        stopping = self._stopping
        self._stopLock.release()
        return stopping

    def log(self, level, message, *args, **kwargs):
        fields = {}
        if self._connid != "":
            fields["connid"] = self._connid
        logWithFields(level, self.name, fields, message, *args, **kwargs)

    def logWithFields(self, level, fields, message, *args, **kwargs):
        if not isinstance(fields, dict):
            fields = {}
        if self._connid != "":
            fields["connid"] = self._connid
        logWithFields(level, self.name, fields, message, *args, **kwargs)

    def _logMap(self):
        return {
            "type":         "start",
            "url":          self.url,
            "user":         self.user,
            "connid":       self._connid,
            "version":      self.version,
            "path":         self.path,
            "pulls":        self.pulls,
            "pullTimeout":  secondsToStr(self.pullTimeout),
            "maxThreads":   self.maxThreads,
            "logLevel":     self.logLevel,
            "statsPeriod":  secondsToStr(self.statsPeriod),
            "gracefulExit": secondsToStr(self.gracefulExit),
        }


    def __repr__(self):
        tup = (self.url, self.user, self._connid, self.version, self.path, self.pulls, secondsToStr(self.pullTimeout), self.maxThreads, self.logLevel, secondsToStr(self.statsPeriod), secondsToStr(self.gracefulExit))
        return "config: url={0} user={1} connid={2} version={3} path={4} pulls={5} pullTimeout={6} maxThreads={7} logLevel={8} statsPeriod={9} gracefulExit={10}".format(*tup)


    def _logStatsMap(self):
        return {
            "threadsUsed": self._stats.threadsUsed,
            "threadsMax": self.maxThreads,
            "taskPullsDone": self._stats.taskPullsDone,
            "taskPullTimeouts": self._stats.taskPullTimeouts,
            "tasksPulled": self._stats.tasksPulled,
            "tasksPanic": self._stats.tasksPanic,
            "tasksMethodNotFound": self._stats.tasksMethodNotFound,
            "tasksServed": self._stats.tasksServed,
            "tasksRunning": self._stats.tasksRunning,
        }

    def _logStatsMsg(self):
        tup = (self._stats.threadsUsed, self.maxThreads, self._stats.taskPullsDone, self._stats.taskPullTimeouts, self._stats.tasksPulled, self._stats.tasksPanic, self._stats.tasksMethodNotFound, self._stats.tasksServed, self._stats.tasksRunning)
        return "stats: threads[ {0}/{1} ] task_pulls[ done={2} timeouts={3} ] tasks[ pulled={4} panic={5} errmethod={6} served={7} running={8} ]".format(*tup)

def _defMethodWrapper(f):
    def wrapped(task):
        res, err = f(task)
        if res != None:
            task.tags["@local-response-result"] = res
        if err != None:
            task.tags["@local-response-error"] = err
        if "@local-repliedTo" in task.tags:
            return
        if err != None:
            err = formatAsJsonRpcErr(err)
            task.sendError(err["code"], err["message"], err["data"])
        else:
            task.sendResult(res)
    return wrapped

def replyToWrapper(f):
    def wrapped(task):
        if isinstance(task.params, dict) and "replyTo" in task.params and isinstance(task.params["replyTo"], dict):
            replyTo = task.params["replyTo"]
            if "path" in replyTo and isinstance(replyTo["path"], string_types) and "type" in replyTo and isinstance(replyTo["type"], string_types) and replyTo["type"] in ["pipe", "service"]:
                res, errm = f(task)
                task.tags["@local-repliedTo"] = True
                _, err = task.accept()
                if err != None:
                    log(WarnLevel, "replyto wrapper", "could not accept task: {0}", errToStr(err))
                elif replyTo["type"] == "pipe":
                    pipe, err = task.nexusConn.pipeOpen(replyTo["path"])
                    if err != None:
                        log(WarnLevel, "replyto wrapper", "could not open received pipeId ({0}): {1}", replyTo["path"], errToStr(err))
                    else:
                        _, err = pipe.write({"result": res, "error": errm, "task": {"path": task.path, "method": task.method, "params": task.params, "tags": task.tags}})
                        if err != None:
                            log(WarnLevel, "replyto wrapper", "error writing response to pipe: {0}", errToStr(err))
                elif replyTo["type"] == "service":
                    _, err = task.nexusConn.taskPush(replyTo["path"], {"result": res, "error": errm, "task": {"path": task.path, "method": task.method, "params": task.params, "tags": task.tags}}, timeout=30, detach=True)
                    if err != None:
                        log(WarnLevel, "replyto wrapper", "could not push response task to received path ({0}): {1}", replyTo["path"], errToStr(err))
                return res, errm
            return f(task)
        return f(task)
    return wrapped

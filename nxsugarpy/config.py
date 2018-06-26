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

from .server import *
from .service import *
from .log import *
import json
import getopt
import sys
from six import string_types

_configServer = {
    "url": "",
    "user": "",
    "pass": "",
    "log-level": InfoLevel,
    "graceful-exit": 20,
    "testing": False,
    "pulls": 1,
    "pull-timeout": 3600,
    "max-threads": 4,
    "stats-period": 1800,
    "version": "",
    "services": {},
}

productionMode = False
configFile = "config.json"
MissingConfigErr = "missing parameter ({0}) on config file"
InvalidConfigErr = "invalid parameter ({0}) on config file: {1}"

_logLevels = [DebugLevel, InfoLevel, WarnLevel, ErrorLevel, FatalLevel, PanicLevel]
_configParsed = False
_config = {}

def usage():
    print('''Usage of {0}:\n  -c, --config string\n  \tJSON configuration file (default "config.json")\n  --production\n  \tEnables Production mode'''.format(sys.argv[0]))

try:
    opts, args = getopt.getopt(sys.argv[1:], "hc:", ["help", "config=", "production"])
except getopt.GetoptError as e:
    print(str(e))
    usage()
    sys.exit(2)
for o, a in opts:
    if o in ("-h", "--help"):
        usage()
        sys.exit()
    elif o == "--production":
        productionMode = True
    elif o in ("-c", "--config"):
        configFile = a

setJSONOutput(productionMode)

def _parseConfig():
    global _configParsed, _config, _configServer
    if not _configParsed:
        try:
            with open(configFile, "r") as file:
                try:
                    d = json.load(file)
                except Exception as e:
                    return "can't parse config file ({0}): {1}".format(configFile, str(e)), {"type": "config_file"}
                if not isinstance(d, dict):
                    return "can't parse config file ({0}): JSON must be map".format(configFile), {"type": "config_file"}

                if "server" not in d:
                    return MissingConfigErr.format("server"), {"type": "missing_param"}
                if not isinstance(d["server"], dict):
                    return InvalidConfigErr.format("server", "must be map"), {"type": "invalid_param"}
                server = d["server"]

                if "url" not in server:
                    return MissingConfigErr.format("server.url"), {"type": "missing_param"}
                if not isinstance(server["url"], string_types):
                    return InvalidConfigErr.format("server.url", "must be string"), {"type": "invalid_param"}
                _configServer["url"] = server["url"]
                if "user" not in server:
                    return MissingConfigErr.format("server.user"), {"type": "missing_param"}
                if not isinstance(server["user"], string_types):
                    return InvalidConfigErr.format("server.user", "must be string"), {"type": "invalid_param"}
                _configServer["user"] = server["user"]
                if "pass" not in server:
                    return MissingConfigErr.format("server.pass"), {"type": "missing_param"}
                if not isinstance(server["pass"], string_types):
                    return InvalidConfigErr.format("server.pass", "must be string"), {"type": "invalid_param"}
                _configServer["pass"] = server["pass"]
                if "log-level" in server:
                    if not isinstance(server["log-level"], string_types):
                        return InvalidConfigErr.format("server.log-level", "must be string"), {"type": "invalid_param"}
                    if server["log-level"] not in _logLevels:
                        return InvalidConfigErr.format("server.log-level", "must be one of [debug info warn error fatal panic]"), {"type": "invalid_param"}
                    _configServer["log-level"] = server["log-level"]
                if "graceful-exit" in server:
                    try:
                        ge = float(server["graceful-exit"])
                        if ge < 0:
                            return InvalidConfigErr.format("server.graceful-exit", "must be positive"), {"type": "invalid_param"}
                        _configServer["graceful-exit"] = ge
                    except:
                        return InvalidConfigErr.format("server.graceful-exit", "must be float"), {"type": "invalid_param"}
                if "testing" in server:
                    try:
                        _configServer["testing"] = bool(server["testing"])
                    except:
                        return InvalidConfigErr.format("server.testing", "must be bool"), {"type": "invalid_param"}
                if "pulls" in server:
                    try:
                        pulls = int(server["pulls"])
                        if pulls < 1:
                            return InvalidConfigErr.format("server.pulls", "must be positive"), {"type": "invalid_param"}
                        _configServer["pulls"]  = pulls
                    except:
                        return InvalidConfigErr.format("server.pulls", "must be int"), {"type": "invalid_param"}
                if "pull-timeout" in server:
                    try:
                        pt = float(server["pull-timeout"])
                        if pt < 0:
                            return InvalidConfigErr.format("server.pull-timeout", "must be positive or 0"), {"type": "invalid_param"}
                        _configServer["pull-timeout"] = pt
                    except:
                        return InvalidConfigErr.format("server.pull-timeout", "must be float"), {"type": "invalid_param"}
                if "max-threads" in server:
                    try:
                        mt = int(server["max-threads"])
                        if mt < 1:
                            return InvalidConfigErr.format("server.max-threads", "must be positive"), {"type": "invalid_param"}
                        _configServer["max-threads"] = mt
                    except:
                        return InvalidConfigErr.format("server.max-threads", "must be int"), {"type": "invalid_param"}
                if "stats-period" in server:
                    try:
                        sp = float(server["stats-period"])
                        if sp < 0:
                            return InvalidConfigErr.format("server.stats-period", "must be positive"), {"type": "invalid_param"}
                        _configServer["stats-period"] = sp
                    except:
                        return InvalidConfigErr.format("server.stats-period", "must be float"), {"type": "invalid_param"}
                if "version" in server:
                    try:
                        _configServer["version"] = str(server["version"])
                    except:
                        return InvalidConfigErr.format("server.version", "must be string"), {"type": "invalid_param"}

                if "services" not in d:
                    return MissingConfigErr.format("services"), {"type": "missing_param"}
                if not isinstance(d["services"], dict):
                    return InvalidConfigErr.format("services", "must be map"), {"type": "invalid_param"}
                services = d["services"]

                for name, opts in services.items():
                    sc = {
                        "description": "",
                        "path": "",
                        "pulls": _configServer["pulls"],
                        "pull-timeout": _configServer["pull-timeout"],
                        "max-threads": _configServer["max-threads"],
                        "version": _configServer["version"],
                    }

                    if not isinstance(opts, dict):
                        return InvalidConfigErr.format("services." + name, "must be map"), {"type": "invalid_param"}

                    if "description" in opts:
                        try:
                            sc["description"] = str(opts["description"])
                        except:
                            return InvalidConfigErr.format("services." + name + ".description", "must be string"), {"type": "invalid_param"}
                    if "path" not in opts:
                        return MissingConfigErr.format("services." + name + ".path"), {"type": "missing_param"}
                    try:
                        pt = str(opts["path"])
                        if pt == "":
                            return InvalidConfigErr.format("services." + name + ".path", "must not be empty"), {"type": "invalid_param"}
                        sc["path"] = pt
                    except:
                        return InvalidConfigErr.format("services." + name + ".path", "must be string"), {"type": "invalid_param"}
                    if "pulls" in opts:
                        try:
                            pulls = int(opts["pulls"])
                            if pulls < 1:
                                return InvalidConfigErr.format("services." + name + ".pulls", "must be positive"), {"type": "invalid_param"}
                            sc["pulls"] = pulls
                        except:
                            return InvalidConfigErr.format("services." + name + ".pulls", "must be int"), {"type": "invalid_param"}
                    if "pull-timeout" in opts:
                        try:
                            pt = float(opts["pull-timeout"])
                            if pt < 0:
                                return InvalidConfigErr.format("services." + name + ".pull-timeout", "must be positive or 0"), {"type": "invalid_param"}
                            sc["pull-timeout"] = pt
                        except:
                            return InvalidConfigErr.format("services." + name + ".pull-timeout", "must be float"), {"type": "invalid_param"}
                    if "max-threads" in opts:
                        try:
                            mt = int(opts["max-threads"])
                            if mt < 1:
                                return InvalidConfigErr.format("services." + name + ".max-threads", "must be positive"), {"type": "invalid_param"}
                            sc["max-threads"] = mt
                        except:
                            return InvalidConfigErr.format("services." + name + ".max-threads", "must be int"), {"type": "invalid_param"}
                    if "version" in opts:
                        try:
                            sc["version"] = str(opts["version"])
                        except:
                            return InvalidConfigErr.format("services." + name + ".version", "must be string"), {"type": "invalid_param"}

                    _configServer["services"][name] = sc

                _config = d
                _configParsed = True

        except Exception as e:
            return "can't open config file ({0}): {1}".format(configFile, str(e)), {"type": "config_file"}

    return None, None

def newServerFromConfig():
    try:
        s = ServerFromConfig()
        return s, None
    except Exception as e:
        return None, str(e)

class ServerFromConfig(Server):
    def __init__(self):
        global _configServer
        err, errM = _parseConfig()
        if err != None:
            logWithFields(ErrorLevel, "config", errM, err)
            raise Exception(err)
        super(ServerFromConfig, self).__init__(_configServer["url"])
        self.user = _configServer["user"]
        self.password = _configServer["pass"]
        self.pulls = _configServer["pulls"]
        self.pullTimeout = _configServer["pull-timeout"]
        self.maxThreads = _configServer["max-threads"]
        self.logLevel = _configServer["log-level"]
        self.statsPeriod = _configServer["stats-period"]
        self.gracefulExit = _configServer["graceful-exit"]
        self.testing = _configServer["testing"]
        self.version = _configServer["version"]

    def addService(self, name):
        global _configServer
        if name not in _configServer["services"]:
            err = MissingConfigErr.format("services."+name)
            logWithFields(ErrorLevel, "config", {"type": "missing_param"}, err)
            return None, err

        svc = _configServer["services"][name]
        s = Service(self.url, svc["path"])

        s.user = self.user
        s.password = self.password
        s.statsPeriod = self.statsPeriod
        s.gracefulExit = self.gracefulExit
        s.logLevel = self.logLevel
        s.testing = self.testing
        s.name = name
        s.description = svc["description"]
        s.pulls = svc["pulls"]
        s.pullTimeout = svc["pull-timeout"]
        s.maxThreads = svc["max-threads"]
        s.version = svc["version"]

        self._services[name] = s
        return s, None

def newServiceFromConfig(name):
    try:
        s = ServiceFromConfig(name)
        return s, None
    except Exception as er:
        return None, str(er)

class ServiceFromConfig(Service):
    def __init__(self, name):
        global _configServer
        err, errM = _parseConfig()
        if err != None:
            logWithFields(ErrorLevel, "config", errM, err)
            raise Exception(err)

        if name not in _configServer["services"]:
            err = MissingConfigErr.format("services."+name)
            logWithFields(ErrorLevel, "config", {"type": "missing_param"}, err)
            raise Exception(err)

        svc = _configServer["services"][name]
        super(ServiceFromConfig, self).__init__(_configServer["url"], svc["path"])

        self.user = _configServer["user"]
        self.password = _configServer["pass"]
        self.statsPeriod = _configServer["stats-period"]
        self.gracefulExit = _configServer["graceful-exit"]
        self.logLevel = _configServer["log-level"]
        self.testing = _configServer["testing"]
        self.name = name
        self.description = svc["description"]
        self.pulls = svc["pulls"]
        self.pullTimeout = svc["pull-timeout"]
        self.maxThreads = svc["max-threads"]
        self.version = svc["version"]

def getConfig():
    err, errM = _parseConfig()
    if err != None:
        logWithFields(ErrorLevel, "config", errM, err)
        return None, err
    return _config, None

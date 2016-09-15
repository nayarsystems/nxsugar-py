# -*- coding: utf-8 -*-
##############################################################################
#
#    nxsugarpy, build microservices over Nexus
#    Copyright (C) 2016 by the pynexus team
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

import sys
sys.path.insert(0, '..')
import pynexus as nxpy
import unittest
from unittest import TextTestRunner
import os
import sys

from service import Service, Server

def test(task):
    return task.params, None

class TestServer(unittest.TestCase):
    def test_1(self):
        self.assertEqual(client.taskPush("test.python.sugar1.test", {"test": "hola"})[0],
                                                                    {"test": "hola"})

    def test_2(self):
        self.assertEqual(client.taskPush("test.python.sugar2.test", {"test": "hola"})[0],
                                                                    {"test": "hola"})

    def test_3(self):
        self.assertEqual(client.taskPush("test.python.sugar3.test", {"test": "hola"})[0],
                                                                    {"test": "hola"})

class TestService(unittest.TestCase):
    def test_1(self):
        self.assertEqual(client.taskPush("test.python.sugar.test1", {"test": "hola"})[0],
                                                                    {"test": "hola"})
    def test_2(self):
        self.assertEqual(client.taskPush("test.python.sugar.test2", {"test": "hola"})[1],
                {u'message': u'Method not found', u'code': -32601})

    def test_3(self):
        pipe, _ = client.nexusConn.pipeCreate()
        params = {"test": "hola", "replyTo": {"type": "pipe", "path": pipe.id()}}
        client.taskPush("test.python.sugar.test3", params, detach=True)
        res, err = pipe.read(1, 10)
        pipe.close()
        del(res.msgs[0].msg["task"])
        self.assertEqual(res.msgs[0].msg, {'error': None, 'result': params})

    def test_4(self):
        params = {"test": "hola", "replyTo": {"type": "service", "path": "test.python.ssugar.testreplyservice"}}
        client.taskPush("test.python.sugar.test4", params, detach=True)
        task, err = client.taskPull("test.python.ssugar", 2)
        task.accept()
        del(task.params["task"])
        self.assertEqual(task.params, {'error': None, 'result': params})


if __name__ == "__main__":
    client = nxpy.Client("tcp://root:root@%s:%s" % (os.environ.get("NEXUS_HOST", "localhost"),
                                                    os.environ.get("NEXUS_TCP_PORT", "1717")))

    # Standalone services
    service = Service("tcp://root:root@%s:%s" % (os.environ.get("NEXUS_HOST", "localhost"),
                                                os.environ.get("NEXUS_TCP_PORT", "1717")),
        "test.python.sugar", {"testing": True})
    service.add_method("test1", test)
    service.add_method("test3", test)
    service.add_method("test4", test)
    service.start()

    # Server with services sharing one connection
    server = Server("tcp://root:root@%s:%s" %  (os.environ.get("NEXUS_HOST", "localhost"),
                                                os.environ.get("NEXUS_TCP_PORT", "1717")))

    service1 = server.add_service("test.python.sugar1", {"testing": True})
    service1.add_method("test", test)

    service2 = server.add_service("test.python.sugar2", {"testing": True})
    service2.add_method("test", test)

    service3 = server.add_service("test.python.sugar3", {"testing": True})
    service3.add_method("test", test)

    server.start()

    # Run tests and stop everything
    test_suite_service = unittest.TestLoader().loadTestsFromTestCase(TestService)
    test_result_service = TextTestRunner().run(test_suite_service)
    test_suite_server = unittest.TestLoader().loadTestsFromTestCase(TestServer)
    test_result_server = TextTestRunner().run(test_suite_server)
    service.stop()
    server.stop()
    client.cancel()
    if not test_result_service.wasSuccessful() or not test_result_server.wasSuccessful():
        sys.exit(1)
#!/usr/bin/env python

# Copyright (C) 2012, Valentin Lorentz
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import logging
import unittest

import psycopg2
import uddlib

from config import HOST, PORT, USER, PASSWORD, DATABASE

logging.basicConfig(logLevel=logging.DEBUG)

connection = psycopg2.connect(host=HOST, port=PORT, user=USER,
    database=DATABASE, password=PASSWORD)
uddlib.connection = connection

class UddlibTestCase(unittest.TestCase):
    def testBug(self):
        bug = uddlib.ArchivedBug.fetch_database(pk=100000)
        self.assertEqual(bug.title, 'xlibs: [xkb] Another alt keys not -> '
                                    'meta keysyms report')
        self.assertIn(77039, [x.id for x in bug.merged_with])

        bug = uddlib.ActiveBug.fetch_database(pk=24043)
        self.assertIn(638791, [x.id for x in bug.blocks])
        bug2 = uddlib.ActiveBug.fetch_database(pk=638791)
        self.assertIn(bug2, bug.blocks)
        self.assertIn(bug, bug2.blockedby)

    def testDevelopper(self):
        dev = uddlib.Developper.fetch_database(pk=1026)
        self.assertEqual(dev.login, 'jamessan')
        self.assertIn('James McCoy', dev.names)
        self.assertIn('James Vega', dev.names)
        self.assertGreater(len(dev.emails), 2)

        devs = uddlib.Developper.fetch_database(login='jamessan')
        self.assertEqual(len(devs), 1)
        self.assertEqual(dev, devs[0])

    def testPackage(self):
        pkg = uddlib.Package.fetch_database(package='python2.7')
        self.failUnless(all([x.package=='python2.7' for x in pkg.subpackages]))
        self.failUnless(any([x.architecture=='i386' for x in pkg.subpackages]))
        self.failUnless(any([x.architecture=='ia64' for x in pkg.subpackages]))

if __name__ == '__main__':
    unittest.main()

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

import os
import sys
import json
import datetime
import urlparse
import psycopg2

sys.path.append(os.path.dirname(__file__))

import uddlib
from config import HOST, PORT, USER, PASSWORD, DATABASE


connection = psycopg2.connect(host=HOST, port=PORT, user=USER,
    database=DATABASE, password=PASSWORD)
uddlib.connection = connection

def dthandler(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, uddlib.UddResource):
        return dict(zip(obj.pk, obj._parameter))
        # Not obj.data, because computed_fields would result in circular
        # computation.
    else:
        return None
        # Perform standart serialization

def serialize(data):
    return json.dumps(data, sort_keys=True, indent=4, default=dthandler)

def get_subclasses(cls):
    subclasses = []
    subclasses = cls.__subclasses__()
    for subclass in cls.__subclasses__():
        subclasses.extend(get_subclasses(subclass))
    return subclasses

def application(environ, start_response):
    url = [x for x in environ['PATH_INFO'].split('/') if x != '']

    if len(url) == 0: # Return list of resources
        resources = dict([
            (x.__name__, {'path': x._path, 'doc': (x.__doc__ or '').strip(),})
            for x in get_subclasses(uddlib.UddResource)])
        data = {'info': 'This is a JSON API for the Ultimate Debian Database.',
                'doc': 'https://github.com/ProgVal/udd-http-api/blob/master/README',
                'resources': resources}
        start_response('200 OK', [('Content-type', 'application/json')])
        return [serialize(data)]

    
    try:
        cls = uddlib.UddResource.resolve_path(url[0])
    except uddlib.ResourceNotFound:
        start_response('404 Not Found', [('Content-type', 'text/plain')])
        return ['Resource not found.']

    if len(url) == 1: # Return a list of objects
        filters = urlparse.parse_qs(environ['QUERY_STRING'])
        assert all([(x in cls._fields) for x in filters])

        # We want {'key': 'value'} and not {'key': ['value']}
        filters = dict([(x,y[0]) for x,y in filters.items()])

        obj = cls.fetch_database(**filters)
        start_response('200 OK', [('Content-type', 'application/json')])
        if isinstance(obj, list):
            return [serialize([x.data for x in obj])]
        else:
            return [serialize(obj.data)]
    elif len(url) == 2 and url[1] == 'doc':
        start_response('200 OK', [('Content-type', 'application/json')])
        doc = {'computed fields': dict([(x, getattr(cls, x).__doc__)
                                   for x in (cls._computed_fields)]),
               'fields from database': cls._fields}
        return [serialize(doc)]

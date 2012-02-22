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
import psycopg2

connection = None

class UddException(Exception):
    """Base exception for everything related to the UDD.
    """
    pass

class RessourceNotFound(UddException):
    """Exception raised when resolving to a resource has no result.
    May happen when resolve_path is called with an invalid path.
    """
    pass

class ObjectNotFound(UddException):
    """Exception raised when requesting a resource with a primary key
    which does not exist.
    """
    pass

class UddResource(object):
    """Base class representing an entry in the database.
    """
    def __init__(self, **kwargs):
        assert self._path is not None
        assert self._table is not None
        assert self._fields is not None
        self._data = kwargs

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        elif name in self._fields:
            return self._data[name]
        else:
            raise AttributeError
        

    # This attributes should be read-only, and set by all subclasses.
    _path = None # string
    _table = None # string
    _fields = None # list

    @property
    def path(self):
        """The path to this resource.
        """
        # We use a property to make this read-only.
        return self._path

    @property
    def table(self):
        """The table containing this resource.
        """
        # We use a property to make this read-only.
        return self._table

    @staticmethod
    def cursor():
        """Return a cursor for the database connection.
        
        :return: the cursor.
        """
        global connection
        assert connection is not None
        return connection.cursor()

    @classmethod
    def resolve_path(cls, path):
        """Return the class associated with the given path.
        
        :param path: The path to the resource
        :return: an UddResource subclass, serving this path.
        :raises ResourceNotFound: if the path cannot be resolved.
        """
        for subclass in cls.__subclasses__:
            if subclass.path == path:
                return subclass
        raise ResourceNotFound()
    
    @classmethod
    def fetch_database(cls, pk=None, fields=None, **kwargs):
        """Returns all objects of this resource matching the criterions.

        :param pk: The primary key. If this parameter is given, an instance of
                   this class will be returned.
                   Otherwise, it will be a list of instances of this class.
        :param list fields: A list of fields that will be fetched.
                            It defaults to all fields.
        :param dict **kwargs: Only valid if `pk` is not given.
                            Only objects matching this conditions (field given
                            as key must have the given value) will be
                            returned.
        :returns: either an instance (if `pk` is given) or a list of instances
                  (if `pk` is not given).
        :raises ObjectNotFound: if pk is given and no objects have this
                                primary key.
        """
        assert cls.table is not None
        fields = fields or '*'
        query = 'SELECT %(fields)s FROM %(table)s %(where)s'
        def data2object(data):
            assert len(cls._fields) == len(data)
            kwargs = dict(zip(cls._fields, data))
            return cls(**kwargs)

        if pk is None:
            # TODO: rewrite this to make it less dependant on the order of
            # items returned by .keys() and .values().
            where_clause = ' AND '.join([key+'=%s' for key in kwargs.keys()])
            where_params = kwargs.values()
            if where_clause != '':
                where_clause = 'WHERE ' + where_clause

            query %= {
                    'fields': fields,
                    'table': cls.table,
                    'where': where_clause,
                    }
            logging.debug('query: ' + query)

            objects = []
            cur = cls.cursor() # __exit__ not implemented in psycopg2
            try:
                cur.execute(query, where_params)

                for data in cur.fetchone():
                    assert len(cls.fields) == len(data)
                    kwargs = dict(zip(cls.fields, data))
                    objects.append(data2object(data))
            finally:
                cur.close()

            return objects
        else:
            query %= {
                    'fields': fields,
                    'table': cls._table,
                    'where': 'WHERE ' + cls._fields[0] + ' = %s',
                    }
            logging.debug('query: ' + query)
            
            cur = cls.cursor() # __exit__ not implemented in psycopg2
            cur.execute(query, [pk])
            try:
                data = cur.fetchone()
                if data is None:
                    raise ObjectNotFound()
                else:
                    return data2object(data)
            except psycopg2.ProgrammingError:
                raise ObjectNotFound()
            finally:
                cur.close()


class Popcon(UddResource):
    _path = 'popcon'
    _table = 'popcon'
    _fields = ['package', 'insts', 'vote', 'olde', 'recent', 'nofiles']

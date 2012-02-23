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

"""Object wrapper of the Ultimate Debian Database."""

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

class CorruptedDatabase(UddException):
    """Exception raised when the database is not consistant (wrong IDs in
    many-to-many relations, bad type, no primary key, ...)
    """
    pass

def data2object(cls, data):
    """Shortcut for creating an object from database data.

    :param cls: The class of the object
    :param data: The data from the database
    :returns: The created object
    """
    assert len(cls._fields) == len(data)
    kwargs = dict(zip(cls._fields, data))
    return cls(**kwargs)

class UddResource(object):
    """Base class representing an entry in the database.
    """

    _singleton = True
    __instances = {}
    def __new__(cls, **kwargs):
        pk = kwargs[cls._fields[0]]
        if not cls._singleton:
            instance = object.__new__(cls)
            instance._parameter = pk
            return instance
        if cls not in cls.__instances:
            cls.__instances[cls] = {}
        if pk not in cls.__instances[cls]:
            instance = object.__new__(cls)
            instance._parameter = pk
            cls.__instances[cls][pk] = instance
        return cls.__instances[cls][pk]

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

        if pk is None:
            # TODO: rewrite this to make it less dependant on the order of
            # items returned by .keys() and .values().
            where_clause = ' AND '.join([key+'=%s' for key in kwargs.keys()])
            where_params = kwargs.values()
            if where_clause != '':
                where_clause = 'WHERE ' + where_clause

            query %= {
                    'fields': fields,
                    'table': cls._table,
                    'where': where_clause,
                    }
            logging.debug('query: ' + query)

            objects = []
            cur = cls.cursor() # __exit__ not implemented in psycopg2
            try:
                cur.execute(query, where_params)

                while True:
                    data = cur.fetchone()
                    if data is None:
                        break
                    objects.append(data2object(cls, data))
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
            try:
                cur.execute(query, [pk])
                data = cur.fetchone()
                if data is None:
                    raise ObjectNotFound()
                else:
                    return data2object(cls, data)
            finally:
                cur.close()

    def _fetch_linked(self, relation_name, field, classes=None,
            base_table_name=None):
        """Fetch a linked object.

        :param relation_name: The identifier of the relation. For example, for
                              table `bugs_fixed_in`, `fixed_in` is the
                              name of the relation
        :type relation_name: string
        :param field: The name of the field we want to retrieve. If it is a
                      string, a single object will be returned. If it is
                      a tuple, a tuple will be returned.
        :type field:  string or list of strings
        :param classes: The class or the list of classes representing the
                        resource we want to fetch. If it is a list, they
                        will be tryed in the given order, and the first that
                        can be used will be used (useful for search both
                        in `bugs` and `archived_bugs`.
        :type classes:  class or list of classes or None
        :param base_table_name: The base name of the tables involved in the
                                relation. For example, for table
                                `bugs` it is `bugs`, and for
                                `carnivor_login` it is `carnivor`.

                                It defaults to the table represented by this
                                class.
        :type base_table_name:  string or None
        """
        if base_table_name is None:
            base_table_name = self._table
        multiple_fields = isinstance(field, list) or isinstance(field, tuple)
        if multiple_fields:
            field = ', '.join(field)
        objects = []
        query = 'SELECT %s FROM %s_%s WHERE id=%%s;' % \
                (field, base_table_name, relation_name)
        cur = self.cursor()
        try:
            cur.execute(query, [self.id])
            while True:
                data = cur.fetchone()
                if data is None:
                    break
                obj = None
                if classes is None: # Native data
                    if multiple_fields:
                        obj = data
                    else:
                        obj = data[0]
                else: # many-to-many relation
                    for cls in classes:
                        try:
                            if multiple_fields:
                                obj = [cls.fetch_database(pk=x) for x in data]
                            else:
                                obj = cls.fetch_database(pk=data[0])
                        except ObjectNotFound as e:
                            pass
                if obj is None:
                    raise CorruptedDatabase(('`%(obj)r` has relationship '
                            '`%(rel)s` with inexisting element: `%(pk)s`') % {
                                'obj': self,
                                'rel': relation_name,
                                'pk': data[0],
                                }
                            )
                objects.append(obj)
        finally:
            cur.close()
        return objects

class AbstractBug(UddResource):
    """Base class for active and inactive bugs.
    """
    _fields = ['id', 'package', 'source', 'arrival', 'status', 'severity',
    'submitter', 'owner', 'done', 'title', 'last_modified', 'forwarded',
    'affects_stable', 'affects_testing', 'affects_unstable',
    'affects_experimental', 'submitter_name', 'submitter_email', 'owner_name',
    'owner_email', 'done_name', 'done_email', 'affects_oldstable',
    'done_date']

    _blocks = None
    @property
    def blocks(self):
        """Bugs this bug blocks."""
        if self._blocks is None:
            self._blocks = self._fetch_linked('blocks', 'blocked',
                    [ActiveBug, ArchivedBug])
        return self._blocks

    _blockedby = None
    @property
    def blockedby(self):
        """Bugs this bug blocks."""
        if self._blockedby is None:
            self._blockedby = self._fetch_linked('blockedby', 'blocker',
                    [ActiveBug, ArchivedBug])
        return self._blockedby

    _merged_with = None
    @property
    def merged_with(self):
        """Bug that has been merge with this one."""
        if self._merged_with is None:
            self._merged_with = self._fetch_linked('merged_with',
                    'merged_with', [ActiveBug, ArchivedBug])
        return self._merged_with

    _fixed_in = None
    @property
    def fixed_in(self):
        """The version this bug has been fixed in."""
        if self._fixed_in is None:
            self._fixed_in = self._fetch_linked('fixed_in', 'version')
        return self._fixed_in

    _found_in = None
    @property
    def found_in(self):
        """The version this bug has been found in."""
        if self._found_in is None:
            self._found_in = self._fetch_linked('found_in', 'version')
        return self._found_in

    _tags = None
    @property
    def tags(self):
        """The version this bug has been found in."""
        if self._tags is None:
            self._tags = self._fetch_linked('tags', 'version')
        return self._tags

    _packages = None
    @property
    def packages(self):
        """The version this bug has been found in.
        This property is a list of tuples (binary, source), where the source
        package may be None."""
        if self._packages is None:
            self._packages = self._fetch_linked('packages',
                    ('package', 'source'))
        return self._packages

class ActiveBug(AbstractBug):
    """An active bug. See also :class:`uddlib.ArchivedBug`.
    """
    _path = 'bugs'
    _table = 'bugs'

class ArchivedBug(AbstractBug):
    """An archived bug. See also :class:`uddlib.ActiveBug`.
    """
    _path = 'archived_bugs'
    _table = 'archived_bugs'


class Developper(UddResource):
    """A Debian Developper, from the Carnivore database.
    """
    _path = 'developpers'
    _table = 'carnivore_login'
    _fields = ['id', 'login']

    _emails = None
    @property
    def emails(self):
        """The email addresses of this developper."""
        if self._emails is None:
            self._emails = self._fetch_linked('emails', 'email',
                    base_table_name='carnivore')
        return self._emails

    _keys = None
    @property
    def keys(self):
        """The key of this developper.
        A list of tuples (key, key_type)."""
        if self._keys is None:
            self._keys = self._fetch_linked('keys', ('key', 'key_type'),
                    base_table_name='carnivore')
        return self._keys

    _names = None
    @property
    def names(self):
        """The names of this developper."""
        if self._names is None:
            self._names = self._fetch_linked('names', 'name',
                    base_table_name='carnivore')
        return self._names


class Popcon(UddResource):
    """Popularity contest."""
    _path = 'popcon'
    _table = 'popcon'
    _fields = ['package', 'insts', 'vote', 'olde', 'recent', 'nofiles']

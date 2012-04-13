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

class ResourceNotFound(UddException):
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

def data2object(cls, data, table=None):
    """Shortcut for creating an object from database data.

    :param cls: The class of the object
    :param data: The data from the database
    :returns: The created object
    """
    assert len(cls._fields) == len(data)
    kwargs = dict(zip(cls._fields, data))
    return cls(_table=table, **kwargs)

class UddResource(object):
    """Base class representing an entry in the database.
    """

    _singleton = True
    __instances = {}
    def __new__(cls, **kwargs):
        # This implements the Parametric Singleton design pattern.
        pk = cls._pk or (cls._fields[0],)
        id = tuple(kwargs[x] for x in pk)
        if not cls._singleton:
            instance = object.__new__(cls)
            instance._parameter = id
            return instance
        if cls not in cls.__instances:
            cls.__instances[cls] = {}
        if id not in cls.__instances[cls]:
            instance = object.__new__(cls)
            instance._parameter = id
            cls.__instances[cls][id] = instance
        return cls.__instances[cls][id]

    def __init__(self, _table=None, **kwargs):
        assert self._path is not None
        assert self._table is not None
        assert self._fields is not None
        self._data = kwargs
        if _table is not None:
            self._table = _table

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        elif name in self._fields:
            return self._data[name]
        else:
            raise AttributeError(name)

    def __repr__(self):
        return '%s.%s(%s)' % (self.__class__.__module__,
                self.__class__.__name__,
                ', '.join(['%s=%r'%x for x in zip(self.pk, self._parameter)]))

    def __eq__(self, other):
        return self._data == other._data
        

    # This attributes should be read-only, and set by all subclasses.
    _pk = None # tuple or None
    _path = None # string
    _table = None # string
    _fields = None # tuple
    _computed_fields = tuple()

    @property
    def pk(self):
        """The primary key of this resource.
        """
        return self._pk or (self._fields[0],)

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

    @property
    def data(self):
        """The data of this object.
        """
        return dict(self._data.items() + [(x, getattr(self, x))
            for x in self._computed_fields])

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
        for subclass in cls.__subclasses__():
            if subclass._path == path:
                return subclass
        raise ResourceNotFound()
    
    @classmethod
    def fetch_database(cls, pk=None, fields=None, _table=None, **kwargs):
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
        table = _table or cls._table
        if isinstance(table, tuple):
            if pk is None:
                results = []
                for table in table:
                    try:
                        results.extend(cls.fetch_database(pk, fields, table,
                            **kwargs))
                    except ObjectNotFound:
                        pass
            else:
                for table in table:
                    try:
                        return cls.fetch_database(pk, fields, table,
                            **kwargs)
                    except ObjectNotFound:
                        pass
            return results
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
                    'table': table,
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
                    objects.append(data2object(cls, data, (table,)))
            finally:
                cur.close()

            return objects
        else:
            query %= {
                    'fields': fields,
                    'table': table,
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
                    return data2object(cls, data, (table,))
            finally:
                cur.close()

    def _fetch_linked(self, relation_name, field, classes=None,
            base_table_name=None, exclude_from_pk=tuple()):
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
                                `bugs` it is `bugs_`, and for
                                `carnivor_login` it is `carnivor_`.

                                It defaults to the table represented by this
                                class.
        :type base_table_name:  string or None
        :param exclude_from_pk: fields that will not be used as primary key.
        :type exclude_from_pl: tuple
        """
        if base_table_name is None:
            base_table_name = tuple([x + '_' for x in self._table])
        if isinstance(base_table_name, tuple):
            results = []
            for table in base_table_name:
                results.extend(self._fetch_linked(relation_name, field,
                        classes, table))
            return results
        multiple_fields = isinstance(field, tuple)
        if multiple_fields:
            field = ', '.join(field)
        objects = []
        query = 'SELECT %s FROM %s%s WHERE %s;' % \
                (field, base_table_name, relation_name,
                        ' AND '.join([(x + '=%s') for x in self._pk
                            if x not in exclude_from_pk]))
        cur = self.cursor()
        try:
            parameter = [y for x,y in zip(self._pk, self._parameter)
                    if x not in exclude_from_pk]
            cur.execute(query, parameter)
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

class Bug(UddResource):
    """Base class for active and inactive bugs.
    """
    _pk = ('id',)
    _fields = ('id', 'package', 'source', 'arrival', 'status', 'severity',
    'submitter', 'owner', 'done', 'title', 'last_modified', 'forwarded',
    'affects_stable', 'affects_testing', 'affects_unstable',
    'affects_experimental', 'submitter_name', 'submitter_email', 'owner_name',
    'owner_email', 'done_name', 'done_email', 'affects_oldstable',
    'done_date')
    _computed_fields = ('blocks', 'blockedby', 'merged_with', 'fixed_in',
    'found_in', 'tags', 'usertags', 'packages', 'archived')

    _path = 'bugs'
    _table = ('bugs', 'archived_bugs')

    @property
    def archived(self):
        """Determines whether or not this bug is archived or not."""
        return ('bugs' not in self._table)

    _blocks = None
    @property
    def blocks(self):
        """Bugs this bug blocks."""
        if self._blocks is None:
            self._blocks = self._fetch_linked('blocks', 'blocked', (Bug,))
        return self._blocks

    _blockedby = None
    @property
    def blockedby(self):
        """Bugs this bug blocks."""
        if self._blockedby is None:
            self._blockedby = self._fetch_linked('blockedby', 'blocker', (Bug,))
        return self._blockedby

    _merged_with = None
    @property
    def merged_with(self):
        """Bug that has been merge with this one."""
        if self._merged_with is None:
            self._merged_with = self._fetch_linked('merged_with',
                    'merged_with', (Bug,))
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
        """The tags of this bug"""
        if self._tags is None:
            self._tags = self._fetch_linked('tags', 'tag')
        return self._tags

    _usertags = None
    @property
    def usertags(self):
        """The tags defined by users on this bug. This property is only
        available if this bug is not archived."""
        if self._usertags is None:
            if self.archived:
                print repr(self._table)
                # Archived bugs have to usertags.
                self._usertags = []
            else:
                self._usertags = self._fetch_linked('usertags', ('email', 'tag'))
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


class Developper(UddResource):
    """A Debian Developper, from the Carnivore database.
    """
    _pk = ('id',)
    _path = 'developers'
    _table = ('carnivore_login',)
    _fields = ('id', 'login')
    _computed_fields = ('emails', 'keys', 'names')

    _emails = None
    @property
    def emails(self):
        """The email addresses of this developer."""
        if self._emails is None:
            self._emails = self._fetch_linked('emails', 'email',
                    base_table_name='carnivore_')
        return self._emails

    _keys = None
    @property
    def keys(self):
        """The key of this developer.
        A list of tuples (key, key_type)."""
        if self._keys is None:
            self._keys = self._fetch_linked('keys', ('key', 'key_type'),
                    base_table_name='carnivore_')
        return self._keys

    _names = None
    @property
    def names(self):
        """The names of this developer.
        """
        if self._names is None:
            self._names = self._fetch_linked('names', 'name',
                    base_table_name='carnivore_')
        return self._names


class Package(UddResource):
    """A Debian package.
    This resource has no representation in the UDD, it is created using
    DISTINCT(package) from the subpackages list. 
    """
    _path = 'packages'
    _pk = ('package',)
    _fields = ('package',)
    _computed_fields = ('subpackages', 'tags',)

    def __init__(self, package):
        self._data = {'package': package}

    @classmethod
    def fetch_database(cls, pk=None, package=None):
        query = 'SELECT DISTINCT(package) AS package FROM packages'
        pk = pk or package
        if pk is not None:
            query += ' WHERE package=%s'
        cur = UddResource.cursor()
        try:
            logging.debug('query: ' + query)
            cur.execute(query, (pk,))
            if pk is None:
                return [Package(package=x) for x in cur.fetchall()]
            else:
                obj = cur.fetchone()
                if obj is None:
                    raise ObjectNotFound()
                else:
                    return Package(package=pk)
        finally:
            cur.close()

    @property
    def name(self):
        return self._data['package']

    _tags = None
    @property
    def tags(self):
        """The debtags associated with this package.
        """
        if self._tags is None:
            self._tags = self._fetch_linked('', 'tag',
                    base_table_name='debtags')
        return self._tags

    def get_subpackages(self, **kwargs):
        """Get all subpackages matching the criterions.
        """
        assert 'package' not in kwargs # It would not make any sense.
        return SubPackage.fetch_database(package=self.name, **kwargs)

    @property
    def subpackages(self):
        """Alias for :method:`uddlib.Package.get_subpackages`()
        """
        return self.get_subpackages()

    # TODO: write some properties to provide easy access to data (versions,
    # maintainers, ...)

class SubPackage(UddResource):
    """Represents a package, in a given version, architecure, distribution,
    release, and component.
    """
    _pk = ('package', 'version', 'architecture', 'distribution', 'release',
            'component')
    _path = 'subpackages'
    _table = 'packages'
    _fields = ('package', 'version', 'architecture', 'maintainer',
            'maintainer_name', 'maintainer_email', 'description',
            'long_description', 'source', 'source_version',
            'essential', 'depends', 'recommends', 'suggests', 'enhances',
            'pre_depends', 'breaks', 'installed_size', 'homepage',
            'size', 'build_essential', 'origin', 'sha1', 'replaces',
            'section', 'md5sum', 'bugs', 'priority', 'tag', 'task',
            'python_version', 'provides', 'conflicts', 'sha256',
            'original_maintainer', 'distribution', 'release', 'component',
            'ruby_versions')
    _computed_fields = ('descriptions', 'lintian')

    _descriptions = None
    @property
    def descriptions(self):
        """Descriptions of the package in multiple languages.
        """
        if self._descriptions is None:
            descriptions = self._fetch_linked('', 
                    ('language', 'description', 'long_description', 'md5sum'),
                    base_table_name='ddtp', exclude_from_pk=('architecture',))
            self._descriptions = dict([(x[0], {'description': x[1],
                                               'long_description': x[2],
                                               'md5sum': x[3]},
                                       ) for x in descriptions])
        return self._descriptions

    _lintian = None
    @property
    def lintian(self):
        """Lintian data for this package.
        """
        if self._lintian is None:
            lintian = self._fetch_linked('', 
                    ('package_type', 'tag', 'information',
                    'package_arch', 'package_version'),
                    base_table_name='lintian',
                    exclude_from_pk=('distribution', 'release', 'component',
                                     'architecture', 'version'))
            lintian = [dict(zip(['type', 'tag', 'information'], x))
                       for x in lintian
                       if x[3:] == (self.architecture, self.version)]
            self._lintian = lintian
        return self._lintian



class Popcon(UddResource):
    """Popularity contest.
    """
    _path = 'popcon'
    _table = 'popcon'
    _fields = ['package', 'insts', 'vote', 'olde', 'recent', 'nofiles']

class PopconSrc(Popcon):
    _path = 'popcon_src'
    _table = 'popcon_src'

class PopconSrcAverage(Popcon):
    _path = 'popcon_src_average'
    _table = 'popcon_src_average'


class Source(UddResource):
    """A source package.
    """
    _path = 'sources'
    _table = 'sources'
    _pk = ('source', 'version', 'distribution', 'release')
    _fields = ('source', 'version', 'maintainer', 'maintainer_name',
            'maintainer_email', 'format', 'files', 'uploaders', 'bin',
            'artchitecture', 'standards_version', 'homepage', 'build_depends',
            'build_depends_indep', 'build_conflicts', 'build_conflicts_indep',
            'priority', 'section', 'distribution', 'release', 'componenet',
            'vcs_type', 'vcs_url', 'vcs_browser', 'python_version',
            'checksums_sha1', 'checksums_sha256', 'original_maintainer',
            'dm_upload_allowed', 'ruby_versions')
    _computed_fields = ('uploaders',)

    _uploaders = None
    @property
    def uploaders(self):
        """People who can upload a new source package.
        """
        if self._uploaders is None:
            self._uploaders = self._fetch_linked('', ('uploader', 'name', 'email'),
                    base_table_name='uploaders')
        return self._uploaders

class Uploader(UddResource):
    """Source packages uploaders.
    """
    _path = 'uploaders'
    _table = ('uploaders',)
    _fields = ('source', 'version', 'distribution', 'release', 'component',
            'uploader', 'name', 'email')
    _pk = _fields

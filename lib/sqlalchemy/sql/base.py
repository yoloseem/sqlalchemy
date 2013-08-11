from .. import util, exc
import itertools
from . import visitors
from .. import events


PARSE_AUTOCOMMIT = util.symbol('PARSE_AUTOCOMMIT')
NO_ARG = util.symbol('NO_ARG')

class Immutable(object):
    """mark a ClauseElement as 'immutable' when expressions are cloned."""

    def unique_params(self, *optionaldict, **kwargs):
        raise NotImplementedError("Immutable objects do not support copying")

    def params(self, *optionaldict, **kwargs):
        raise NotImplementedError("Immutable objects do not support copying")

    def _clone(self):
        return self


def _from_objects(*elements):
    return itertools.chain(*[element._from_objects for element in elements])


@util.decorator
def _generative(fn, *args, **kw):
    """Mark a method as generative."""

    self = args[0]._generate()
    fn(self, *args[1:], **kw)
    return self


class Generative(object):
    """Allow a ClauseElement to generate itself via the
    @_generative decorator.

    """

    def _generate(self):
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        return s


class Executable(Generative):
    """Mark a ClauseElement as supporting execution.

    :class:`.Executable` is a superclass for all "statement" types
    of objects, including :func:`select`, :func:`delete`, :func:`update`,
    :func:`insert`, :func:`text`.

    """

    supports_execution = True
    _execution_options = util.immutabledict()
    _bind = None

    @_generative
    def execution_options(self, **kw):
        """ Set non-SQL options for the statement which take effect during
        execution.

        Execution options can be set on a per-statement or
        per :class:`.Connection` basis.   Additionally, the
        :class:`.Engine` and ORM :class:`~.orm.query.Query` objects provide
        access to execution options which they in turn configure upon
        connections.

        The :meth:`execution_options` method is generative.  A new
        instance of this statement is returned that contains the options::

            statement = select([table.c.x, table.c.y])
            statement = statement.execution_options(autocommit=True)

        Note that only a subset of possible execution options can be applied
        to a statement - these include "autocommit" and "stream_results",
        but not "isolation_level" or "compiled_cache".
        See :meth:`.Connection.execution_options` for a full list of
        possible options.

        .. seealso::

            :meth:`.Connection.execution_options()`

            :meth:`.Query.execution_options()`

        """
        if 'isolation_level' in kw:
            raise exc.ArgumentError(
                "'isolation_level' execution option may only be specified "
                "on Connection.execution_options(), or "
                "per-engine using the isolation_level "
                "argument to create_engine()."
            )
        if 'compiled_cache' in kw:
            raise exc.ArgumentError(
                "'compiled_cache' execution option may only be specified "
                "on Connection.execution_options(), not per statement."
            )
        self._execution_options = self._execution_options.union(kw)

    def execute(self, *multiparams, **params):
        """Compile and execute this :class:`.Executable`."""
        e = self.bind
        if e is None:
            label = getattr(self, 'description', self.__class__.__name__)
            msg = ('This %s is not directly bound to a Connection or Engine.'
                   'Use the .execute() method of a Connection or Engine '
                   'to execute this construct.' % label)
            raise exc.UnboundExecutionError(msg)
        return e._execute_clauseelement(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Compile and execute this :class:`.Executable`, returning the
        result's scalar representation.

        """
        return self.execute(*multiparams, **params).scalar()

    @property
    def bind(self):
        """Returns the :class:`.Engine` or :class:`.Connection` to
        which this :class:`.Executable` is bound, or None if none found.

        This is a traversal which checks locally, then
        checks among the "from" clauses of associated objects
        until a bound engine or connection is found.

        """
        if self._bind is not None:
            return self._bind

        for f in _from_objects(self):
            if f is self:
                continue
            engine = f.bind
            if engine is not None:
                return engine
        else:
            return None




class HasPrefixes(object):
    _prefixes = ()

    @_generative
    def prefix_with(self, *expr, **kw):
        """Add one or more expressions following the statement keyword, i.e.
        SELECT, INSERT, UPDATE, or DELETE. Generative.

        This is used to support backend-specific prefix keywords such as those
        provided by MySQL.

        E.g.::

            stmt = table.insert().prefix_with("LOW_PRIORITY", dialect="mysql")

        Multiple prefixes can be specified by multiple calls
        to :meth:`.prefix_with`.

        :param \*expr: textual or :class:`.ClauseElement` construct which
         will be rendered following the INSERT, UPDATE, or DELETE
         keyword.
        :param \**kw: A single keyword 'dialect' is accepted.  This is an
         optional string dialect name which will
         limit rendering of this prefix to only that dialect.

        """
        dialect = kw.pop('dialect', None)
        if kw:
            raise exc.ArgumentError("Unsupported argument(s): %s" %
                            ",".join(kw))
        self._setup_prefixes(expr, dialect)

    def _setup_prefixes(self, prefixes, dialect=None):
        self._prefixes = self._prefixes + tuple(
                            [(_literal_as_text(p), dialect) for p in prefixes])


class SchemaItem(events.SchemaEventTarget, visitors.Visitable):
    """Base class for items that define a database schema."""

    __visit_name__ = 'schema_item'
    quote = None

    def _init_items(self, *args):
        """Initialize the list of child items for this SchemaItem."""

        for item in args:
            if item is not None:
                item._set_parent_with_dispatch(self)

    def get_children(self, **kwargs):
        """used to allow SchemaVisitor access"""
        return []

    def __repr__(self):
        return util.generic_repr(self)

    @util.memoized_property
    def info(self):
        """Info dictionary associated with the object, allowing user-defined
        data to be associated with this :class:`.SchemaItem`.

        The dictionary is automatically generated when first accessed.
        It can also be specified in the constructor of some objects,
        such as :class:`.Table` and :class:`.Column`.

        """
        return {}


class SchemaVisitor(visitors.ClauseVisitor):
    """Define the visiting for ``SchemaItem`` objects."""

    __traverse_options__ = {'schema_visitor': True}



class ColumnCollection(util.OrderedProperties):
    """An ordered dictionary that stores a list of ColumnElement
    instances.

    Overrides the ``__eq__()`` method to produce SQL clauses between
    sets of correlated columns.

    """

    def __init__(self, *cols):
        super(ColumnCollection, self).__init__()
        self._data.update((c.key, c) for c in cols)
        self.__dict__['_all_cols'] = util.column_set(self)

    def __str__(self):
        return repr([str(c) for c in self])

    def replace(self, column):
        """add the given column to this collection, removing unaliased
           versions of this column  as well as existing columns with the
           same key.

            e.g.::

                t = Table('sometable', metadata, Column('col1', Integer))
                t.columns.replace(Column('col1', Integer, key='columnone'))

            will remove the original 'col1' from the collection, and add
            the new column under the name 'columnname'.

           Used by schema.Column to override columns during table reflection.

        """
        if column.name in self and column.key != column.name:
            other = self[column.name]
            if other.name == other.key:
                del self._data[other.name]
                self._all_cols.remove(other)
        if column.key in self._data:
            self._all_cols.remove(self._data[column.key])
        self._all_cols.add(column)
        self._data[column.key] = column

    def add(self, column):
        """Add a column to this collection.

        The key attribute of the column will be used as the hash key
        for this dictionary.

        """
        self[column.key] = column

    def __delitem__(self, key):
        raise NotImplementedError()

    def __setattr__(self, key, object):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        if key in self:

            # this warning is primarily to catch select() statements
            # which have conflicting column names in their exported
            # columns collection

            existing = self[key]
            if not existing.shares_lineage(value):
                util.warn('Column %r on table %r being replaced by '
                          '%r, which has the same key.  Consider '
                          'use_labels for select() statements.' % (key,
                          getattr(existing, 'table', None), value))
            self._all_cols.remove(existing)
            # pop out memoized proxy_set as this
            # operation may very well be occurring
            # in a _make_proxy operation
            memoized_property.reset(value, "proxy_set")
        self._all_cols.add(value)
        self._data[key] = value

    def clear(self):
        self._data.clear()
        self._all_cols.clear()

    def remove(self, column):
        del self._data[column.key]
        self._all_cols.remove(column)

    def update(self, value):
        self._data.update(value)
        self._all_cols.clear()
        self._all_cols.update(self._data.values())

    def extend(self, iter):
        self.update((c.key, c) for c in iter)

    __hash__ = None

    def __eq__(self, other):
        l = []
        for c in other:
            for local in self:
                if c.shares_lineage(local):
                    l.append(c == local)
        return and_(*l)

    def __contains__(self, other):
        if not isinstance(other, util.string_types):
            raise exc.ArgumentError("__contains__ requires a string argument")
        return util.OrderedProperties.__contains__(self, other)

    def __setstate__(self, state):
        self.__dict__['_data'] = state['_data']
        self.__dict__['_all_cols'] = util.column_set(self._data.values())

    def contains_column(self, col):
        # this has to be done via set() membership
        return col in self._all_cols

    def as_immutable(self):
        return ImmutableColumnCollection(self._data, self._all_cols)


class ImmutableColumnCollection(util.ImmutableProperties, ColumnCollection):
    def __init__(self, data, colset):
        util.ImmutableProperties.__init__(self, data)
        self.__dict__['_all_cols'] = colset

    extend = remove = util.ImmutableProperties._immutable


class ColumnSet(util.ordered_column_set):
    def contains_column(self, col):
        return col in self

    def extend(self, cols):
        for col in cols:
            self.add(col)

    def __add__(self, other):
        return list(self) + list(other)

    def __eq__(self, other):
        l = []
        for c in other:
            for local in self:
                if c.shares_lineage(local):
                    l.append(c == local)
        return and_(*l)

    def __hash__(self):
        return hash(tuple(x for x in self))


class Compiled(object):
    """Represent a compiled SQL or DDL expression.

    The ``__str__`` method of the ``Compiled`` object should produce
    the actual text of the statement.  ``Compiled`` objects are
    specific to their underlying database dialect, and also may
    or may not be specific to the columns referenced within a
    particular set of bind parameters.  In no case should the
    ``Compiled`` object be dependent on the actual values of those
    bind parameters, even though it may reference those values as
    defaults.
    """

    def __init__(self, dialect, statement, bind=None,
                compile_kwargs=util.immutabledict()):
        """Construct a new ``Compiled`` object.

        :param dialect: ``Dialect`` to compile against.

        :param statement: ``ClauseElement`` to be compiled.

        :param bind: Optional Engine or Connection to compile this
          statement against.

        :param compile_kwargs: additional kwargs that will be
         passed to the initial call to :meth:`.Compiled.process`.

         .. versionadded:: 0.8

        """

        self.dialect = dialect
        self.bind = bind
        if statement is not None:
            self.statement = statement
            self.can_execute = statement.supports_execution
            self.string = self.process(self.statement, **compile_kwargs)

    @util.deprecated("0.7", ":class:`.Compiled` objects now compile "
                        "within the constructor.")
    def compile(self):
        """Produce the internal string representation of this element."""
        pass

    @property
    def sql_compiler(self):
        """Return a Compiled that is capable of processing SQL expressions.

        If this compiler is one, it would likely just return 'self'.

        """

        raise NotImplementedError()

    def process(self, obj, **kwargs):
        return obj._compiler_dispatch(self, **kwargs)

    def __str__(self):
        """Return the string text of the generated SQL or DDL."""

        return self.string or ''

    def construct_params(self, params=None):
        """Return the bind params for this compiled object.

        :param params: a dict of string/object pairs whose values will
                       override bind values compiled in to the
                       statement.
        """

        raise NotImplementedError()

    @property
    def params(self):
        """Return the bind params for this compiled object."""
        return self.construct_params()

    def execute(self, *multiparams, **params):
        """Execute this compiled object."""

        e = self.bind
        if e is None:
            raise exc.UnboundExecutionError(
                        "This Compiled object is not bound to any Engine "
                        "or Connection.")
        return e._execute_compiled(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Execute this compiled object and return the result's
        scalar value."""

        return self.execute(*multiparams, **params).scalar()


class TypeCompiler(object):
    """Produces DDL specification for TypeEngine objects."""

    def __init__(self, dialect):
        self.dialect = dialect

    def process(self, type_):
        return type_._compiler_dispatch(self)

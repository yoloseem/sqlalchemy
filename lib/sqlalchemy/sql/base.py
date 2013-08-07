from .. import util, exc
import itertools

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

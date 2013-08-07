# sql/expression.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines the base components of SQL expression trees.

All components are derived from a common base class
:class:`.ClauseElement`.  Common behaviors are organized
based on class hierarchies, in some cases via mixins.

All object construction from this package occurs via functions which
in some cases will construct composite :class:`.ClauseElement` structures
together, and in other cases simply return a single :class:`.ClauseElement`
constructed directly.  The function interface affords a more "DSL-ish"
feel to constructing SQL expressions and also allows future class
reorganizations.

Even though classes are not constructed directly from the outside,
most classes which have additional public methods are considered to be
public (i.e. have no leading underscore).  Other classes which are
"semi-public" are marked with a single leading underscore; these
classes usually have few or no public methods and are less guaranteed
to stay the same in future releases.

"""

from __future__ import unicode_literals

from .. import util
from . import operators
from .visitors import Visitable
from .functions import _FunctionGenerator
from .. import types as sqltypes

from . import util as sqlutil
from .elements import ClauseElement, ColumnElement,\
  BindParameter, UnaryExpression, BooleanClauseList, Exists,\
  Label, Cast, Case, ColumnClause, TextClause, Over, Null, \
  True_, False_, BinaryExpression, Tuple, TypeClause, Extract, \
  Grouping, ScalarSelect, and_, or_, not_

from .base import ColumnCollection, Generative, Executable, PARSE_AUTOCOMMIT, NO_ARG

from .selectable import Alias, Join, Select, Selectable, TableClause, \
        CompoundSelect, FromClause, FromGrouping, SelectBase
from .dml import Insert, Update, Delete


__all__ = [
    'Alias', 'ClauseElement', 'ColumnCollection', 'ColumnElement',
    'CompoundSelect', 'Delete', 'FromClause', 'Insert', 'Join', 'Select',
    'Selectable', 'TableClause', 'Update', 'alias', 'and_', 'asc', 'between',
    'bindparam', 'case', 'cast', 'column', 'delete', 'desc', 'distinct',
    'except_', 'except_all', 'exists', 'extract', 'func', 'modifier',
    'collate', 'insert', 'intersect', 'intersect_all', 'join', 'label',
    'literal', 'literal_column', 'not_', 'null', 'nullsfirst', 'nullslast',
    'or_', 'outparam', 'outerjoin', 'over', 'select', 'subquery',
    'table', 'text',
    'tuple_', 'type_coerce', 'union', 'union_all', 'update', ]



def nullsfirst(column):
    """Return a NULLS FIRST ``ORDER BY`` clause element.

    e.g.::

      someselect.order_by(desc(table1.mycol).nullsfirst())

    produces::

      ORDER BY mycol DESC NULLS FIRST

    """
    return UnaryExpression(column, modifier=operators.nullsfirst_op)


def nullslast(column):
    """Return a NULLS LAST ``ORDER BY`` clause element.

    e.g.::

      someselect.order_by(desc(table1.mycol).nullslast())

    produces::

        ORDER BY mycol DESC NULLS LAST

    """
    return UnaryExpression(column, modifier=operators.nullslast_op)


def desc(column):
    """Return a descending ``ORDER BY`` clause element.

    e.g.::

      someselect.order_by(desc(table1.mycol))

    produces::

        ORDER BY mycol DESC

    """
    return UnaryExpression(column, modifier=operators.desc_op)


def asc(column):
    """Return an ascending ``ORDER BY`` clause element.

    e.g.::

      someselect.order_by(asc(table1.mycol))

    produces::

      ORDER BY mycol ASC

    """
    return UnaryExpression(column, modifier=operators.asc_op)


def outerjoin(left, right, onclause=None):
    """Return an ``OUTER JOIN`` clause element.

    The returned object is an instance of :class:`.Join`.

    Similar functionality is also available via the
    :meth:`~.FromClause.outerjoin()` method on any
    :class:`.FromClause`.

    :param left: The left side of the join.

    :param right: The right side of the join.

    :param onclause:  Optional criterion for the ``ON`` clause, is
      derived from foreign key relationships established between
      left and right otherwise.

    To chain joins together, use the :meth:`.FromClause.join` or
    :meth:`.FromClause.outerjoin` methods on the resulting
    :class:`.Join` object.

    """
    return Join(left, right, onclause, isouter=True)


def join(left, right, onclause=None, isouter=False):
    """Return a ``JOIN`` clause element (regular inner join).

    The returned object is an instance of :class:`.Join`.

    Similar functionality is also available via the
    :meth:`~.FromClause.join()` method on any
    :class:`.FromClause`.

    :param left: The left side of the join.

    :param right: The right side of the join.

    :param onclause:  Optional criterion for the ``ON`` clause, is
      derived from foreign key relationships established between
      left and right otherwise.

    To chain joins together, use the :meth:`.FromClause.join` or
    :meth:`.FromClause.outerjoin` methods on the resulting
    :class:`.Join` object.


    """
    return Join(left, right, onclause, isouter)


def select(columns=None, whereclause=None, from_obj=[], **kwargs):
    """Returns a ``SELECT`` clause element.

    Similar functionality is also available via the :func:`select()`
    method on any :class:`.FromClause`.

    The returned object is an instance of :class:`.Select`.

    All arguments which accept :class:`.ClauseElement` arguments also accept
    string arguments, which will be converted as appropriate into
    either :func:`text()` or :func:`literal_column()` constructs.

    .. seealso::

        :ref:`coretutorial_selecting` - Core Tutorial description of
        :func:`.select`.

    :param columns:
      A list of :class:`.ClauseElement` objects, typically
      :class:`.ColumnElement` objects or subclasses, which will form the
      columns clause of the resulting statement. For all members which are
      instances of :class:`.Selectable`, the individual :class:`.ColumnElement`
      members of the :class:`.Selectable` will be added individually to the
      columns clause. For example, specifying a
      :class:`~sqlalchemy.schema.Table` instance will result in all the
      contained :class:`~sqlalchemy.schema.Column` objects within to be added
      to the columns clause.

      This argument is not present on the form of :func:`select()`
      available on :class:`~sqlalchemy.schema.Table`.

    :param whereclause:
      A :class:`.ClauseElement` expression which will be used to form the
      ``WHERE`` clause.

    :param from_obj:
      A list of :class:`.ClauseElement` objects which will be added to the
      ``FROM`` clause of the resulting statement. Note that "from" objects are
      automatically located within the columns and whereclause ClauseElements.
      Use this parameter to explicitly specify "from" objects which are not
      automatically locatable. This could include
      :class:`~sqlalchemy.schema.Table` objects that aren't otherwise present,
      or :class:`.Join` objects whose presence will supercede that of the
      :class:`~sqlalchemy.schema.Table` objects already located in the other
      clauses.

    :param autocommit:
      Deprecated.  Use .execution_options(autocommit=<True|False>)
      to set the autocommit option.

    :param bind=None:
      an :class:`~.base.Engine` or :class:`~.base.Connection` instance
      to which the
      resulting :class:`.Select` object will be bound.  The :class:`.Select`
      object will otherwise automatically bind to whatever
      :class:`~.base.Connectable` instances can be located within its contained
      :class:`.ClauseElement` members.

    :param correlate=True:
      indicates that this :class:`.Select` object should have its
      contained :class:`.FromClause` elements "correlated" to an enclosing
      :class:`.Select` object.  This means that any :class:`.ClauseElement`
      instance within the "froms" collection of this :class:`.Select`
      which is also present in the "froms" collection of an
      enclosing select will not be rendered in the ``FROM`` clause
      of this select statement.

    :param distinct=False:
      when ``True``, applies a ``DISTINCT`` qualifier to the columns
      clause of the resulting statement.

      The boolean argument may also be a column expression or list
      of column expressions - this is a special calling form which
      is understood by the Postgresql dialect to render the
      ``DISTINCT ON (<columns>)`` syntax.

      ``distinct`` is also available via the :meth:`~.Select.distinct`
      generative method.

    :param for_update=False:
      when ``True``, applies ``FOR UPDATE`` to the end of the
      resulting statement.

      Certain database dialects also support
      alternate values for this parameter:

      * With the MySQL dialect, the value ``"read"`` translates to
        ``LOCK IN SHARE MODE``.
      * With the Oracle and Postgresql dialects, the value ``"nowait"``
        translates to ``FOR UPDATE NOWAIT``.
      * With the Postgresql dialect, the values "read" and ``"read_nowait"``
        translate to ``FOR SHARE`` and ``FOR SHARE NOWAIT``, respectively.

        .. versionadded:: 0.7.7

    :param group_by:
      a list of :class:`.ClauseElement` objects which will comprise the
      ``GROUP BY`` clause of the resulting select.

    :param having:
      a :class:`.ClauseElement` that will comprise the ``HAVING`` clause
      of the resulting select when ``GROUP BY`` is used.

    :param limit=None:
      a numerical value which usually compiles to a ``LIMIT``
      expression in the resulting select.  Databases that don't
      support ``LIMIT`` will attempt to provide similar
      functionality.

    :param offset=None:
      a numeric value which usually compiles to an ``OFFSET``
      expression in the resulting select.  Databases that don't
      support ``OFFSET`` will attempt to provide similar
      functionality.

    :param order_by:
      a scalar or list of :class:`.ClauseElement` objects which will
      comprise the ``ORDER BY`` clause of the resulting select.

    :param use_labels=False:
      when ``True``, the statement will be generated using labels
      for each column in the columns clause, which qualify each
      column with its parent table's (or aliases) name so that name
      conflicts between columns in different tables don't occur.
      The format of the label is <tablename>_<column>.  The "c"
      collection of the resulting :class:`.Select` object will use these
      names as well for targeting column members.

      use_labels is also available via the :meth:`~.SelectBase.apply_labels`
      generative method.

    """
    return Select(columns, whereclause=whereclause, from_obj=from_obj,
                  **kwargs)


def subquery(alias, *args, **kwargs):
    """Return an :class:`.Alias` object derived
    from a :class:`.Select`.

    name
      alias name

    \*args, \**kwargs

      all other arguments are delivered to the
      :func:`select` function.

    """
    return Select(*args, **kwargs).alias(alias)


def insert(table, values=None, inline=False, **kwargs):
    """Represent an ``INSERT`` statement via the :class:`.Insert` SQL
    construct.

    Similar functionality is available via the
    :meth:`~.TableClause.insert` method on
    :class:`~.schema.Table`.


    :param table: :class:`.TableClause` which is the subject of the insert.

    :param values: collection of values to be inserted; see
     :meth:`.Insert.values` for a description of allowed formats here.
     Can be omitted entirely; a :class:`.Insert` construct will also
     dynamically render the VALUES clause at execution time based on
     the parameters passed to :meth:`.Connection.execute`.

    :param inline: if True, SQL defaults will be compiled 'inline' into the
      statement and not pre-executed.

    If both `values` and compile-time bind parameters are present, the
    compile-time bind parameters override the information specified
    within `values` on a per-key basis.

    The keys within `values` can be either :class:`~sqlalchemy.schema.Column`
    objects or their string identifiers. Each key may reference one of:

    * a literal data value (i.e. string, number, etc.);
    * a Column object;
    * a SELECT statement.

    If a ``SELECT`` statement is specified which references this
    ``INSERT`` statement's table, the statement will be correlated
    against the ``INSERT`` statement.

    .. seealso::

        :ref:`coretutorial_insert_expressions` - SQL Expression Tutorial

        :ref:`inserts_and_updates` - SQL Expression Tutorial

    """
    return Insert(table, values, inline=inline, **kwargs)


def update(table, whereclause=None, values=None, inline=False, **kwargs):
    """Represent an ``UPDATE`` statement via the :class:`.Update` SQL
    construct.

    E.g.::

        from sqlalchemy import update

        stmt = update(users).where(users.c.id==5).\\
                values(name='user #5')

    Similar functionality is available via the
    :meth:`~.TableClause.update` method on
    :class:`.Table`::


        stmt = users.update().\\
                    where(users.c.id==5).\\
                    values(name='user #5')

    :param table: A :class:`.Table` object representing the database
     table to be updated.

    :param whereclause: Optional SQL expression describing the ``WHERE``
     condition of the ``UPDATE`` statement.   Modern applications
     may prefer to use the generative :meth:`~Update.where()`
     method to specify the ``WHERE`` clause.

     The WHERE clause can refer to multiple tables.
     For databases which support this, an ``UPDATE FROM`` clause will
     be generated, or on MySQL, a multi-table update.  The statement
     will fail on databases that don't have support for multi-table
     update statements.  A SQL-standard method of referring to
     additional tables in the WHERE clause is to use a correlated
     subquery::

        users.update().values(name='ed').where(
                users.c.name==select([addresses.c.email_address]).\\
                            where(addresses.c.user_id==users.c.id).\\
                            as_scalar()
                )

     .. versionchanged:: 0.7.4
         The WHERE clause can refer to multiple tables.

    :param values:
      Optional dictionary which specifies the ``SET`` conditions of the
      ``UPDATE``.  If left as ``None``, the ``SET``
      conditions are determined from those parameters passed to the
      statement during the execution and/or compilation of the
      statement.   When compiled standalone without any parameters,
      the ``SET`` clause generates for all columns.

      Modern applications may prefer to use the generative
      :meth:`.Update.values` method to set the values of the
      UPDATE statement.

    :param inline:
      if True, SQL defaults present on :class:`.Column` objects via
      the ``default`` keyword will be compiled 'inline' into the statement
      and not pre-executed.  This means that their values will not
      be available in the dictionary returned from
      :meth:`.ResultProxy.last_updated_params`.

    If both ``values`` and compile-time bind parameters are present, the
    compile-time bind parameters override the information specified
    within ``values`` on a per-key basis.

    The keys within ``values`` can be either :class:`.Column`
    objects or their string identifiers (specifically the "key" of the
    :class:`.Column`, normally but not necessarily equivalent to
    its "name").  Normally, the
    :class:`.Column` objects used here are expected to be
    part of the target :class:`.Table` that is the table
    to be updated.  However when using MySQL, a multiple-table
    UPDATE statement can refer to columns from any of
    the tables referred to in the WHERE clause.

    The values referred to in ``values`` are typically:

    * a literal data value (i.e. string, number, etc.)
    * a SQL expression, such as a related :class:`.Column`,
      a scalar-returning :func:`.select` construct,
      etc.

    When combining :func:`.select` constructs within the values
    clause of an :func:`.update` construct,
    the subquery represented by the :func:`.select` should be
    *correlated* to the parent table, that is, providing criterion
    which links the table inside the subquery to the outer table
    being updated::

        users.update().values(
                name=select([addresses.c.email_address]).\\
                        where(addresses.c.user_id==users.c.id).\\
                        as_scalar()
            )

    .. seealso::

        :ref:`inserts_and_updates` - SQL Expression
        Language Tutorial


    """
    return Update(
            table,
            whereclause=whereclause,
            values=values,
            inline=inline,
            **kwargs)


def delete(table, whereclause=None, **kwargs):
    """Represent a ``DELETE`` statement via the :class:`.Delete` SQL
    construct.

    Similar functionality is available via the
    :meth:`~.TableClause.delete` method on
    :class:`~.schema.Table`.

    :param table: The table to be updated.

    :param whereclause: A :class:`.ClauseElement` describing the ``WHERE``
      condition of the ``UPDATE`` statement. Note that the
      :meth:`~Delete.where()` generative method may be used instead.

    .. seealso::

        :ref:`deletes` - SQL Expression Tutorial

    """
    return Delete(table, whereclause, **kwargs)




def distinct(expr):
    """Return a ``DISTINCT`` clause.

    e.g.::

        distinct(a)

    renders::

        DISTINCT a

    """
    expr = sqlutil._literal_as_binds(expr)
    return UnaryExpression(expr,
                operator=operators.distinct_op, type_=expr.type)


def between(ctest, cleft, cright):
    """Return a ``BETWEEN`` predicate clause.

    Equivalent of SQL ``clausetest BETWEEN clauseleft AND clauseright``.

    The :func:`between()` method on all
    :class:`.ColumnElement` subclasses provides
    similar functionality.

    """
    ctest = sqlutil._literal_as_binds(ctest)
    return ctest.between(cleft, cright)


def case(whens, value=None, else_=None):
    """Produce a ``CASE`` statement.

    whens
      A sequence of pairs, or alternatively a dict,
      to be translated into "WHEN / THEN" clauses.

    value
      Optional for simple case statements, produces
      a column expression as in "CASE <expr> WHEN ..."

    else\_
      Optional as well, for case defaults produces
      the "ELSE" portion of the "CASE" statement.

    The expressions used for THEN and ELSE,
    when specified as strings, will be interpreted
    as bound values. To specify textual SQL expressions
    for these, use the :func:`literal_column`
    construct.

    The expressions used for the WHEN criterion
    may only be literal strings when "value" is
    present, i.e. CASE table.somecol WHEN "x" THEN "y".
    Otherwise, literal strings are not accepted
    in this position, and either the text(<string>)
    or literal(<string>) constructs must be used to
    interpret raw string values.

    Usage examples::

      case([(orderline.c.qty > 100, item.c.specialprice),
            (orderline.c.qty > 10, item.c.bulkprice)
          ], else_=item.c.regularprice)
      case(value=emp.c.type, whens={
              'engineer': emp.c.salary * 1.1,
              'manager':  emp.c.salary * 3,
          })

    Using :func:`literal_column()`, to allow for databases that
    do not support bind parameters in the ``then`` clause.  The type
    can be specified which determines the type of the :func:`case()` construct
    overall::

        case([(orderline.c.qty > 100,
                literal_column("'greaterthan100'", String)),
              (orderline.c.qty > 10, literal_column("'greaterthan10'",
                String))
            ], else_=literal_column("'lethan10'", String))

    """

    return Case(whens, value=value, else_=else_)


def cast(clause, totype, **kwargs):
    """Return a ``CAST`` function.

    Equivalent of SQL ``CAST(clause AS totype)``.

    Use with a :class:`~sqlalchemy.types.TypeEngine` subclass, i.e::

      cast(table.c.unit_price * table.c.qty, Numeric(10,4))

    or::

      cast(table.c.timestamp, DATE)

    """
    return Cast(clause, totype, **kwargs)


def extract(field, expr):
    """Return the clause ``extract(field FROM expr)``."""

    return Extract(field, expr)


def collate(expression, collation):
    """Return the clause ``expression COLLATE collation``.

    e.g.::

        collate(mycolumn, 'utf8_bin')

    produces::

        mycolumn COLLATE utf8_bin

    """

    expr = sqlutil._literal_as_binds(expression)
    return BinaryExpression(
        expr,
        sqlutil._literal_as_text(collation),
        operators.collate, type_=expr.type)


def exists(*args, **kwargs):
    """Return an ``EXISTS`` clause as applied to a :class:`.Select` object.

    Calling styles are of the following forms::

        # use on an existing select()
        s = select([table.c.col1]).where(table.c.col2==5)
        s = exists(s)

        # construct a select() at once
        exists(['*'], **select_arguments).where(criterion)

        # columns argument is optional, generates "EXISTS (SELECT *)"
        # by default.
        exists().where(table.c.col2==5)

    """
    return Exists(*args, **kwargs)


def union(*selects, **kwargs):
    """Return a ``UNION`` of multiple selectables.

    The returned object is an instance of
    :class:`.CompoundSelect`.

    A similar :func:`union()` method is available on all
    :class:`.FromClause` subclasses.

    \*selects
      a list of :class:`.Select` instances.

    \**kwargs
       available keyword arguments are the same as those of
       :func:`select`.

    """
    return CompoundSelect(CompoundSelect.UNION, *selects, **kwargs)


def union_all(*selects, **kwargs):
    """Return a ``UNION ALL`` of multiple selectables.

    The returned object is an instance of
    :class:`.CompoundSelect`.

    A similar :func:`union_all()` method is available on all
    :class:`.FromClause` subclasses.

    \*selects
      a list of :class:`.Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.UNION_ALL, *selects, **kwargs)


def except_(*selects, **kwargs):
    """Return an ``EXCEPT`` of multiple selectables.

    The returned object is an instance of
    :class:`.CompoundSelect`.

    \*selects
      a list of :class:`.Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.EXCEPT, *selects, **kwargs)


def except_all(*selects, **kwargs):
    """Return an ``EXCEPT ALL`` of multiple selectables.

    The returned object is an instance of
    :class:`.CompoundSelect`.

    \*selects
      a list of :class:`.Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.EXCEPT_ALL, *selects, **kwargs)


def intersect(*selects, **kwargs):
    """Return an ``INTERSECT`` of multiple selectables.

    The returned object is an instance of
    :class:`.CompoundSelect`.

    \*selects
      a list of :class:`.Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.INTERSECT, *selects, **kwargs)


def intersect_all(*selects, **kwargs):
    """Return an ``INTERSECT ALL`` of multiple selectables.

    The returned object is an instance of
    :class:`.CompoundSelect`.

    \*selects
      a list of :class:`.Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.INTERSECT_ALL, *selects, **kwargs)


def alias(selectable, name=None, flat=False):
    """Return an :class:`.Alias` object.

    An :class:`.Alias` represents any :class:`.FromClause`
    with an alternate name assigned within SQL, typically using the ``AS``
    clause when generated, e.g. ``SELECT * FROM table AS aliasname``.

    Similar functionality is available via the
    :meth:`~.FromClause.alias` method
    available on all :class:`.FromClause` subclasses.

    When an :class:`.Alias` is created from a :class:`.Table` object,
    this has the effect of the table being rendered
    as ``tablename AS aliasname`` in a SELECT statement.

    For :func:`.select` objects, the effect is that of creating a named
    subquery, i.e. ``(select ...) AS aliasname``.

    The ``name`` parameter is optional, and provides the name
    to use in the rendered SQL.  If blank, an "anonymous" name
    will be deterministically generated at compile time.
    Deterministic means the name is guaranteed to be unique against
    other constructs used in the same statement, and will also be the
    same name for each successive compilation of the same statement
    object.

    :param selectable: any :class:`.FromClause` subclass,
        such as a table, select statement, etc.

    :param name: string name to be assigned as the alias.
        If ``None``, a name will be deterministically generated
        at compile time.

    :param flat: Will be passed through to if the given selectable
     is an instance of :class:`.Join` - see :meth:`.Join.alias`
     for details.

     .. versionadded:: 0.9.0

    """
    return selectable.alias(name=name, flat=flat)


def literal(value, type_=None):
    """Return a literal clause, bound to a bind parameter.

    Literal clauses are created automatically when non- :class:`.ClauseElement`
    objects (such as strings, ints, dates, etc.) are used in a comparison
    operation with a :class:`.ColumnElement`
    subclass, such as a :class:`~sqlalchemy.schema.Column` object.
    Use this function to force the
    generation of a literal clause, which will be created as a
    :class:`BindParameter` with a bound value.

    :param value: the value to be bound. Can be any Python object supported by
        the underlying DB-API, or is translatable via the given type argument.

    :param type\_: an optional :class:`~sqlalchemy.types.TypeEngine` which
        will provide bind-parameter translation for this literal.

    """
    return BindParameter(None, value, type_=type_, unique=True)


def tuple_(*expr):
    """Return a SQL tuple.

    Main usage is to produce a composite IN construct::

        tuple_(table.c.col1, table.c.col2).in_(
            [(1, 2), (5, 12), (10, 19)]
        )

    .. warning::

        The composite IN construct is not supported by all backends,
        and is currently known to work on Postgresql and MySQL,
        but not SQLite.   Unsupported backends will raise
        a subclass of :class:`~sqlalchemy.exc.DBAPIError` when such
        an expression is invoked.

    """
    return Tuple(*expr)


def type_coerce(expr, type_):
    """Coerce the given expression into the given type,
    on the Python side only.

    :func:`.type_coerce` is roughly similar to :func:`.cast`, except no
    "CAST" expression is rendered - the given type is only applied towards
    expression typing and against received result values.

    e.g.::

        from sqlalchemy.types import TypeDecorator
        import uuid

        class AsGuid(TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                if value is not None:
                    return str(value)
                else:
                    return None

            def process_result_value(self, value, dialect):
                if value is not None:
                    return uuid.UUID(value)
                else:
                    return None

        conn.execute(
            select([type_coerce(mytable.c.ident, AsGuid)]).\\
                    where(
                        type_coerce(mytable.c.ident, AsGuid) ==
                        uuid.uuid3(uuid.NAMESPACE_URL, 'bar')
                    )
        )

    """
    type_ = sqltypes.to_instance(type_)

    if hasattr(expr, '__clause_expr__'):
        return type_coerce(expr.__clause_expr__())
    elif isinstance(expr, BindParameter):
        bp = expr._clone()
        bp.type = type_
        return bp
    elif not isinstance(expr, Visitable):
        if expr is None:
            return null()
        else:
            return literal(expr, type_=type_)
    else:
        return Label(None, expr, type_=type_)


def label(name, obj):
    """Return a :class:`Label` object for the
    given :class:`.ColumnElement`.

    A label changes the name of an element in the columns clause of a
    ``SELECT`` statement, typically via the ``AS`` SQL keyword.

    This functionality is more conveniently available via the
    :func:`label()` method on :class:`.ColumnElement`.

    name
      label name

    obj
      a :class:`.ColumnElement`.

    """
    return Label(name, obj)


def column(text, type_=None):
    """Return a textual column clause, as would be in the columns clause of a
    ``SELECT`` statement.

    The object returned is an instance of :class:`.ColumnClause`, which
    represents the "syntactical" portion of the schema-level
    :class:`~sqlalchemy.schema.Column` object.  It is often used directly
    within :func:`~.expression.select` constructs or with lightweight
    :func:`~.expression.table` constructs.

    Note that the :func:`~.expression.column` function is not part of
    the ``sqlalchemy`` namespace.  It must be imported from the
    ``sql`` package::

        from sqlalchemy.sql import table, column

    :param text: the name of the column.  Quoting rules will be applied
      to the clause like any other column name. For textual column constructs
      that are not to be quoted, use the :func:`literal_column` function.

    :param type\_: an optional :class:`~sqlalchemy.types.TypeEngine` object
      which will provide result-set translation for this column.

    See :class:`.ColumnClause` for further examples.

    """
    return ColumnClause(text, type_=type_)


def literal_column(text, type_=None):
    """Return a textual column expression, as would be in the columns
    clause of a ``SELECT`` statement.

    The object returned supports further expressions in the same way as any
    other column object, including comparison, math and string operations.
    The type\_ parameter is important to determine proper expression behavior
    (such as, '+' means string concatenation or numerical addition based on
    the type).

    :param text: the text of the expression; can be any SQL expression.
      Quoting rules will not be applied. To specify a column-name expression
      which should be subject to quoting rules, use the :func:`column`
      function.

    :param type\_: an optional :class:`~sqlalchemy.types.TypeEngine`
      object which will
      provide result-set translation and additional expression semantics for
      this column. If left as None the type will be NullType.

    """
    return ColumnClause(text, type_=type_, is_literal=True)


def table(name, *columns):
    """Represent a textual table clause.

    The object returned is an instance of :class:`.TableClause`, which
    represents the "syntactical" portion of the schema-level
    :class:`~.schema.Table` object.
    It may be used to construct lightweight table constructs.

    Note that the :func:`~.expression.table` function is not part of
    the ``sqlalchemy`` namespace.  It must be imported from the
    ``sql`` package::

        from sqlalchemy.sql import table, column

    :param name: Name of the table.

    :param columns: A collection of :func:`~.expression.column` constructs.

    See :class:`.TableClause` for further examples.

    """
    return TableClause(name, *columns)


def bindparam(key, value=NO_ARG, type_=None, unique=False, required=NO_ARG,
                        quote=None, callable_=None):
    """Create a bind parameter clause with the given key.

        :param key:
          the key for this bind param.  Will be used in the generated
          SQL statement for dialects that use named parameters.  This
          value may be modified when part of a compilation operation,
          if other :class:`BindParameter` objects exist with the same
          key, or if its length is too long and truncation is
          required.

        :param value:
          Initial value for this bind param.  This value may be
          overridden by the dictionary of parameters sent to statement
          compilation/execution.

          Defaults to ``None``, however if neither ``value`` nor
          ``callable`` are passed explicitly, the ``required`` flag will be
          set to ``True`` which has the effect of requiring a value be present
          when the statement is actually executed.

          .. versionchanged:: 0.8 The ``required`` flag is set to ``True``
             automatically if ``value`` or ``callable`` is not passed.

        :param callable\_:
          A callable function that takes the place of "value".  The function
          will be called at statement execution time to determine the
          ultimate value.   Used for scenarios where the actual bind
          value cannot be determined at the point at which the clause
          construct is created, but embedded bind values are still desirable.

        :param type\_:
          A ``TypeEngine`` object that will be used to pre-process the
          value corresponding to this :class:`BindParameter` at
          execution time.

        :param unique:
          if True, the key name of this BindParamClause will be
          modified if another :class:`BindParameter` of the same name
          already has been located within the containing
          :class:`.ClauseElement`.

        :param required:
          If ``True``, a value is required at execution time.  If not passed,
          is set to ``True`` or ``False`` based on whether or not
          one of ``value`` or ``callable`` were passed..

          .. versionchanged:: 0.8 If the ``required`` flag is not specified,
             it will be set automatically to ``True`` or ``False`` depending
             on whether or not the ``value`` or ``callable`` parameters
             were specified.

        :param quote:
          True if this parameter name requires quoting and is not
          currently known as a SQLAlchemy reserved word; this currently
          only applies to the Oracle backend.

    """
    if isinstance(key, ColumnClause):
        type_ = key.type
        key = key.name
    if required is NO_ARG:
        required = (value is NO_ARG and callable_ is None)
    if value is NO_ARG:
        value = None
    return BindParameter(key, value, type_=type_,
                            callable_=callable_,
                            unique=unique, required=required,
                            quote=quote)


def outparam(key, type_=None):
    """Create an 'OUT' parameter for usage in functions (stored procedures),
    for databases which support them.

    The ``outparam`` can be used like a regular function parameter.
    The "output" value will be available from the
    :class:`~sqlalchemy.engine.ResultProxy` object via its ``out_parameters``
    attribute, which returns a dictionary containing the values.

    """
    return BindParameter(
                key, None, type_=type_, unique=False, isoutparam=True)


def text(text, bind=None, *args, **kwargs):
    """Create a SQL construct that is represented by a literal string.

    E.g.::

        t = text("SELECT * FROM users")
        result = connection.execute(t)

    The advantages :func:`text` provides over a plain string are
    backend-neutral support for bind parameters, per-statement
    execution options, as well as
    bind parameter and result-column typing behavior, allowing
    SQLAlchemy type constructs to play a role when executing
    a statement that is specified literally.

    Bind parameters are specified by name, using the format ``:name``.
    E.g.::

        t = text("SELECT * FROM users WHERE id=:user_id")
        result = connection.execute(t, user_id=12)

    To invoke SQLAlchemy typing logic for bind parameters, the
    ``bindparams`` list allows specification of :func:`bindparam`
    constructs which specify the type for a given name::

        t = text("SELECT id FROM users WHERE updated_at>:updated",
                    bindparams=[bindparam('updated', DateTime())]
                )

    Typing during result row processing is also an important concern.
    Result column types
    are specified using the ``typemap`` dictionary, where the keys
    match the names of columns.  These names are taken from what
    the DBAPI returns as ``cursor.description``::

        t = text("SELECT id, name FROM users",
                typemap={
                    'id':Integer,
                    'name':Unicode
                }
        )

    The :func:`text` construct is used internally for most cases when
    a literal string is specified for part of a larger query, such as
    within :func:`select()`, :func:`update()`,
    :func:`insert()` or :func:`delete()`.   In those cases, the same
    bind parameter syntax is applied::

        s = select([users.c.id, users.c.name]).where("id=:user_id")
        result = connection.execute(s, user_id=12)

    Using :func:`text` explicitly usually implies the construction
    of a full, standalone statement.   As such, SQLAlchemy refers
    to it as an :class:`.Executable` object, and it supports
    the :meth:`Executable.execution_options` method.  For example,
    a :func:`text` construct that should be subject to "autocommit"
    can be set explicitly so using the ``autocommit`` option::

        t = text("EXEC my_procedural_thing()").\\
                execution_options(autocommit=True)

    Note that SQLAlchemy's usual "autocommit" behavior applies to
    :func:`text` constructs - that is, statements which begin
    with a phrase such as ``INSERT``, ``UPDATE``, ``DELETE``,
    or a variety of other phrases specific to certain backends, will
    be eligible for autocommit if no transaction is in progress.

    :param text:
      the text of the SQL statement to be created.  use ``:<param>``
      to specify bind parameters; they will be compiled to their
      engine-specific format.

    :param autocommit:
      Deprecated.  Use .execution_options(autocommit=<True|False>)
      to set the autocommit option.

    :param bind:
      an optional connection or engine to be used for this text query.

    :param bindparams:
      a list of :func:`bindparam()` instances which can be used to define
      the types and/or initial values for the bind parameters within
      the textual statement; the keynames of the bindparams must match
      those within the text of the statement.  The types will be used
      for pre-processing on bind values.

    :param typemap:
      a dictionary mapping the names of columns represented in the
      columns clause of a ``SELECT`` statement  to type objects,
      which will be used to perform post-processing on columns within
      the result set.   This argument applies to any expression
      that returns result sets.

    """
    return TextClause(text, bind=bind, *args, **kwargs)


def over(func, partition_by=None, order_by=None):
    """Produce an OVER clause against a function.

    Used against aggregate or so-called "window" functions,
    for database backends that support window functions.

    E.g.::

        from sqlalchemy import over
        over(func.row_number(), order_by='x')

    Would produce "ROW_NUMBER() OVER(ORDER BY x)".

    :param func: a :class:`.FunctionElement` construct, typically
     generated by :data:`~.expression.func`.
    :param partition_by: a column element or string, or a list
     of such, that will be used as the PARTITION BY clause
     of the OVER construct.
    :param order_by: a column element or string, or a list
     of such, that will be used as the ORDER BY clause
     of the OVER construct.

    This function is also available from the :data:`~.expression.func`
    construct itself via the :meth:`.FunctionElement.over` method.

    .. versionadded:: 0.7

    """
    return Over(func, partition_by=partition_by, order_by=order_by)


def null():
    """Return a :class:`Null` object, which compiles to ``NULL``.

    """
    return Null()


def true():
    """Return a :class:`True_` object, which compiles to ``true``, or the
    boolean equivalent for the target dialect.

    """
    return True_()


def false():
    """Return a :class:`False_` object, which compiles to ``false``, or the
    boolean equivalent for the target dialect.

    """
    return False_()


# "func" global - i.e. func.count()
func = _FunctionGenerator()
"""Generate SQL function expressions.

   :data:`.func` is a special object instance which generates SQL
   functions based on name-based attributes, e.g.::

        >>> print func.count(1)
        count(:param_1)

   The element is a column-oriented SQL element like any other, and is
   used in that way::

        >>> print select([func.count(table.c.id)])
        SELECT count(sometable.id) FROM sometable

   Any name can be given to :data:`.func`. If the function name is unknown to
   SQLAlchemy, it will be rendered exactly as is. For common SQL functions
   which SQLAlchemy is aware of, the name may be interpreted as a *generic
   function* which will be compiled appropriately to the target database::

        >>> print func.current_timestamp()
        CURRENT_TIMESTAMP

   To call functions which are present in dot-separated packages,
   specify them in the same manner::

        >>> print func.stats.yield_curve(5, 10)
        stats.yield_curve(:yield_curve_1, :yield_curve_2)

   SQLAlchemy can be made aware of the return type of functions to enable
   type-specific lexical and result-based behavior. For example, to ensure
   that a string-based function returns a Unicode value and is similarly
   treated as a string in expressions, specify
   :class:`~sqlalchemy.types.Unicode` as the type:

        >>> print func.my_string(u'hi', type_=Unicode) + ' ' + \
        ... func.my_string(u'there', type_=Unicode)
        my_string(:my_string_1) || :my_string_2 || my_string(:my_string_3)

   The object returned by a :data:`.func` call is usually an instance of
   :class:`.Function`.
   This object meets the "column" interface, including comparison and labeling
   functions.  The object can also be passed the :meth:`~.Connectable.execute`
   method of a :class:`.Connection` or :class:`.Engine`, where it will be
   wrapped inside of a SELECT statement first::

        print connection.execute(func.current_timestamp()).scalar()

   In a few exception cases, the :data:`.func` accessor
   will redirect a name to a built-in expression such as :func:`.cast`
   or :func:`.extract`, as these names have well-known meaning
   but are not exactly the same as "functions" from a SQLAlchemy
   perspective.

   .. versionadded:: 0.8 :data:`.func` can return non-function expression
      constructs for common quasi-functional names like :func:`.cast`
      and :func:`.extract`.

   Functions which are interpreted as "generic" functions know how to
   calculate their return type automatically. For a listing of known generic
   functions, see :ref:`generic_functions`.

"""

# "modifier" global - i.e. modifier.distinct
# TODO: use UnaryExpression for this instead ?
modifier = _FunctionGenerator(group=False)


# legacy, some outside users may be calling this
_Executable = Executable


# old names for compatibility
_BindParamClause = BindParameter
_Label = Label
_SelectBase = SelectBase
_BinaryExpression = BinaryExpression
_Cast = Cast
_Null = Null
_False = False_
_True = True_
_TextClause = TextClause
_UnaryExpression = UnaryExpression
_Case = Case
_Tuple = Tuple
_Over = Over
_Generative = Generative
_TypeClause = TypeClause
_Extract = Extract
_Exists = Exists
_Grouping = Grouping
_FromGrouping = FromGrouping
_ScalarSelect = ScalarSelect

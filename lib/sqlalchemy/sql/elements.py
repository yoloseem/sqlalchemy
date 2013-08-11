from .. import util, exc, inspection
from . import types_base as sqltypes
from . import operators
from .visitors import Visitable
from . import visitors
from .annotation import Annotated
import itertools
from .base import Executable, PARSE_AUTOCOMMIT, Immutable, Generative, _generative
import re

annotation = util.importlater("sqlalchemy.sql", "annotation")

class _truncated_label(util.text_type):
    """A unicode subclass used to identify symbolic "
    "names that may require truncation."""

    def apply_map(self, map_):
        return self

# for backwards compatibility in case
# someone is re-implementing the
# _truncated_identifier() sequence in a custom
# compiler
_generated_label = _truncated_label


class _anonymous_label(_truncated_label):
    """A unicode subclass used to identify anonymously
    generated names."""

    def __add__(self, other):
        return _anonymous_label(
                    util.text_type(self) +
                    util.text_type(other))

    def __radd__(self, other):
        return _anonymous_label(
                    util.text_type(other) +
                    util.text_type(self))

    def apply_map(self, map_):
        return self % map_


def _as_truncated(value):
    """coerce the given value to :class:`._truncated_label`.

    Existing :class:`._truncated_label` and
    :class:`._anonymous_label` objects are passed
    unchanged.
    """

    if isinstance(value, _truncated_label):
        return value
    else:
        return _truncated_label(value)


def _string_or_unprintable(element):
    if isinstance(element, util.string_types):
        return element
    else:
        try:
            return str(element)
        except:
            return "unprintable element %r" % element


def _clone(element, **kw):
    return element._clone()


def _expand_cloned(elements):
    """expand the given set of ClauseElements to be the set of all 'cloned'
    predecessors.

    """
    return itertools.chain(*[x._cloned_set for x in elements])


def _select_iterables(elements):
    """expand tables into individual columns in the
    given list of column expressions.

    """
    return itertools.chain(*[c._select_iterable for c in elements])


def _cloned_intersection(a, b):
    """return the intersection of sets a and b, counting
    any overlap between 'cloned' predecessors.

    The returned set is in terms of the entities present within 'a'.

    """
    all_overlap = set(_expand_cloned(a)).intersection(_expand_cloned(b))
    return set(elem for elem in a
               if all_overlap.intersection(elem._cloned_set))

def _cloned_difference(a, b):
    all_overlap = set(_expand_cloned(a)).intersection(_expand_cloned(b))
    return set(elem for elem in a
                if not all_overlap.intersection(elem._cloned_set))


def _labeled(element):
    if not hasattr(element, 'name'):
        return element.label(None)
    else:
        return element


def find_columns(clause):
    """locate Column objects within the given expression."""

    cols = util.column_set()
    visitors.traverse(clause, {}, {'column': cols.add})
    return cols


# there is some inconsistency here between the usage of
# inspect() vs. checking for Visitable and __clause_element__.
# Ideally all functions here would derive from inspect(),
# however the inspect() versions add significant callcount
# overhead for critical functions like _interpret_as_column_or_from().
# Generally, the column-based functions are more performance critical
# and are fine just checking for __clause_element__().  it's only
# _interpret_as_from() where we'd like to be able to receive ORM entities
# that have no defined namespace, hence inspect() is needed there.


def _column_as_key(element):
    if isinstance(element, util.string_types):
        return element
    if hasattr(element, '__clause_element__'):
        element = element.__clause_element__()
    try:
        return element.key
    except AttributeError:
        return None


def _clause_element_as_expr(element):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    else:
        return element


def _literal_as_text(element):
    if isinstance(element, Visitable):
        return element
    elif hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif isinstance(element, util.string_types):
        return TextClause(util.text_type(element))
    elif isinstance(element, (util.NoneType, bool)):
        return _const_expr(element)
    else:
        raise exc.ArgumentError(
            "SQL expression object or string expected."
        )


def _no_literals(element):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif not isinstance(element, Visitable):
        raise exc.ArgumentError("Ambiguous literal: %r.  Use the 'text()' "
                                "function to indicate a SQL expression "
                                "literal, or 'literal()' to indicate a "
                                "bound value." % element)
    else:
        return element


def _is_literal(element):
    return not isinstance(element, Visitable) and \
            not hasattr(element, '__clause_element__')


def _only_column_elements_or_none(element, name):
    if element is None:
        return None
    else:
        return _only_column_elements(element, name)


def _only_column_elements(element, name):
    if hasattr(element, '__clause_element__'):
        element = element.__clause_element__()
    if not isinstance(element, ColumnElement):
        raise exc.ArgumentError(
                "Column-based expression object expected for argument "
                "'%s'; got: '%s', type %s" % (name, element, type(element)))
    return element


def _literal_as_binds(element, name=None, type_=None):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif not isinstance(element, Visitable):
        if element is None:
            return Null()
        else:
            return BindParameter(name, element, type_=type_, unique=True)
    else:
        return element


def _interpret_as_column_or_from(element):
    if isinstance(element, Visitable):
        return element
    elif hasattr(element, '__clause_element__'):
        return element.__clause_element__()

    insp = inspection.inspect(element, raiseerr=False)
    if insp is None:
        if isinstance(element, (util.NoneType, bool)):
            return _const_expr(element)
    elif hasattr(insp, "selectable"):
        return insp.selectable

    return ColumnClause(str(element), is_literal=True)

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

def collate(expression, collation):
    """Return the clause ``expression COLLATE collation``.

    e.g.::

        collate(mycolumn, 'utf8_bin')

    produces::

        mycolumn COLLATE utf8_bin

    """

    expr = _literal_as_binds(expression)
    return BinaryExpression(
        expr,
        _literal_as_text(collation),
        operators.collate, type_=expr.type)


def _const_expr(element):
    if isinstance(element, (Null, False_, True_)):
        return element
    elif element is None:
        return null()
    elif element is False:
        return false()
    elif element is True:
        return true()
    else:
        raise exc.ArgumentError(
            "Expected None, False, or True"
        )


def _type_from_args(args):
    for a in args:
        if not isinstance(a.type, sqltypes.NullType):
            return a.type
    else:
        return sqltypes.NullType


def _corresponding_column_or_error(fromclause, column,
                                        require_embedded=False):
    c = fromclause.corresponding_column(column,
            require_embedded=require_embedded)
    if c is None:
        raise exc.InvalidRequestError(
                "Given column '%s', attached to table '%s', "
                "failed to locate a corresponding column from table '%s'"
                %
                (column,
                    getattr(column, 'table', None),
                    fromclause.description)
                )
    return c


def and_(*clauses):
    """Join a list of clauses together using the ``AND`` operator.

    The ``&`` operator is also overloaded on all :class:`.ColumnElement`
    subclasses to produce the
    same result.

    """
    if len(clauses) == 1:
        return clauses[0]
    return BooleanClauseList(operator=operators.and_, *clauses)


def or_(*clauses):
    """Join a list of clauses together using the ``OR`` operator.

    The ``|`` operator is also overloaded on all
    :class:`.ColumnElement` subclasses to produce the
    same result.

    """
    if len(clauses) == 1:
        return clauses[0]
    return BooleanClauseList(operator=operators.or_, *clauses)


def not_(clause):
    """Return a negation of the given clause, i.e. ``NOT(clause)``.

    The ``~`` operator is also overloaded on all
    :class:`.ColumnElement` subclasses to produce the
    same result.

    """
    return operators.inv(_literal_as_binds(clause))



class ClauseElement(Visitable):
    """Base class for elements of a programmatically constructed SQL
    expression.

    """
    __visit_name__ = 'clause'

    _annotations = {}
    supports_execution = False
    _from_objects = []
    bind = None
    _is_clone_of = None
    is_selectable = False
    is_clause_element = True

    _order_by_label_element = None

    def _clone(self):
        """Create a shallow copy of this ClauseElement.

        This method may be used by a generative API.  Its also used as
        part of the "deep" copy afforded by a traversal that combines
        the _copy_internals() method.

        """
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        ClauseElement._cloned_set._reset(c)
        ColumnElement.comparator._reset(c)

        # this is a marker that helps to "equate" clauses to each other
        # when a Select returns its list of FROM clauses.  the cloning
        # process leaves around a lot of remnants of the previous clause
        # typically in the form of column expressions still attached to the
        # old table.
        c._is_clone_of = self

        return c

    @property
    def _constructor(self):
        """return the 'constructor' for this ClauseElement.

        This is for the purposes for creating a new object of
        this type.   Usually, its just the element's __class__.
        However, the "Annotated" version of the object overrides
        to return the class of its proxied element.

        """
        return self.__class__

    @util.memoized_property
    def _cloned_set(self):
        """Return the set consisting all cloned ancestors of this
        ClauseElement.

        Includes this ClauseElement.  This accessor tends to be used for
        FromClause objects to identify 'equivalent' FROM clauses, regardless
        of transformative operations.

        """
        s = util.column_set()
        f = self
        while f is not None:
            s.add(f)
            f = f._is_clone_of
        return s

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('_is_clone_of', None)
        return d

    def _annotate(self, values):
        """return a copy of this ClauseElement with annotations
        updated by the given dictionary.

        """
        return annotation.Annotated(self, values)

    def _with_annotations(self, values):
        """return a copy of this ClauseElement with annotations
        replaced by the given dictionary.

        """
        return annotation.Annotated(self, values)

    def _deannotate(self, values=None, clone=False):
        """return a copy of this :class:`.ClauseElement` with annotations
        removed.

        :param values: optional tuple of individual values
         to remove.

        """
        if clone:
            # clone is used when we are also copying
            # the expression for a deep deannotation
            return self._clone()
        else:
            # if no clone, since we have no annotations we return
            # self
            return self

    def unique_params(self, *optionaldict, **kwargs):
        """Return a copy with :func:`bindparam()` elements replaced.

        Same functionality as ``params()``, except adds `unique=True`
        to affected bind parameters so that multiple statements can be
        used.

        """
        return self._params(True, optionaldict, kwargs)

    def params(self, *optionaldict, **kwargs):
        """Return a copy with :func:`bindparam()` elements replaced.

        Returns a copy of this ClauseElement with :func:`bindparam()`
        elements replaced with values taken from the given dictionary::

          >>> clause = column('x') + bindparam('foo')
          >>> print clause.compile().params
          {'foo':None}
          >>> print clause.params({'foo':7}).compile().params
          {'foo':7}

        """
        return self._params(False, optionaldict, kwargs)

    def _params(self, unique, optionaldict, kwargs):
        if len(optionaldict) == 1:
            kwargs.update(optionaldict[0])
        elif len(optionaldict) > 1:
            raise exc.ArgumentError(
                "params() takes zero or one positional dictionary argument")

        def visit_bindparam(bind):
            if bind.key in kwargs:
                bind.value = kwargs[bind.key]
                bind.required = False
            if unique:
                bind._convert_to_unique()
        return cloned_traverse(self, {}, {'bindparam': visit_bindparam})

    def compare(self, other, **kw):
        """Compare this ClauseElement to the given ClauseElement.

        Subclasses should override the default behavior, which is a
        straight identity comparison.

        \**kw are arguments consumed by subclass compare() methods and
        may be used to modify the criteria for comparison.
        (see :class:`.ColumnElement`)

        """
        return self is other

    def _copy_internals(self, clone=_clone, **kw):
        """Reassign internal elements to be clones of themselves.

        Called during a copy-and-traverse operation on newly
        shallow-copied elements to create a deep copy.

        The given clone function should be used, which may be applying
        additional transformations to the element (i.e. replacement
        traversal, cloned traversal, annotations).

        """
        pass

    def get_children(self, **kwargs):
        """Return immediate child elements of this :class:`.ClauseElement`.

        This is used for visit traversal.

        \**kwargs may contain flags that change the collection that is
        returned, for example to return a subset of items in order to
        cut down on larger traversals, or to return child items from a
        different context (such as schema-level collections instead of
        clause-level).

        """
        return []

    def self_group(self, against=None):
        """Apply a 'grouping' to this :class:`.ClauseElement`.

        This method is overridden by subclasses to return a
        "grouping" construct, i.e. parenthesis.   In particular
        it's used by "binary" expressions to provide a grouping
        around themselves when placed into a larger expression,
        as well as by :func:`.select` constructs when placed into
        the FROM clause of another :func:`.select`.  (Note that
        subqueries should be normally created using the
        :func:`.Select.alias` method, as many platforms require
        nested SELECT statements to be named).

        As expressions are composed together, the application of
        :meth:`self_group` is automatic - end-user code should never
        need to use this method directly.  Note that SQLAlchemy's
        clause constructs take operator precedence into account -
        so parenthesis might not be needed, for example, in
        an expression like ``x OR (y AND z)`` - AND takes precedence
        over OR.

        The base :meth:`self_group` method of :class:`.ClauseElement`
        just returns self.
        """
        return self

    @util.dependencies("sqlalchemy.engine.default")
    def compile(self, default, bind=None, dialect=None, **kw):
        """Compile this SQL expression.

        The return value is a :class:`~.Compiled` object.
        Calling ``str()`` or ``unicode()`` on the returned value will yield a
        string representation of the result. The
        :class:`~.Compiled` object also can return a
        dictionary of bind parameter names and values
        using the ``params`` accessor.

        :param bind: An ``Engine`` or ``Connection`` from which a
            ``Compiled`` will be acquired. This argument takes precedence over
            this :class:`.ClauseElement`'s bound engine, if any.

        :param column_keys: Used for INSERT and UPDATE statements, a list of
            column names which should be present in the VALUES clause of the
            compiled statement. If ``None``, all columns from the target table
            object are rendered.

        :param dialect: A ``Dialect`` instance from which a ``Compiled``
            will be acquired. This argument takes precedence over the `bind`
            argument as well as this :class:`.ClauseElement`'s bound engine, if
            any.

        :param inline: Used for INSERT statements, for a dialect which does
            not support inline retrieval of newly generated primary key
            columns, will force the expression used to create the new primary
            key value to be rendered inline within the INSERT statement's
            VALUES clause. This typically refers to Sequence execution but may
            also refer to any server-side default generation function
            associated with a primary key `Column`.

        """

        if not dialect:
            if bind:
                dialect = bind.dialect
            elif self.bind:
                dialect = self.bind.dialect
                bind = self.bind
            else:
                dialect = default.DefaultDialect()
        return self._compiler(dialect, bind=bind, **kw)

    def _compiler(self, dialect, **kw):
        """Return a compiler appropriate for this ClauseElement, given a
        Dialect."""

        return dialect.statement_compiler(dialect, self, **kw)

    def __str__(self):
        if util.py3k:
            return str(self.compile())
        else:
            return unicode(self.compile()).encode('ascii', 'backslashreplace')

    def __and__(self, other):
        return and_(self, other)

    def __or__(self, other):
        return or_(self, other)

    def __invert__(self):
        return self._negate()

    def __bool__(self):
        raise TypeError("Boolean value of this clause is not defined")

    __nonzero__ = __bool__

    def _negate(self):
        if hasattr(self, 'negation_clause'):
            return self.negation_clause
        else:
            return UnaryExpression(
                        self.self_group(against=operators.inv),
                        operator=operators.inv,
                        negate=None)

    def __repr__(self):
        friendly = getattr(self, 'description', None)
        if friendly is None:
            return object.__repr__(self)
        else:
            return '<%s.%s at 0x%x; %s>' % (
                self.__module__, self.__class__.__name__, id(self), friendly)

inspection._self_inspects(ClauseElement)



class _DefaultColumnComparator(operators.ColumnOperators):
    """Defines comparison and math operations.

    See :class:`.ColumnOperators` and :class:`.Operators` for descriptions
    of all operations.

    """

    @util.memoized_property
    def type(self):
        return self.expr.type

    def operate(self, op, *other, **kwargs):
        o = self.operators[op.__name__]
        return o[0](self, self.expr, op, *(other + o[1:]), **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        o = self.operators[op.__name__]
        return o[0](self, self.expr, op, other, reverse=True, *o[1:], **kwargs)

    def _adapt_expression(self, op, other_comparator):
        """evaluate the return type of <self> <op> <othertype>,
        and apply any adaptations to the given operator.

        This method determines the type of a resulting binary expression
        given two source types and an operator.   For example, two
        :class:`.Column` objects, both of the type :class:`.Integer`, will
        produce a :class:`.BinaryExpression` that also has the type
        :class:`.Integer` when compared via the addition (``+``) operator.
        However, using the addition operator with an :class:`.Integer`
        and a :class:`.Date` object will produce a :class:`.Date`, assuming
        "days delta" behavior by the database (in reality, most databases
        other than Postgresql don't accept this particular operation).

        The method returns a tuple of the form <operator>, <type>.
        The resulting operator and type will be those applied to the
        resulting :class:`.BinaryExpression` as the final operator and the
        right-hand side of the expression.

        Note that only a subset of operators make usage of
        :meth:`._adapt_expression`,
        including math operators and user-defined operators, but not
        boolean comparison or special SQL keywords like MATCH or BETWEEN.

        """
        return op, other_comparator.type

    def _boolean_compare(self, expr, op, obj, negate=None, reverse=False,
                        _python_is_types=(util.NoneType, bool),
                        **kwargs):

        if isinstance(obj, _python_is_types + (Null, True_, False_)):

            # allow x ==/!= True/False to be treated as a literal.
            # this comes out to "== / != true/false" or "1/0" if those
            # constants aren't supported and works on all platforms
            if op in (operators.eq, operators.ne) and \
                    isinstance(obj, (bool, True_, False_)):
                return BinaryExpression(expr,
                                obj,
                                op,
                                type_=sqltypes.BOOLEANTYPE,
                                negate=negate, modifiers=kwargs)
            else:
                # all other None/True/False uses IS, IS NOT
                if op in (operators.eq, operators.is_):
                    return BinaryExpression(expr, _const_expr(obj),
                            operators.is_,
                            negate=operators.isnot)
                elif op in (operators.ne, operators.isnot):
                    return BinaryExpression(expr, _const_expr(obj),
                            operators.isnot,
                            negate=operators.is_)
                else:
                    raise exc.ArgumentError(
                        "Only '=', '!=', 'is_()', 'isnot()' operators can "
                        "be used with None/True/False")
        else:
            obj = self._check_literal(expr, op, obj)

        if reverse:
            return BinaryExpression(obj,
                            expr,
                            op,
                            type_=sqltypes.BOOLEANTYPE,
                            negate=negate, modifiers=kwargs)
        else:
            return BinaryExpression(expr,
                            obj,
                            op,
                            type_=sqltypes.BOOLEANTYPE,
                            negate=negate, modifiers=kwargs)

    def _binary_operate(self, expr, op, obj, reverse=False, result_type=None,
                            **kw):
        obj = self._check_literal(expr, op, obj)

        if reverse:
            left, right = obj, expr
        else:
            left, right = expr, obj

        if result_type is None:
            op, result_type = left.comparator._adapt_expression(
                                                op, right.comparator)

        return BinaryExpression(left, right, op, type_=result_type)

    def _scalar(self, expr, op, fn, **kw):
        return fn(expr)

    def _in_impl(self, expr, op, seq_or_selectable, negate_op, **kw):
        seq_or_selectable = _clause_element_as_expr(seq_or_selectable)

        if isinstance(seq_or_selectable, ScalarSelect):
            return self._boolean_compare(expr, op, seq_or_selectable,
                                  negate=negate_op)
        elif isinstance(seq_or_selectable, SelectBase):

            # TODO: if we ever want to support (x, y, z) IN (select x,
            # y, z from table), we would need a multi-column version of
            # as_scalar() to produce a multi- column selectable that
            # does not export itself as a FROM clause

            return self._boolean_compare(
                expr, op, seq_or_selectable.as_scalar(),
                negate=negate_op, **kw)
        elif isinstance(seq_or_selectable, (Selectable, TextClause)):
            return self._boolean_compare(expr, op, seq_or_selectable,
                                  negate=negate_op, **kw)

        # Handle non selectable arguments as sequences
        args = []
        for o in seq_or_selectable:
            if not _is_literal(o):
                if not isinstance(o, ColumnOperators):
                    raise exc.InvalidRequestError('in() function accept'
                            's either a list of non-selectable values, '
                            'or a selectable: %r' % o)
            elif o is None:
                o = null()
            else:
                o = expr._bind_param(op, o)
            args.append(o)
        if len(args) == 0:

            # Special case handling for empty IN's, behave like
            # comparison against zero row selectable.  We use != to
            # build the contradiction as it handles NULL values
            # appropriately, i.e. "not (x IN ())" should not return NULL
            # values for x.

            util.warn('The IN-predicate on "%s" was invoked with an '
                      'empty sequence. This results in a '
                      'contradiction, which nonetheless can be '
                      'expensive to evaluate.  Consider alternative '
                      'strategies for improved performance.' % expr)
            return expr != expr

        return self._boolean_compare(expr, op,
                              ClauseList(*args).self_group(against=op),
                              negate=negate_op)

    def _unsupported_impl(self, expr, op, *arg, **kw):
        raise NotImplementedError("Operator '%s' is not supported on "
                            "this expression" % op.__name__)

    def _neg_impl(self, expr, op, **kw):
        """See :meth:`.ColumnOperators.__neg__`."""
        return UnaryExpression(expr, operator=operators.neg)

    def _match_impl(self, expr, op, other, **kw):
        """See :meth:`.ColumnOperators.match`."""
        return self._boolean_compare(expr, operators.match_op,
                              self._check_literal(expr, operators.match_op,
                              other))

    def _distinct_impl(self, expr, op, **kw):
        """See :meth:`.ColumnOperators.distinct`."""
        return UnaryExpression(expr, operator=operators.distinct_op,
                                type_=expr.type)

    def _between_impl(self, expr, op, cleft, cright, **kw):
        """See :meth:`.ColumnOperators.between`."""
        return BinaryExpression(
                expr,
                ClauseList(
                    self._check_literal(expr, operators.and_, cleft),
                    self._check_literal(expr, operators.and_, cright),
                    operator=operators.and_,
                    group=False),
                operators.between_op)

    def _collate_impl(self, expr, op, other, **kw):
        return collate(expr, other)

    # a mapping of operators with the method they use, along with
    # their negated operator for comparison operators
    operators = {
        "add": (_binary_operate,),
        "mul": (_binary_operate,),
        "sub": (_binary_operate,),
        "div": (_binary_operate,),
        "mod": (_binary_operate,),
        "truediv": (_binary_operate,),
        "custom_op": (_binary_operate,),
        "concat_op": (_binary_operate,),
        "lt": (_boolean_compare, operators.ge),
        "le": (_boolean_compare, operators.gt),
        "ne": (_boolean_compare, operators.eq),
        "gt": (_boolean_compare, operators.le),
        "ge": (_boolean_compare, operators.lt),
        "eq": (_boolean_compare, operators.ne),
        "like_op": (_boolean_compare, operators.notlike_op),
        "ilike_op": (_boolean_compare, operators.notilike_op),
        "notlike_op": (_boolean_compare, operators.like_op),
        "notilike_op": (_boolean_compare, operators.ilike_op),
        "contains_op": (_boolean_compare, operators.notcontains_op),
        "startswith_op": (_boolean_compare, operators.notstartswith_op),
        "endswith_op": (_boolean_compare, operators.notendswith_op),
        "desc_op": (_scalar, desc),
        "asc_op": (_scalar, asc),
        "nullsfirst_op": (_scalar, nullsfirst),
        "nullslast_op": (_scalar, nullslast),
        "in_op": (_in_impl, operators.notin_op),
        "notin_op": (_in_impl, operators.in_op),
        "is_": (_boolean_compare, operators.is_),
        "isnot": (_boolean_compare, operators.isnot),
        "collate": (_collate_impl,),
        "match_op": (_match_impl,),
        "distinct_op": (_distinct_impl,),
        "between_op": (_between_impl, ),
        "neg": (_neg_impl,),
        "getitem": (_unsupported_impl,),
        "lshift": (_unsupported_impl,),
        "rshift": (_unsupported_impl,),
    }

    def _check_literal(self, expr, operator, other):
        if isinstance(other, (ColumnElement, TextClause)):
            if isinstance(other, BindParameter) and \
                    isinstance(other.type, sqltypes.NullType):
                # TODO: perhaps we should not mutate the incoming
                # bindparam() here and instead make a copy of it.
                # this might be the only place that we're mutating
                # an incoming construct.
                other.type = expr.type
            return other
        elif hasattr(other, '__clause_element__'):
            other = other.__clause_element__()
        elif isinstance(other, sqltypes.TypeEngine.Comparator):
            other = other.expr

        if isinstance(other, (SelectBase, Alias)):
            return other.as_scalar()
        elif not isinstance(other, (ColumnElement, TextClause)):
            return expr._bind_param(operator, other)
        else:
            return other


class ColumnElement(ClauseElement, operators.ColumnOperators):
    """Represent a column-oriented SQL expression suitable for usage in the
    "columns" clause, WHERE clause etc. of a statement.

    While the most familiar kind of :class:`.ColumnElement` is the
    :class:`.Column` object, :class:`.ColumnElement` serves as the basis
    for any unit that may be present in a SQL expression, including
    the expressions themselves, SQL functions, bound parameters,
    literal expressions, keywords such as ``NULL``, etc.
    :class:`.ColumnElement` is the ultimate base class for all such elements.

    A :class:`.ColumnElement` provides the ability to generate new
    :class:`.ColumnElement`
    objects using Python expressions.  This means that Python operators
    such as ``==``, ``!=`` and ``<`` are overloaded to mimic SQL operations,
    and allow the instantiation of further :class:`.ColumnElement` instances
    which are composed from other, more fundamental :class:`.ColumnElement`
    objects.  For example, two :class:`.ColumnClause` objects can be added
    together with the addition operator ``+`` to produce
    a :class:`.BinaryExpression`.
    Both :class:`.ColumnClause` and :class:`.BinaryExpression` are subclasses
    of :class:`.ColumnElement`::

        >>> from sqlalchemy.sql import column
        >>> column('a') + column('b')
        <sqlalchemy.sql.expression.BinaryExpression object at 0x101029dd0>
        >>> print column('a') + column('b')
        a + b

    :class:`.ColumnElement` supports the ability to be a *proxy* element,
    which indicates that the :class:`.ColumnElement` may be associated with
    a :class:`.Selectable` which was derived from another :class:`.Selectable`.
    An example of a "derived" :class:`.Selectable` is an :class:`.Alias` of a
    :class:`~sqlalchemy.schema.Table`.  For the ambitious, an in-depth
    discussion of this concept can be found at
    `Expression Transformations <http://techspot.zzzeek.org/2008/01/23/expression-transformations/>`_.

    """

    __visit_name__ = 'column'
    primary_key = False
    foreign_keys = []
    quote = None
    _label = None
    _key_label = None
    _alt_names = ()

    @util.memoized_property
    def type(self):
        return sqltypes.NULLTYPE

    @util.memoized_property
    def comparator(self):
        return self.type.comparator_factory(self)

    def __getattr__(self, key):
        try:
            return getattr(self.comparator, key)
        except AttributeError:
            raise AttributeError(
                    'Neither %r object nor %r object has an attribute %r' % (
                    type(self).__name__,
                    type(self.comparator).__name__,
                    key)
            )

    def operate(self, op, *other, **kwargs):
        return op(self.comparator, *other, **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        return op(other, self.comparator, **kwargs)

    def _bind_param(self, operator, obj):
        return BindParameter(None, obj,
                                    _compared_to_operator=operator,
                                    _compared_to_type=self.type, unique=True)

    @property
    def expression(self):
        """Return a column expression.

        Part of the inspection interface; returns self.

        """
        return self

    @property
    def _select_iterable(self):
        return (self, )

    @util.memoized_property
    def base_columns(self):
        return util.column_set(c for c in self.proxy_set
                                     if not hasattr(c, '_proxies'))

    @util.memoized_property
    def proxy_set(self):
        s = util.column_set([self])
        if hasattr(self, '_proxies'):
            for c in self._proxies:
                s.update(c.proxy_set)
        return s

    def shares_lineage(self, othercolumn):
        """Return True if the given :class:`.ColumnElement`
        has a common ancestor to this :class:`.ColumnElement`."""

        return bool(self.proxy_set.intersection(othercolumn.proxy_set))

    def _compare_name_for_result(self, other):
        """Return True if the given column element compares to this one
        when targeting within a result row."""

        return hasattr(other, 'name') and hasattr(self, 'name') and \
                other.name == self.name

    def _make_proxy(self, selectable, name=None, name_is_truncatable=False, **kw):
        """Create a new :class:`.ColumnElement` representing this
        :class:`.ColumnElement` as it appears in the select list of a
        descending selectable.

        """
        if name is None:
            name = self.anon_label
            try:
                key = str(self)
            except exc.UnsupportedCompilationError:
                key = self.anon_label
        else:
            key = name
        co = ColumnClause(_as_truncated(name) if name_is_truncatable else name,
                            selectable,
                            type_=getattr(self,
                          'type', None))
        co._proxies = [self]
        if selectable._is_clone_of is not None:
            co._is_clone_of = \
                selectable._is_clone_of.columns.get(key)
        selectable._columns[key] = co
        return co

    def compare(self, other, use_proxies=False, equivalents=None, **kw):
        """Compare this ColumnElement to another.

        Special arguments understood:

        :param use_proxies: when True, consider two columns that
          share a common base column as equivalent (i.e. shares_lineage())

        :param equivalents: a dictionary of columns as keys mapped to sets
          of columns. If the given "other" column is present in this
          dictionary, if any of the columns in the corresponding set() pass the
          comparison test, the result is True. This is used to expand the
          comparison to other columns that may be known to be equivalent to
          this one via foreign key or other criterion.

        """
        to_compare = (other, )
        if equivalents and other in equivalents:
            to_compare = equivalents[other].union(to_compare)

        for oth in to_compare:
            if use_proxies and self.shares_lineage(oth):
                return True
            elif hash(oth) == hash(self):
                return True
        else:
            return False

    def label(self, name):
        """Produce a column label, i.e. ``<columnname> AS <name>``.

        This is a shortcut to the :func:`~.expression.label` function.

        if 'name' is None, an anonymous label name will be generated.

        """
        return Label(name, self, self.type)

    @util.memoized_property
    def anon_label(self):
        """provides a constant 'anonymous label' for this ColumnElement.

        This is a label() expression which will be named at compile time.
        The same label() is returned each time anon_label is called so
        that expressions can reference anon_label multiple times, producing
        the same label name at compile time.

        the compiler uses this function automatically at compile time
        for expressions that are known to be 'unnamed' like binary
        expressions and function calls.

        """
        return _anonymous_label('%%(%d %s)s' % (id(self), getattr(self,
                                'name', 'anon')))



class BindParameter(ColumnElement):
    """Represent a bind parameter.

    Public constructor is the :func:`bindparam()` function.

    """

    __visit_name__ = 'bindparam'
    quote = None

    _is_crud = False

    def __init__(self, key, value, type_=None, unique=False,
                            callable_=None,
                            isoutparam=False, required=False,
                            quote=None,
                            _compared_to_operator=None,
                            _compared_to_type=None):
        """Construct a BindParameter.

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
          if True, the key name of this BindParameter will be
          modified if another :class:`BindParameter` of the same name
          already has been located within the containing
          :class:`.ClauseElement`.

        :param quote:
          True if this parameter name requires quoting and is not
          currently known as a SQLAlchemy reserved word; this currently
          only applies to the Oracle backend.

        :param required:
          a value is required at execution time.

        :param isoutparam:
          if True, the parameter should be treated like a stored procedure
          "OUT" parameter.

        """
        if unique:
            self.key = _anonymous_label('%%(%d %s)s' % (id(self), key
                    or 'param'))
        else:
            self.key = key or _anonymous_label('%%(%d param)s'
                    % id(self))

        # identifying key that won't change across
        # clones, used to identify the bind's logical
        # identity
        self._identifying_key = self.key

        # key that was passed in the first place, used to
        # generate new keys
        self._orig_key = key or 'param'

        self.unique = unique
        self.value = value
        self.callable = callable_
        self.isoutparam = isoutparam
        self.required = required
        self.quote = quote
        if type_ is None:
            if _compared_to_type is not None:
                self.type = \
                    _compared_to_type.coerce_compared_value(
                        _compared_to_operator, value)
            else:
                self.type = sqltypes._type_map.get(type(value),
                        sqltypes.NULLTYPE)
        elif isinstance(type_, type):
            self.type = type_()
        else:
            self.type = type_

    @property
    def effective_value(self):
        """Return the value of this bound parameter,
        taking into account if the ``callable`` parameter
        was set.

        The ``callable`` value will be evaluated
        and returned if present, else ``value``.

        """
        if self.callable:
            return self.callable()
        else:
            return self.value

    def _clone(self):
        c = ClauseElement._clone(self)
        if self.unique:
            c.key = _anonymous_label('%%(%d %s)s' % (id(c), c._orig_key
                    or 'param'))
        return c

    def _convert_to_unique(self):
        if not self.unique:
            self.unique = True
            self.key = _anonymous_label('%%(%d %s)s' % (id(self),
                    self._orig_key or 'param'))

    def compare(self, other, **kw):
        """Compare this :class:`BindParameter` to the given
        clause."""

        return isinstance(other, BindParameter) \
            and self.type._compare_type_affinity(other.type) \
            and self.value == other.value

    def __getstate__(self):
        """execute a deferred value for serialization purposes."""

        d = self.__dict__.copy()
        v = self.value
        if self.callable:
            v = self.callable()
            d['callable'] = None
        d['value'] = v
        return d

    def __repr__(self):
        return 'BindParameter(%r, %r, type_=%r)' % (self.key,
                self.value, self.type)


class TypeClause(ClauseElement):
    """Handle a type keyword in a SQL statement.

    Used by the ``Case`` statement.

    """

    __visit_name__ = 'typeclause'

    def __init__(self, type):
        self.type = type


class TextClause(Executable, ClauseElement):
    """Represent a literal SQL text fragment.

    Public constructor is the :func:`text()` function.

    """

    __visit_name__ = 'textclause'

    _bind_params_regex = re.compile(r'(?<![:\w\x5c]):(\w+)(?!:)', re.UNICODE)
    _execution_options = \
        Executable._execution_options.union(
            {'autocommit': PARSE_AUTOCOMMIT})

    @property
    def _select_iterable(self):
        return (self,)

    @property
    def selectable(self):
        return self

    _hide_froms = []

    def __init__(
                    self,
                    text='',
                    bind=None,
                    bindparams=None,
                    typemap=None,
                    autocommit=None):

        self._bind = bind
        self.bindparams = {}
        self.typemap = typemap
        if autocommit is not None:
            util.warn_deprecated('autocommit on text() is deprecated.  '
                                 'Use .execution_options(autocommit=Tru'
                                 'e)')
            self._execution_options = \
                self._execution_options.union(
                    {'autocommit': autocommit})
        if typemap is not None:
            for key in typemap:
                typemap[key] = sqltypes.to_instance(typemap[key])

        def repl(m):
            self.bindparams[m.group(1)] = bindparam(m.group(1))
            return ':%s' % m.group(1)

        # scan the string and search for bind parameter names, add them
        # to the list of bindparams

        self.text = self._bind_params_regex.sub(repl, text)
        if bindparams is not None:
            for b in bindparams:
                self.bindparams[b.key] = b

    @property
    def type(self):
        if self.typemap is not None and len(self.typemap) == 1:
            return list(self.typemap)[0]
        else:
            return sqltypes.NULLTYPE

    @property
    def comparator(self):
        return self.type.comparator_factory(self)

    def self_group(self, against=None):
        if against is operators.in_op:
            return Grouping(self)
        else:
            return self

    def _copy_internals(self, clone=_clone, **kw):
        self.bindparams = dict((b.key, clone(b, **kw))
                               for b in self.bindparams.values())

    def get_children(self, **kwargs):
        return list(self.bindparams.values())


class Null(ColumnElement):
    """Represent the NULL keyword in a SQL statement.

    Public constructor is the :func:`null()` function.

    """

    __visit_name__ = 'null'

    def __init__(self):
        self.type = sqltypes.NULLTYPE

    def compare(self, other):
        return isinstance(other, Null)


class False_(ColumnElement):
    """Represent the ``false`` keyword in a SQL statement.

    Public constructor is the :func:`false()` function.

    """

    __visit_name__ = 'false'

    def __init__(self):
        self.type = sqltypes.BOOLEANTYPE

    def compare(self, other):
        return isinstance(other, False_)

class True_(ColumnElement):
    """Represent the ``true`` keyword in a SQL statement.

    Public constructor is the :func:`true()` function.

    """

    __visit_name__ = 'true'

    def __init__(self):
        self.type = sqltypes.BOOLEANTYPE

    def compare(self, other):
        return isinstance(other, True_)


class ClauseList(ClauseElement):
    """Describe a list of clauses, separated by an operator.

    By default, is comma-separated, such as a column listing.

    """
    __visit_name__ = 'clauselist'

    def __init__(self, *clauses, **kwargs):
        self.operator = kwargs.pop('operator', operators.comma_op)
        self.group = kwargs.pop('group', True)
        self.group_contents = kwargs.pop('group_contents', True)
        if self.group_contents:
            self.clauses = [
                _literal_as_text(clause).self_group(against=self.operator)
                for clause in clauses if clause is not None]
        else:
            self.clauses = [
                _literal_as_text(clause)
                for clause in clauses if clause is not None]

    def __iter__(self):
        return iter(self.clauses)

    def __len__(self):
        return len(self.clauses)

    @property
    def _select_iterable(self):
        return iter(self)

    def append(self, clause):
        # TODO: not sure if i like the 'group_contents' flag.  need to
        # define the difference between a ClauseList of ClauseLists,
        # and a "flattened" ClauseList of ClauseLists.  flatten()
        # method ?
        if self.group_contents:
            self.clauses.append(_literal_as_text(clause).\
                                self_group(against=self.operator))
        else:
            self.clauses.append(_literal_as_text(clause))

    def _copy_internals(self, clone=_clone, **kw):
        self.clauses = [clone(clause, **kw) for clause in self.clauses]

    def get_children(self, **kwargs):
        return self.clauses

    @property
    def _from_objects(self):
        return list(itertools.chain(*[c._from_objects for c in self.clauses]))

    def self_group(self, against=None):
        if self.group and operators.is_precedent(self.operator, against):
            return Grouping(self)
        else:
            return self

    def compare(self, other, **kw):
        """Compare this :class:`.ClauseList` to the given :class:`.ClauseList`,
        including a comparison of all the clause items.

        """
        if not isinstance(other, ClauseList) and len(self.clauses) == 1:
            return self.clauses[0].compare(other, **kw)
        elif isinstance(other, ClauseList) and \
                len(self.clauses) == len(other.clauses):
            for i in range(0, len(self.clauses)):
                if not self.clauses[i].compare(other.clauses[i], **kw):
                    return False
            else:
                return self.operator == other.operator
        else:
            return False


class BooleanClauseList(ClauseList, ColumnElement):
    __visit_name__ = 'clauselist'

    def __init__(self, *clauses, **kwargs):
        super(BooleanClauseList, self).__init__(*clauses, **kwargs)
        self.type = sqltypes.to_instance(kwargs.get('type_',
                sqltypes.Boolean))

    @property
    def _select_iterable(self):
        return (self, )

    def self_group(self, against=None):
        if not self.clauses:
            return self
        else:
            return super(BooleanClauseList, self).self_group(against=against)


class Tuple(ClauseList, ColumnElement):

    def __init__(self, *clauses, **kw):
        clauses = [_literal_as_binds(c) for c in clauses]
        self.type = kw.pop('type_', None)
        if self.type is None:
            self.type = _type_from_args(clauses)
        super(Tuple, self).__init__(*clauses, **kw)

    @property
    def _select_iterable(self):
        return (self, )

    def _bind_param(self, operator, obj):
        return Tuple(*[
            BindParameter(None, o, _compared_to_operator=operator,
                             _compared_to_type=self.type, unique=True)
            for o in obj
        ]).self_group()


class Case(ColumnElement):
    __visit_name__ = 'case'

    def __init__(self, whens, value=None, else_=None):
        try:
            whens = util.dictlike_iteritems(whens)
        except TypeError:
            pass

        if value is not None:
            whenlist = [
                (_literal_as_binds(c).self_group(),
                _literal_as_binds(r)) for (c, r) in whens
            ]
        else:
            whenlist = [
                (_no_literals(c).self_group(),
                _literal_as_binds(r)) for (c, r) in whens
            ]

        if whenlist:
            type_ = list(whenlist[-1])[-1].type
        else:
            type_ = None

        if value is None:
            self.value = None
        else:
            self.value = _literal_as_binds(value)

        self.type = type_
        self.whens = whenlist
        if else_ is not None:
            self.else_ = _literal_as_binds(else_)
        else:
            self.else_ = None

    def _copy_internals(self, clone=_clone, **kw):
        if self.value is not None:
            self.value = clone(self.value, **kw)
        self.whens = [(clone(x, **kw), clone(y, **kw))
                            for x, y in self.whens]
        if self.else_ is not None:
            self.else_ = clone(self.else_, **kw)

    def get_children(self, **kwargs):
        if self.value is not None:
            yield self.value
        for x, y in self.whens:
            yield x
            yield y
        if self.else_ is not None:
            yield self.else_

    @property
    def _from_objects(self):
        return list(itertools.chain(*[x._from_objects for x in
                    self.get_children()]))


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

def cast(clause, totype, **kwargs):
    """Return a ``CAST`` function.

    Equivalent of SQL ``CAST(clause AS totype)``.

    Use with a :class:`~sqlalchemy.types.TypeEngine` subclass, i.e::

      cast(table.c.unit_price * table.c.qty, Numeric(10,4))

    or::

      cast(table.c.timestamp, DATE)

    """
    return Cast(clause, totype, **kwargs)


class Cast(ColumnElement):

    __visit_name__ = 'cast'

    def __init__(self, clause, totype, **kwargs):
        self.type = sqltypes.to_instance(totype)
        self.clause = _literal_as_binds(clause, None)
        self.typeclause = TypeClause(self.type)

    def _copy_internals(self, clone=_clone, **kw):
        self.clause = clone(self.clause, **kw)
        self.typeclause = clone(self.typeclause, **kw)

    def get_children(self, **kwargs):
        return self.clause, self.typeclause

    @property
    def _from_objects(self):
        return self.clause._from_objects


def extract(field, expr):
    """Return the clause ``extract(field FROM expr)``."""

    return Extract(field, expr)

class Extract(ColumnElement):

    __visit_name__ = 'extract'

    def __init__(self, field, expr, **kwargs):
        self.type = sqltypes.Integer()
        self.field = field
        self.expr = _literal_as_binds(expr, None)

    def _copy_internals(self, clone=_clone, **kw):
        self.expr = clone(self.expr, **kw)

    def get_children(self, **kwargs):
        return self.expr,

    @property
    def _from_objects(self):
        return self.expr._from_objects


class UnaryExpression(ColumnElement):
    """Define a 'unary' expression.

    A unary expression has a single column expression
    and an operator.  The operator can be placed on the left
    (where it is called the 'operator') or right (where it is called the
    'modifier') of the column expression.

    """
    __visit_name__ = 'unary'

    def __init__(self, element, operator=None, modifier=None,
                            type_=None, negate=None):
        self.operator = operator
        self.modifier = modifier

        self.element = _literal_as_text(element).\
                    self_group(against=self.operator or self.modifier)
        self.type = sqltypes.to_instance(type_)
        self.negate = negate

    @util.memoized_property
    def _order_by_label_element(self):
        if self.modifier in (operators.desc_op, operators.asc_op):
            return self.element._order_by_label_element
        else:
            return None

    @property
    def _from_objects(self):
        return self.element._from_objects

    def _copy_internals(self, clone=_clone, **kw):
        self.element = clone(self.element, **kw)

    def get_children(self, **kwargs):
        return self.element,

    def compare(self, other, **kw):
        """Compare this :class:`UnaryExpression` against the given
        :class:`.ClauseElement`."""

        return (
            isinstance(other, UnaryExpression) and
            self.operator == other.operator and
            self.modifier == other.modifier and
            self.element.compare(other.element, **kw)
        )

    def _negate(self):
        if self.negate is not None:
            return UnaryExpression(
                self.element,
                operator=self.negate,
                negate=self.operator,
                modifier=self.modifier,
                type_=self.type)
        else:
            return super(UnaryExpression, self)._negate()

    def self_group(self, against=None):
        if self.operator and operators.is_precedent(self.operator,
                against):
            return Grouping(self)
        else:
            return self


class BinaryExpression(ColumnElement):
    """Represent an expression that is ``LEFT <operator> RIGHT``.

    A :class:`.BinaryExpression` is generated automatically
    whenever two column expressions are used in a Python binary expresion::

        >>> from sqlalchemy.sql import column
        >>> column('a') + column('b')
        <sqlalchemy.sql.expression.BinaryExpression object at 0x101029dd0>
        >>> print column('a') + column('b')
        a + b

    """

    __visit_name__ = 'binary'

    def __init__(self, left, right, operator, type_=None,
                    negate=None, modifiers=None):
        # allow compatibility with libraries that
        # refer to BinaryExpression directly and pass strings
        if isinstance(operator, util.string_types):
            operator = operators.custom_op(operator)
        self._orig = (left, right)
        self.left = _literal_as_text(left).self_group(against=operator)
        self.right = _literal_as_text(right).self_group(against=operator)
        self.operator = operator
        self.type = sqltypes.to_instance(type_)
        self.negate = negate

        if modifiers is None:
            self.modifiers = {}
        else:
            self.modifiers = modifiers

    def __bool__(self):
        if self.operator in (operator.eq, operator.ne):
            return self.operator(hash(self._orig[0]), hash(self._orig[1]))
        else:
            raise TypeError("Boolean value of this clause is not defined")

    __nonzero__ = __bool__

    @property
    def is_comparison(self):
        return operators.is_comparison(self.operator)

    @property
    def _from_objects(self):
        return self.left._from_objects + self.right._from_objects

    def _copy_internals(self, clone=_clone, **kw):
        self.left = clone(self.left, **kw)
        self.right = clone(self.right, **kw)

    def get_children(self, **kwargs):
        return self.left, self.right

    def compare(self, other, **kw):
        """Compare this :class:`BinaryExpression` against the
        given :class:`BinaryExpression`."""

        return (
            isinstance(other, BinaryExpression) and
            self.operator == other.operator and
            (
                self.left.compare(other.left, **kw) and
                self.right.compare(other.right, **kw) or
                (
                    operators.is_commutative(self.operator) and
                    self.left.compare(other.right, **kw) and
                    self.right.compare(other.left, **kw)
                )
            )
        )

    def self_group(self, against=None):
        if operators.is_precedent(self.operator, against):
            return Grouping(self)
        else:
            return self

    def _negate(self):
        if self.negate is not None:
            return BinaryExpression(
                self.left,
                self.right,
                self.negate,
                negate=self.operator,
                type_=sqltypes.BOOLEANTYPE,
                modifiers=self.modifiers)
        else:
            return super(BinaryExpression, self)._negate()


class Exists(UnaryExpression):
    __visit_name__ = UnaryExpression.__visit_name__
    _from_objects = []

    def __init__(self, *args, **kwargs):
        if args and isinstance(args[0], (SelectBase, ScalarSelect)):
            s = args[0]
        else:
            if not args:
                args = ([literal_column('*')],)
            s = select(*args, **kwargs).as_scalar().self_group()

        UnaryExpression.__init__(self, s, operator=operators.exists,
                                  type_=sqltypes.Boolean)

    def select(self, whereclause=None, **params):
        return select([self], whereclause, **params)

    def correlate(self, *fromclause):
        e = self._clone()
        e.element = self.element.correlate(*fromclause).self_group()
        return e

    def correlate_except(self, *fromclause):
        e = self._clone()
        e.element = self.element.correlate_except(*fromclause).self_group()
        return e

    def select_from(self, clause):
        """return a new :class:`.Exists` construct, applying the given
        expression to the :meth:`.Select.select_from` method of the select
        statement contained.

        """
        e = self._clone()
        e.element = self.element.select_from(clause).self_group()
        return e

    def where(self, clause):
        """return a new exists() construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """
        e = self._clone()
        e.element = self.element.where(clause).self_group()
        return e



class Grouping(ColumnElement):
    """Represent a grouping within a column expression"""

    __visit_name__ = 'grouping'

    def __init__(self, element):
        self.element = element
        self.type = getattr(element, 'type', sqltypes.NULLTYPE)

    @property
    def _label(self):
        return getattr(self.element, '_label', None) or self.anon_label

    def _copy_internals(self, clone=_clone, **kw):
        self.element = clone(self.element, **kw)

    def get_children(self, **kwargs):
        return self.element,

    @property
    def _from_objects(self):
        return self.element._from_objects

    def __getattr__(self, attr):
        return getattr(self.element, attr)

    def __getstate__(self):
        return {'element': self.element, 'type': self.type}

    def __setstate__(self, state):
        self.element = state['element']
        self.type = state['type']

    def compare(self, other, **kw):
        return isinstance(other, Grouping) and \
            self.element.compare(other.element)


class Over(ColumnElement):
    """Represent an OVER clause.

    This is a special operator against a so-called
    "window" function, as well as any aggregate function,
    which produces results relative to the result set
    itself.  It's supported only by certain database
    backends.

    """
    __visit_name__ = 'over'

    order_by = None
    partition_by = None

    def __init__(self, func, partition_by=None, order_by=None):
        self.func = func
        if order_by is not None:
            self.order_by = ClauseList(*util.to_list(order_by))
        if partition_by is not None:
            self.partition_by = ClauseList(*util.to_list(partition_by))

    @util.memoized_property
    def type(self):
        return self.func.type

    def get_children(self, **kwargs):
        return [c for c in
                (self.func, self.partition_by, self.order_by)
                if c is not None]

    def _copy_internals(self, clone=_clone, **kw):
        self.func = clone(self.func, **kw)
        if self.partition_by is not None:
            self.partition_by = clone(self.partition_by, **kw)
        if self.order_by is not None:
            self.order_by = clone(self.order_by, **kw)

    @property
    def _from_objects(self):
        return list(itertools.chain(
            *[c._from_objects for c in
                (self.func, self.partition_by, self.order_by)
            if c is not None]
        ))


class Label(ColumnElement):
    """Represents a column label (AS).

    Represent a label, as typically applied to any column-level
    element using the ``AS`` sql keyword.

    This object is constructed from the :func:`label()` module level
    function as well as the :func:`label()` method available on all
    :class:`.ColumnElement` subclasses.

    """

    __visit_name__ = 'label'

    def __init__(self, name, element, type_=None):
        while isinstance(element, Label):
            element = element.element
        if name:
            self.name = name
        else:
            self.name = _anonymous_label('%%(%d %s)s' % (id(self),
                                getattr(element, 'name', 'anon')))
        self.key = self._label = self._key_label = self.name
        self._element = element
        self._type = type_
        self.quote = element.quote
        self._proxies = [element]

    @util.memoized_property
    def _order_by_label_element(self):
        return self

    @util.memoized_property
    def type(self):
        return sqltypes.to_instance(
                    self._type or getattr(self._element, 'type', None)
                )

    @util.memoized_property
    def element(self):
        return self._element.self_group(against=operators.as_)

    def self_group(self, against=None):
        sub_element = self._element.self_group(against=against)
        if sub_element is not self._element:
            return Label(self.name,
                        sub_element,
                        type_=self._type)
        else:
            return self

    @property
    def primary_key(self):
        return self.element.primary_key

    @property
    def foreign_keys(self):
        return self.element.foreign_keys

    def get_children(self, **kwargs):
        return self.element,

    def _copy_internals(self, clone=_clone, **kw):
        self.element = clone(self.element, **kw)

    @property
    def _from_objects(self):
        return self.element._from_objects

    def _make_proxy(self, selectable, name=None, **kw):
        e = self.element._make_proxy(selectable,
                                name=name if name else self.name)
        e._proxies.append(self)
        if self._type is not None:
            e.type = self._type
        return e


class ColumnClause(Immutable, ColumnElement):
    """Represents a generic column expression from any textual string.

    This includes columns associated with tables, aliases and select
    statements, but also any arbitrary text.  May or may not be bound
    to an underlying :class:`.Selectable`.

    :class:`.ColumnClause` is constructed by itself typically via
    the :func:`~.expression.column` function.  It may be placed directly
    into constructs such as :func:`.select` constructs::

        from sqlalchemy.sql import column, select

        c1, c2 = column("c1"), column("c2")
        s = select([c1, c2]).where(c1==5)

    There is also a variant on :func:`~.expression.column` known
    as :func:`~.expression.literal_column` - the difference is that
    in the latter case, the string value is assumed to be an exact
    expression, rather than a column name, so that no quoting rules
    or similar are applied::

        from sqlalchemy.sql import literal_column, select

        s = select([literal_column("5 + 7")])

    :class:`.ColumnClause` can also be used in a table-like
    fashion by combining the :func:`~.expression.column` function
    with the :func:`~.expression.table` function, to produce
    a "lightweight" form of table metadata::

        from sqlalchemy.sql import table, column

        user = table("user",
                column("id"),
                column("name"),
                column("description"),
        )

    The above construct can be created in an ad-hoc fashion and is
    not associated with any :class:`.schema.MetaData`, unlike it's
    more full fledged :class:`.schema.Table` counterpart.

    :param text: the text of the element.

    :param selectable: parent selectable.

    :param type: :class:`.types.TypeEngine` object which can associate
      this :class:`.ColumnClause` with a type.

    :param is_literal: if True, the :class:`.ColumnClause` is assumed to
      be an exact expression that will be delivered to the output with no
      quoting rules applied regardless of case sensitive settings. the
      :func:`literal_column()` function is usually used to create such a
      :class:`.ColumnClause`.


    """
    __visit_name__ = 'column'

    onupdate = default = server_default = server_onupdate = None

    _memoized_property = util.group_expirable_memoized_property()

    def __init__(self, text, selectable=None, type_=None, is_literal=False):
        self.key = self.name = text
        self.table = selectable
        self.type = sqltypes.to_instance(type_)
        self.is_literal = is_literal

    def _compare_name_for_result(self, other):
        if self.is_literal or \
            self.table is None or \
            not hasattr(other, 'proxy_set') or (
            isinstance(other, ColumnClause) and other.is_literal
        ):
            return super(ColumnClause, self).\
                    _compare_name_for_result(other)
        else:
            return other.proxy_set.intersection(self.proxy_set)

    def _get_table(self):
        return self.__dict__['table']

    def _set_table(self, table):
        self._memoized_property.expire_instance(self)
        self.__dict__['table'] = table
    table = property(_get_table, _set_table)

    @_memoized_property
    def _from_objects(self):
        t = self.table
        if t is not None:
            return [t]
        else:
            return []

    @util.memoized_property
    def description(self):
        if util.py3k:
            return self.name
        else:
            return self.name.encode('ascii', 'backslashreplace')

    @_memoized_property
    def _key_label(self):
        if self.key != self.name:
            return self._gen_label(self.key)
        else:
            return self._label

    @_memoized_property
    def _label(self):
        return self._gen_label(self.name)

    def _gen_label(self, name):
        t = self.table
        if self.is_literal:
            return None

        elif t is not None and t.named_with_column:
            if getattr(t, 'schema', None):
                label = t.schema.replace('.', '_') + "_" + \
                            t.name + "_" + name
            else:
                label = t.name + "_" + name

            # ensure the label name doesn't conflict with that
            # of an existing column
            if label in t.c:
                _label = label
                counter = 1
                while _label in t.c:
                    _label = label + "_" + str(counter)
                    counter += 1
                label = _label

            return _as_truncated(label)

        else:
            return name

    def _bind_param(self, operator, obj):
        return BindParameter(self.name, obj,
                                _compared_to_operator=operator,
                                _compared_to_type=self.type,
                                unique=True)

    def _make_proxy(self, selectable, name=None, attach=True,
                            name_is_truncatable=False, **kw):
        # propagate the "is_literal" flag only if we are keeping our name,
        # otherwise its considered to be a label
        is_literal = self.is_literal and (name is None or name == self.name)
        c = self._constructor(
                    _as_truncated(name or self.name) if \
                                    name_is_truncatable else \
                                    (name or self.name),
                    selectable=selectable,
                    type_=self.type,
                    is_literal=is_literal
                )
        if name is None:
            c.key = self.key
        c._proxies = [self]
        if selectable._is_clone_of is not None:
            c._is_clone_of = \
                selectable._is_clone_of.columns.get(c.key)

        if attach:
            selectable._columns[c.key] = c
        return c



class ScalarSelect(Generative, Grouping):
    _from_objects = []

    def __init__(self, element):
        self.element = element
        self.type = element._scalar_type()

    @property
    def columns(self):
        raise exc.InvalidRequestError('Scalar Select expression has no '
                'columns; use this object directly within a '
                'column-level expression.')
    c = columns

    @_generative
    def where(self, crit):
        """Apply a WHERE clause to the SELECT statement referred to
        by this :class:`.ScalarSelect`.

        """
        self.element = self.element.where(crit)

    def self_group(self, **kwargs):
        return self

class _IdentifiedClause(Executable, ClauseElement):

    __visit_name__ = 'identified'
    _execution_options = \
        Executable._execution_options.union({'autocommit': False})
    quote = None

    def __init__(self, ident):
        self.ident = ident


class SavepointClause(_IdentifiedClause):
    __visit_name__ = 'savepoint'


class RollbackToSavepointClause(_IdentifiedClause):
    __visit_name__ = 'rollback_to_savepoint'


class ReleaseSavepointClause(_IdentifiedClause):
    __visit_name__ = 'release_savepoint'


class AnnotatedColumnElement(Annotated):
    def __init__(self, element, values):
        ColumnElement.comparator._reset(self)
        Annotated.__init__(self, element, values)
        for attr in ('name', 'key'):
            if self.__dict__.get(attr, False) is None:
                self.__dict__.pop(attr)

    def _with_annotations(self, values):
        clone = super(AnnotatedColumnElement, self)._with_annotations(values)
        ColumnElement.comparator._reset(clone)
        return clone

    @util.memoized_property
    def name(self):
        """pull 'name' from parent, if not present"""
        return self._Annotated__element.name

    @util.memoized_property
    def key(self):
        """pull 'key' from parent, if not present"""
        return self._Annotated__element.key

    @util.memoized_property
    def info(self):
        return self._Annotated__element.info


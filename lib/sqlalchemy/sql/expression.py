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


from .visitors import Visitable
from .functions import func, modifier, FunctionElement
from ..util.langhelpers import public_factory
from .elements import ClauseElement, ColumnElement,\
  BindParameter, UnaryExpression, BooleanClauseList, Exists,\
  Label, Cast, Case, ColumnClause, TextClause, Over, Null, \
  True_, False_, BinaryExpression, Tuple, TypeClause, Extract, \
  Grouping, ScalarSelect, and_, or_, not_, null, false, true, \
  collate, asc, desc, cast, extract, literal_column, between,\
  case, distinct, exists, label, literal, outparam, \
  tuple_, type_coerce

from .base import ColumnCollection, Generative, Executable

from .selectable import Alias, Join, Select, Selectable, TableClause, \
        CompoundSelect, FromClause, FromGrouping, SelectBase, \
        alias, except_, except_all, intersect, intersect_all, \
        subquery, union, union_all

from .dml import Insert, Update, Delete, insert, update, delete


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


bindparam = public_factory(BindParameter)
select = public_factory(Select)
text = public_factory(TextClause)
table = public_factory(TableClause)
column = public_factory(ColumnClause)
over = public_factory(Over)

join = public_factory(Join._create_join)
outerjoin = public_factory(Join._create_outerjoin)

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

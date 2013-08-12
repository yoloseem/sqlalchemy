# sql/expression.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines the public namespace for SQL expression constructs.

"""

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


from .visitors import Visitable
from .functions import func, modifier, FunctionElement
from ..util.langhelpers import public_factory
from .elements import ClauseElement, ColumnElement,\
  BindParameter, UnaryExpression, BooleanClauseList, \
  Label, Cast, Case, ColumnClause, TextClause, Over, Null, \
  True_, False_, BinaryExpression, Tuple, TypeClause, Extract, \
  Grouping, and_, or_, not_, null, false, true, \
  collate, cast, extract, literal_column, between,\
  case, label, literal, outparam, \
  tuple_, type_coerce, ClauseList

from .elements import _literal_as_text, _clause_element_as_expr,\
  is_column, _labeled, _only_column_elements, _string_or_unprintable, \
    _truncated_label, _clone

from .base import ColumnCollection, Generative, Executable, PARSE_AUTOCOMMIT, _from_objects

from .selectable import Alias, Join, Select, Selectable, TableClause, \
        CompoundSelect, FromClause, FromGrouping, SelectBase, \
        alias, except_, except_all, intersect, intersect_all, \
        subquery, union, union_all, HasPrefixes, Exists, ScalarSelect

from .dml import Insert, Update, Delete

from .selectable import _interpret_as_from


bindparam = public_factory(BindParameter)
select = public_factory(Select)
text = public_factory(TextClause)
table = public_factory(TableClause)
column = public_factory(ColumnClause)
over = public_factory(Over)

exists = public_factory(Exists)
nullsfirst = public_factory(UnaryExpression._create_nullsfirst)
nullslast = public_factory(UnaryExpression._create_nullslast)
asc = public_factory(UnaryExpression._create_asc)
desc = public_factory(UnaryExpression._create_desc)
distinct = public_factory(UnaryExpression._create_distinct)

join = public_factory(Join._create_join)
outerjoin = public_factory(Join._create_outerjoin)

insert = public_factory(Insert)
update = public_factory(Update)
delete = public_factory(Delete)



# old names for compatibility
_Executable = Executable
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

# sql/expression.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines the public namespace for SQL expression constructs.

"""


from .visitors import Visitable
from .functions import func, modifier, FunctionElement
from ..util.langhelpers import public_factory
from .elements import ClauseElement, ColumnElement,\
  BindParameter, UnaryExpression, BooleanClauseList, Exists,\
  Label, Cast, Case, ColumnClause, TextClause, Over, Null, \
  True_, False_, BinaryExpression, Tuple, TypeClause, Extract, \
  Grouping, ScalarSelect, and_, or_, not_, null, false, true, \
  collate, cast, extract, literal_column, between,\
  case, exists, label, literal, outparam, \
  tuple_, type_coerce

from .base import ColumnCollection, Generative, Executable

from .selectable import Alias, Join, Select, Selectable, TableClause, \
        CompoundSelect, FromClause, FromGrouping, SelectBase, \
        alias, except_, except_all, intersect, intersect_all, \
        subquery, union, union_all

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


bindparam = public_factory(BindParameter)
select = public_factory(Select)
text = public_factory(TextClause)
table = public_factory(TableClause)
column = public_factory(ColumnClause)
over = public_factory(Over)

nullsfirst = public_factory(UnaryExpression.nullsfirst)
nullslast = public_factory(UnaryExpression.nullslast)
asc = public_factory(UnaryExpression.asc)
desc = public_factory(UnaryExpression.desc)
distinct = public_factory(UnaryExpression.distinct)

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

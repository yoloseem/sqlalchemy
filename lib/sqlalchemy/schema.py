# schema.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Compatiblity namespace for sqlalchemy.sql.schema and related.

"""


from .sql.schema import (
    CheckConstraint,
    Column,
    ColumnDefault,
    Constraint,
    DefaultClause,
    FetchedValue,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    MetaData,
    PassiveDefault,
    PrimaryKeyConstraint,
    Sequence,
    Table,
    ThreadLocalMetaData,
    UniqueConstraint,
    _get_table_key,
    ColumnCollectionConstraint
    )


from .sql.ddl import (
    DDL,
    _CreateDropBase
)

# sqlalchemy/__init__.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import inspect as _inspect
import sys

from .sql import (
    alias,
    and_,
    asc,
    between,
    bindparam,
    case,
    cast,
    collate,
    delete,
    desc,
    distinct,
    except_,
    except_all,
    exists,
    extract,
    func,
    insert,
    intersect,
    intersect_all,
    join,
    literal,
    literal_column,
    modifier,
    not_,
    null,
    or_,
    outerjoin,
    outparam,
    over,
    select,
    subquery,
    text,
    tuple_,
    type_coerce,
    union,
    union_all,
    update,
    )

from .types import *

from .schema import *

from .inspection import inspect

from .engine import create_engine, engine_from_config


__all__ = sorted(name for name, obj in locals().items()
                 if not (name.startswith('_') or _inspect.ismodule(obj)))

__version__ = '0.9.0'

del _inspect, sys

from . import util as _sa_util
_sa_util.importlater.resolve_all("sqlalchemy")
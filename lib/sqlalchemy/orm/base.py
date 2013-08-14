# orm/base.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Constants and rudimental functions used throughout the ORM.

"""

from .. import util, inspection, exc as sa_exc
from . import exc
import operator

PASSIVE_NO_RESULT = util.symbol('PASSIVE_NO_RESULT',
"""Symbol returned by a loader callable or other attribute/history
retrieval operation when a value could not be determined, based
on loader callable flags.
"""
)

ATTR_WAS_SET = util.symbol('ATTR_WAS_SET',
"""Symbol returned by a loader callable to indicate the
retrieved value, or values, were assigned to their attributes
on the target object.
""")

ATTR_EMPTY = util.symbol('ATTR_EMPTY',
"""Symbol used internally to indicate an attribute had no callable.
""")

NO_VALUE = util.symbol('NO_VALUE',
"""Symbol which may be placed as the 'previous' value of an attribute,
indicating no value was loaded for an attribute when it was modified,
and flags indicated we were not to load it.
"""
)

NEVER_SET = util.symbol('NEVER_SET',
"""Symbol which may be placed as the 'previous' value of an attribute
indicating that the attribute had not been assigned to previously.
"""
)

NO_CHANGE = util.symbol("NO_CHANGE",
"""No callables or SQL should be emitted on attribute access
and no state should change""", canonical=0
)

CALLABLES_OK = util.symbol("CALLABLES_OK",
"""Loader callables can be fired off if a value
is not present.""", canonical=1
)

SQL_OK = util.symbol("SQL_OK",
"""Loader callables can emit SQL at least on scalar value
attributes.""", canonical=2)

RELATED_OBJECT_OK = util.symbol("RELATED_OBJECT_OK",
"""callables can use SQL to load related objects as well
as scalar value attributes.
""", canonical=4
)

INIT_OK = util.symbol("INIT_OK",
"""Attributes should be initialized with a blank
value (None or an empty collection) upon get, if no other
value can be obtained.
""", canonical=8
)

NON_PERSISTENT_OK = util.symbol("NON_PERSISTENT_OK",
"""callables can be emitted if the parent is not persistent.""",
canonical=16
)

LOAD_AGAINST_COMMITTED = util.symbol("LOAD_AGAINST_COMMITTED",
"""callables should use committed values as primary/foreign keys during a load
""", canonical=32
)

# pre-packaged sets of flags used as inputs
PASSIVE_OFF = util.symbol("PASSIVE_OFF",
    "Callables can be emitted in all cases.",
    canonical=(RELATED_OBJECT_OK | NON_PERSISTENT_OK |
                    INIT_OK | CALLABLES_OK | SQL_OK)
)
PASSIVE_RETURN_NEVER_SET = util.symbol("PASSIVE_RETURN_NEVER_SET",
        """PASSIVE_OFF ^ INIT_OK""",
        canonical=PASSIVE_OFF ^ INIT_OK
)
PASSIVE_NO_INITIALIZE = util.symbol("PASSIVE_NO_INITIALIZE",
        "PASSIVE_RETURN_NEVER_SET ^ CALLABLES_OK",
        canonical=PASSIVE_RETURN_NEVER_SET ^ CALLABLES_OK
)
PASSIVE_NO_FETCH = util.symbol("PASSIVE_NO_FETCH",
        "PASSIVE_OFF ^ SQL_OK",
        canonical=PASSIVE_OFF ^ SQL_OK
)
PASSIVE_NO_FETCH_RELATED = util.symbol("PASSIVE_NO_FETCH_RELATED",
        "PASSIVE_OFF ^ RELATED_OBJECT_OK",
        canonical=PASSIVE_OFF ^ RELATED_OBJECT_OK
)
PASSIVE_ONLY_PERSISTENT = util.symbol("PASSIVE_ONLY_PERSISTENT",
        "PASSIVE_OFF ^ NON_PERSISTENT_OK",
        canonical=PASSIVE_OFF ^ NON_PERSISTENT_OK
)

DEFAULT_MANAGER_ATTR = '_sa_class_manager'
DEFAULT_STATE_ATTR = '_sa_instance_state'
_INSTRUMENTOR = ('mapper', 'instrumentor')

# these can be replaced by sqlalchemy.ext.instrumentation
# if augmented class instrumentation is enabled.
def manager_of_class(cls):
    return cls.__dict__.get(DEFAULT_MANAGER_ATTR, None)

instance_state = operator.attrgetter(DEFAULT_STATE_ATTR)

instance_dict = operator.attrgetter('__dict__')

def instance_str(instance):
    """Return a string describing an instance."""

    return state_str(instance_state(instance))

def state_str(state):
    """Return a string describing an instance via its InstanceState."""

    if state is None:
        return "None"
    else:
        return '<%s at 0x%x>' % (state.class_.__name__, id(state.obj()))

def state_class_str(state):
    """Return a string describing an instance's class via its InstanceState."""

    if state is None:
        return "None"
    else:
        return '<%s>' % (state.class_.__name__, )


def attribute_str(instance, attribute):
    return instance_str(instance) + "." + attribute


def state_attribute_str(state, attribute):
    return state_str(state) + "." + attribute

def _class_to_mapper(class_or_mapper):
    insp = inspection.inspect(class_or_mapper, False)
    if insp is not None:
        return insp.mapper
    else:
        raise exc.UnmappedClassError(class_or_mapper)


def _mapper_or_none(entity):
    """Return the :class:`.Mapper` for the given class or None if the
    class is not mapped."""

    insp = inspection.inspect(entity, False)
    if insp is not None:
        return insp.mapper
    else:
        return None


def _is_mapped_class(entity):
    """Return True if the given object is a mapped class,
    :class:`.Mapper`, or :class:`.AliasedClass`."""

    insp = inspection.inspect(entity, False)
    return insp is not None and \
        hasattr(insp, "mapper") and \
        (
            insp.is_mapper
            or insp.is_aliased_class
        )


def _is_aliased_class(entity):
    insp = inspection.inspect(entity, False)
    return insp is not None and \
        getattr(insp, "is_aliased_class", False)


def _entity_descriptor(entity, key):
    """Return a class attribute given an entity and string name.

    May return :class:`.InstrumentedAttribute` or user-defined
    attribute.

    """
    insp = inspection.inspect(entity)
    if insp.is_selectable:
        description = entity
        entity = insp.c
    elif insp.is_aliased_class:
        entity = insp.entity
        description = entity
    elif hasattr(insp, "mapper"):
        description = entity = insp.mapper.class_
    else:
        description = entity

    try:
        return getattr(entity, key)
    except AttributeError:
        raise sa_exc.InvalidRequestError(
                    "Entity '%s' has no property '%s'" %
                    (description, key)
                )

_state_mapper = util.dottedgetter('manager.mapper')

@inspection._inspects(type)
def _inspect_mapped_class(class_, configure=False):
    try:
        class_manager = manager_of_class(class_)
        if not class_manager.is_mapped:
            return None
        mapper = class_manager.mapper
        if configure and mapper._new_mappers:
            mapper._configure_all()
        return mapper

    except exc.NO_STATE:
        return None

def class_mapper(class_, configure=True):
    """Given a class, return the primary :class:`.Mapper` associated
    with the key.

    Raises :class:`.UnmappedClassError` if no mapping is configured
    on the given class, or :class:`.ArgumentError` if a non-class
    object is passed.

    Equivalent functionality is available via the :func:`.inspect`
    function as::

        inspect(some_mapped_class)

    Using the inspection system will raise
    :class:`sqlalchemy.exc.NoInspectionAvailable` if the class is not mapped.

    """
    mapper = _inspect_mapped_class(class_, configure=configure)
    if mapper is None:
        if not isinstance(class_, type):
            raise sa_exc.ArgumentError(
                    "Class object expected, got '%r'." % class_)
        raise exc.UnmappedClassError(class_)
    else:
        return mapper


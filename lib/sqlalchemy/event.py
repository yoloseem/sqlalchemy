# sqlalchemy/event.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Base event API."""

from __future__ import absolute_import

from . import util, exc
from itertools import chain
import weakref
import collections

CANCEL = util.symbol('CANCEL')
NO_RETVAL = util.symbol('NO_RETVAL')


def listen(target, identifier, fn, *args, **kw):
    """Register a listener function for the given target.

    e.g.::

        from sqlalchemy import event
        from sqlalchemy.schema import UniqueConstraint

        def unique_constraint_name(const, table):
            const.name = "uq_%s_%s" % (
                table.name,
                list(const.columns)[0].name
            )
        event.listen(
                UniqueConstraint,
                "after_parent_attach",
                unique_constraint_name)

    """

    for evt_cls in _registrars[identifier]:
        tgt = evt_cls._accept_with(target)
        if tgt is not None:
            _EventKey(target, identifier, fn, tgt).listen(*args, **kw)
            break
    else:
        raise exc.InvalidRequestError("No such event '%s' for target '%s'" %
                                (identifier, target))


def listens_for(target, identifier, *args, **kw):
    """Decorate a function as a listener for the given target + identifier.

    e.g.::

        from sqlalchemy import event
        from sqlalchemy.schema import UniqueConstraint

        @event.listens_for(UniqueConstraint, "after_parent_attach")
        def unique_constraint_name(const, table):
            const.name = "uq_%s_%s" % (
                table.name,
                list(const.columns)[0].name
            )
    """
    def decorate(fn):
        listen(target, identifier, fn, *args, **kw)
        return fn
    return decorate


def remove(target, identifier, fn):
    """Remove an event listener.

    Note that some event removals, particularly for those event dispatchers
    which create wrapper functions and secondary even listeners, may not yet
    be supported.

    """
    raise NotImplementedError()

def _legacy_signature(since, argnames, converter=None):
    def leg(fn):
        if not hasattr(fn, '_legacy_signatures'):
            fn._legacy_signatures = []
        fn._legacy_signatures.append((since, argnames, converter))
        return fn
    return leg


_registrars = util.defaultdict(list)


def _is_event_name(name):
    return not name.startswith('_') and name != 'dispatch'


class _UnpickleDispatch(object):
    """Serializable callable that re-generates an instance of
    :class:`_Dispatch` given a particular :class:`.Events` subclass.

    """
    def __call__(self, _parent_cls):
        for cls in _parent_cls.__mro__:
            if 'dispatch' in cls.__dict__:
                return cls.__dict__['dispatch'].dispatch_cls(_parent_cls)
        else:
            raise AttributeError("No class with a 'dispatch' member present.")


class _Dispatch(object):
    """Mirror the event listening definitions of an Events class with
    listener collections.

    Classes which define a "dispatch" member will return a
    non-instantiated :class:`._Dispatch` subclass when the member
    is accessed at the class level.  When the "dispatch" member is
    accessed at the instance level of its owner, an instance
    of the :class:`._Dispatch` class is returned.

    A :class:`._Dispatch` class is generated for each :class:`.Events`
    class defined, by the :func:`._create_dispatcher_class` function.
    The original :class:`.Events` classes remain untouched.
    This decouples the construction of :class:`.Events` subclasses from
    the implementation used by the event internals, and allows
    inspecting tools like Sphinx to work in an unsurprising
    way against the public API.

    """

    def __init__(self, _parent_cls):
        self._parent_cls = _parent_cls

    def _join(self, other):
        """Create a 'join' of this :class:`._Dispatch` and another.

        This new dispatcher will dispatch events to both
        :class:`._Dispatch` objects.

        """
        if '_joined_dispatch_cls' not in self.__class__.__dict__:
            cls = type(
                    "Joined%s" % self.__class__.__name__,
                    (_JoinedDispatcher, self.__class__), {}
                )
            for ls in _event_descriptors(self):
                setattr(cls, ls.name, _JoinedDispatchDescriptor(ls.name))

            self.__class__._joined_dispatch_cls = cls
        return self._joined_dispatch_cls(self, other)

    def __reduce__(self):
        return _UnpickleDispatch(), (self._parent_cls, )

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        for ls in _event_descriptors(other):
            if isinstance(ls, _EmptyListener):
                continue
            getattr(self, ls.name).\
                for_modify(self)._update(ls, only_propagate=only_propagate)

    @util.hybridmethod
    def _clear(self):
        for attr in dir(self):
            if _is_event_name(attr):
                getattr(self, attr).for_modify(self).clear()


def _event_descriptors(target):
    return [getattr(target, k) for k in dir(target) if _is_event_name(k)]


class _EventMeta(type):
    """Intercept new Event subclasses and create
    associated _Dispatch classes."""

    def __init__(cls, classname, bases, dict_):
        _create_dispatcher_class(cls, classname, bases, dict_)
        return type.__init__(cls, classname, bases, dict_)


def _create_dispatcher_class(cls, classname, bases, dict_):
    """Create a :class:`._Dispatch` class corresponding to an
    :class:`.Events` class."""

    # there's all kinds of ways to do this,
    # i.e. make a Dispatch class that shares the '_listen' method
    # of the Event class, this is the straight monkeypatch.
    dispatch_base = getattr(cls, 'dispatch', _Dispatch)
    cls.dispatch = dispatch_cls = type("%sDispatch" % classname,
                                        (dispatch_base, ), {})
    dispatch_cls._listen = cls._listen

    for k in dict_:
        if _is_event_name(k):
            setattr(dispatch_cls, k, _DispatchDescriptor(cls, dict_[k]))
            _registrars[k].append(cls)


def _remove_dispatcher(cls):
    for k in dir(cls):
        if _is_event_name(k):
            _registrars[k].remove(cls)
            if not _registrars[k]:
                del _registrars[k]

class Events(util.with_metaclass(_EventMeta, object)):
    """Define event listening functions for a particular target type."""

    @classmethod
    def _accept_with(cls, target):
        # Mapper, ClassManager, Session override this to
        # also accept classes, scoped_sessions, sessionmakers, etc.
        if hasattr(target, 'dispatch') and (
                    isinstance(target.dispatch, cls.dispatch) or \
                    isinstance(target.dispatch, type) and \
                    issubclass(target.dispatch, cls.dispatch)
                ):
            return target
        else:
            return None

    @classmethod
    def _listen(cls, event_key, propagate=False, insert=False, named=False):
        event_key.base_listen(propagate=propagate, insert=insert, named=named)

    @classmethod
    def _remove(cls, event_key):
        event_key.remove()

    @classmethod
    def _clear(cls):
        cls.dispatch._clear()


class _DispatchDescriptor(object):
    """Class-level attributes on :class:`._Dispatch` classes."""

    def __init__(self, parent_dispatch_cls, fn):
        self.__name__ = fn.__name__
        argspec = util.inspect_getargspec(fn)
        self.arg_names = argspec.args[1:]
        self.has_kw = bool(argspec.keywords)
        self.legacy_signatures = list(reversed(
                        sorted(
                            getattr(fn, '_legacy_signatures', []),
                            key=lambda s: s[0]
                        )
                    ))
        self.__doc__ = fn.__doc__ = self._augment_fn_docs(parent_dispatch_cls, fn)

        self._clslevel = weakref.WeakKeyDictionary()
        self._empty_listeners = weakref.WeakKeyDictionary()

    def _adjust_fn_spec(self, fn, named):
        argspec = util.get_callable_argspec(fn, no_self=True)
        if named:
            fn = self._wrap_fn_for_kw(fn)
        fn = self._wrap_fn_for_legacy(fn, argspec)
        return fn

    def _wrap_fn_for_kw(self, fn):
        def wrap_kw(*args, **kw):
            argdict = dict(zip(self.arg_names, args))
            argdict.update(kw)
            return fn(**argdict)
        return wrap_kw

    def _wrap_fn_for_legacy(self, fn, argspec):
        for since, argnames, conv in self.legacy_signatures:
            if argnames[-1] == "**kw":
                has_kw = True
                argnames = argnames[0:-1]
            else:
                has_kw = False

            if len(argnames) == len(argspec.args) \
                and has_kw is bool(argspec.keywords):

                if conv:
                    assert not has_kw
                    def wrap_leg(*args):
                        return fn(*conv(*args))
                else:
                    def wrap_leg(*args, **kw):
                        argdict = dict(zip(self.arg_names, args))
                        args = [argdict[name] for name in argnames]
                        if has_kw:
                            return fn(*args, **kw)
                        else:
                            return fn(*args)
                return wrap_leg
        else:
            return fn

    def _indent(self, text, indent):
        return "\n".join(
                    indent + line
                    for line in text.split("\n")
                )

    def _standard_listen_example(self, sample_target, fn):
        example_kw_arg = self._indent(
                "\n".join(
                    "%(arg)s = kw['%(arg)s']" % {"arg": arg}
                    for arg in self.arg_names[0:2]
                ),
                "    ")
        if self.legacy_signatures:
            current_since = max(since for since, args, conv in self.legacy_signatures)
        else:
            current_since = None
        text = (
                "from sqlalchemy import event\n\n"
                "# standard decorator style%(current_since)s\n"
                "@event.listens_for(%(sample_target)s, '%(event_name)s')\n"
                "def receive_%(event_name)s(%(named_event_arguments)s%(has_kw_arguments)s):\n"
                "    \"listen for the '%(event_name)s' event\"\n"
                "\n    # ... (event handling logic) ...\n"
        )

        if len(self.arg_names) > 2:
            text += (

                "\n# named argument style (new in 0.9)\n"
                "@event.listens_for(%(sample_target)s, '%(event_name)s', named=True)\n"
                "def receive_%(event_name)s(**kw):\n"
                "    \"listen for the '%(event_name)s' event\"\n"
                "%(example_kw_arg)s\n"
                "\n    # ... (event handling logic) ...\n"
            )

        text %= {
                    "current_since": " (arguments as of %s)" %
                                    current_since if current_since else "",
                    "event_name": fn.__name__,
                    "has_kw_arguments": " **kw" if self.has_kw else "",
                    "named_event_arguments": ", ".join(self.arg_names),
                    "example_kw_arg": example_kw_arg,
                    "sample_target": sample_target
                }
        return text

    def _legacy_listen_examples(self, sample_target, fn):
        text = ""
        for since, args, conv in self.legacy_signatures:
            text += (
                "\n# legacy calling style (pre-%(since)s)\n"
                "@event.listens_for(%(sample_target)s, '%(event_name)s')\n"
                "def receive_%(event_name)s(%(named_event_arguments)s%(has_kw_arguments)s):\n"
                "    \"listen for the '%(event_name)s' event\"\n"
                "\n    # ... (event handling logic) ...\n" % {
                    "since": since,
                    "event_name": fn.__name__,
                    "has_kw_arguments": " **kw" if self.has_kw else "",
                    "named_event_arguments": ", ".join(args),
                    "sample_target": sample_target
                }
            )
        return text

    def _version_signature_changes(self):
        since, args, conv = self.legacy_signatures[0]
        return (
                "\n.. versionchanged:: %(since)s\n"
                "    The ``%(event_name)s`` event now accepts the \n"
                "    arguments ``%(named_event_arguments)s%(has_kw_arguments)s``.\n"
                "    Listener functions which accept the previous argument \n"
                "    signature(s) listed above will be automatically \n"
                "    adapted to the new signature." % {
                    "since": since,
                    "event_name": self.__name__,
                    "named_event_arguments": ", ".join(self.arg_names),
                    "has_kw_arguments": ", **kw" if self.has_kw else ""
                }
            )

    def _augment_fn_docs(self, parent_dispatch_cls, fn):
        header = ".. container:: event_signatures\n\n"\
                "     Example argument forms::\n"\
                "\n"

        sample_target = getattr(parent_dispatch_cls, "_target_class_doc", "obj")
        text = (
                header +
                self._indent(
                            self._standard_listen_example(sample_target, fn),
                            " " * 8)
            )
        if self.legacy_signatures:
            text += self._indent(
                            self._legacy_listen_examples(sample_target, fn),
                            " " * 8)

            text += self._version_signature_changes()

        return util.inject_docstring_text(fn.__doc__,
                text,
                1
            )

    def insert(self, event_key, propagate):
        target = event_key.dispatch_target
        assert isinstance(target, type), \
                "Class-level Event targets must be classes."
        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            if cls is not target and cls not in self._clslevel:
                self.update_subclass(cls)
            else:
                if cls not in self._clslevel:
                    self._clslevel[cls] = []
                self._clslevel[cls].insert(0, event_key._listen_fn)
        event_key._stored_in_collection(self)

    def append(self, event_key, propagate):
        target = event_key.dispatch_target
        assert isinstance(target, type), \
                "Class-level Event targets must be classes."

        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            if cls is not target and cls not in self._clslevel:
                self.update_subclass(cls)
            else:
                if cls not in self._clslevel:
                    self._clslevel[cls] = []
                self._clslevel[cls].append(event_key._listen_fn)
        event_key._stored_in_collection(self)

    def update_subclass(self, target):
        if target not in self._clslevel:
            self._clslevel[target] = []
        clslevel = self._clslevel[target]
        for cls in target.__mro__[1:]:
            if cls in self._clslevel:
                clslevel.extend([
                    fn for fn
                    in self._clslevel[cls]
                    if fn not in clslevel
                ])

    def remove(self, event_key):
        target = event_key.dispatch_target
        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subrclasses__())
            if cls in self._clslevel:
                self._clslevel[cls].remove(event_key.fn)
        event_key._removed_from_collection(self)

    def clear(self):
        """Clear all class level listeners"""

        for dispatcher in self._clslevel.values():
            _EventKey.clear_list(self, dispatcher)

    def for_modify(self, obj):
        """Return an event collection which can be modified.

        For _DispatchDescriptor at the class level of
        a dispatcher, this returns self.

        """
        return self

    def __get__(self, obj, cls):
        if obj is None:
            return self
        elif obj._parent_cls in self._empty_listeners:
            ret = self._empty_listeners[obj._parent_cls]
        else:
            self._empty_listeners[obj._parent_cls] = ret = \
                _EmptyListener(self, obj._parent_cls)
        # assigning it to __dict__ means
        # memoized for fast re-access.  but more memory.
        obj.__dict__[self.__name__] = ret
        return ret

class _HasParentDispatchDescriptor(object):
    def _adjust_fn_spec(self, fn, named):
        return self.parent._adjust_fn_spec(fn, named)

class _EmptyListener(_HasParentDispatchDescriptor):
    """Serves as a class-level interface to the events
    served by a _DispatchDescriptor, when there are no
    instance-level events present.

    Is replaced by _ListenerCollection when instance-level
    events are added.

    """
    def __init__(self, parent, target_cls):
        if target_cls not in parent._clslevel:
            parent.update_subclass(target_cls)
        self.parent = parent  # _DispatchDescriptor
        self.parent_listeners = parent._clslevel[target_cls]
        self.name = parent.__name__
        self.propagate = frozenset()
        self.listeners = ()


    def for_modify(self, obj):
        """Return an event collection which can be modified.

        For _EmptyListener at the instance level of
        a dispatcher, this generates a new
        _ListenerCollection, applies it to the instance,
        and returns it.

        """
        result = _ListenerCollection(self.parent, obj._parent_cls)
        if obj.__dict__[self.name] is self:
            obj.__dict__[self.name] = result
        return result

    def _needs_modify(self, *args, **kw):
        raise NotImplementedError("need to call for_modify()")

    exec_once = insert = append = remove = clear = _needs_modify

    def __call__(self, *args, **kw):
        """Execute this event."""

        for fn in self.parent_listeners:
            fn(*args, **kw)

    def __len__(self):
        return len(self.parent_listeners)

    def __iter__(self):
        return iter(self.parent_listeners)

    def __bool__(self):
        return bool(self.parent_listeners)

    __nonzero__ = __bool__


class _CompoundListener(_HasParentDispatchDescriptor):
    _exec_once = False

    def exec_once(self, *args, **kw):
        """Execute this event, but only if it has not been
        executed already for this collection."""

        if not self._exec_once:
            self(*args, **kw)
            self._exec_once = True

    def __call__(self, *args, **kw):
        """Execute this event."""

        for fn in self.parent_listeners:
            fn(*args, **kw)
        for fn in self.listeners:
            fn(*args, **kw)

    def __len__(self):
        return len(self.parent_listeners) + len(self.listeners)

    def __iter__(self):
        return chain(self.parent_listeners, self.listeners)

    def __bool__(self):
        return bool(self.listeners or self.parent_listeners)

    __nonzero__ = __bool__

class _ListenerCollection(_CompoundListener):
    """Instance-level attributes on instances of :class:`._Dispatch`.

    Represents a collection of listeners.

    As of 0.7.9, _ListenerCollection is only first
    created via the _EmptyListener.for_modify() method.

    """

    def __init__(self, parent, target_cls):
        if target_cls not in parent._clslevel:
            parent.update_subclass(target_cls)
        self.parent_listeners = parent._clslevel[target_cls]
        self.parent = parent
        self.name = parent.__name__
        self.listeners = []
        self.propagate = set()

    def for_modify(self, obj):
        """Return an event collection which can be modified.

        For _ListenerCollection at the instance level of
        a dispatcher, this returns self.

        """
        return self

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        existing_listeners = self.listeners
        existing_listener_set = set(existing_listeners)
        self.propagate.update(other.propagate)
        other_listeners = [l for l
                in other.listeners
                if l not in existing_listener_set
                and not only_propagate or l in self.propagate
                ]

        existing_listeners.extend(other_listeners)

        to_associate = other.propagate.union(other_listeners)
        _EventKey._stored_in_collection_multi(self, other, to_associate)

    def insert(self, event_key, propagate):
        if event_key.conditional_prepend(self, self.listeners) and propagate:
            self.propagate.add(event_key._listen_fn)

    def append(self, event_key, propagate):
        if event_key.conditional_append(self, self.listeners) and propagate:
            self.propagate.add(event_key._listen_fn)

    def remove(self, event_key):
        self.listeners.remove(event_key._listen_fn)
        self.propagate.discard(event_key._listen_fn)
        event_key._removed_from_collection(self)

    def clear(self):
        _EventKey.clear_list(self, self.listeners)
        _EventKey.clear_set(self, self.propagate)


class _JoinedDispatcher(object):
    """Represent a connection between two _Dispatch objects."""

    def __init__(self, local, parent):
        self.local = local
        self.parent = parent
        self._parent_cls = local._parent_cls


class _JoinedDispatchDescriptor(object):
    def __init__(self, name):
        self.name = name

    def __get__(self, obj, cls):
        if obj is None:
            return self
        else:
            obj.__dict__[self.name] = ret = _JoinedListener(
                        obj.parent, self.name,
                        getattr(obj.local, self.name)
                    )
            return ret


class _JoinedListener(_CompoundListener):
    _exec_once = False

    def __init__(self, parent, name, local):
        self.parent = parent
        self.name = name
        self.local = local
        self.parent_listeners = self.local

        # fix .listeners for the parent.  This means
        # new events added to the parent won't be picked
        # up here, and also means listener functions are
        # copied to a new list.
        #self.listeners = _EventKey.copy_list(getattr(self.parent, self.name))

    # Alternatively, the listeners can
    # be via @property to just return getattr(self.parent, self.name)
    # each time.  this is possibly less performant than having the list
    # set up ahead of time, however it remains to be seen how expensive
    # copy_list() is going to be.
    @property
    def listeners(self):
        return getattr(self.parent, self.name)

    def _adjust_fn_spec(self, fn, named):
        return self.local._adjust_fn_spec(fn, named)

    def for_modify(self, obj):
        self.local = self.parent_listeners = self.local.for_modify(obj)
        return self

    def insert(self, event_key, propagate):
        self.local.insert(event_key, propagate)

    def append(self, event_key, propagate):
        self.local.append(event_key, propagate)

    def remove(self, event_key):
        self.local.remove(event_key)

    def clear(self):
        raise NotImplementedError()


class dispatcher(object):
    """Descriptor used by target classes to
    deliver the _Dispatch class at the class level
    and produce new _Dispatch instances for target
    instances.

    """
    def __init__(self, events):
        self.dispatch_cls = events.dispatch
        self.events = events

    def __get__(self, obj, cls):
        if obj is None:
            return self.dispatch_cls
        obj.__dict__['dispatch'] = disp = self.dispatch_cls(cls)
        return disp


class _EventKey(object):
    """Represents the arguments passed to :func:`.listen` and
    provides managed registration services on behalf of those arguments.

    By "managed registration", we mean that event listening functions and
    other objects can be added to various collections in such a way that their
    membership in all those collections can be revoked at once, based on
    an equivalent :class:`._EventKey`.

    """

    _key_to_collection = collections.defaultdict(weakref.WeakKeyDictionary)
    _collection_to_key = weakref.WeakKeyDictionary()

    def __init__(self, target, identifier, fn, dispatch_target,
                                _fn_wrap=None, _dispatch_reg=None):
        self.target = target
        self.identifier = identifier
        self.fn = fn
        self.fn_wrap = _fn_wrap
        self.dispatch_target = dispatch_target

        self._dispatch_reg = None

        """
        lookup #1:

        given: ListenerCollection/_DispatchDescriptor, function contained
        locates: target/identifier/fn key used to establish the relationship
        used by:  when one ListenerCollection/_DispatchDescriptor copies its functions
                  over to another, the fns that were set up with it also need to be
                  set up with the new one for removal

        lookup #2:

        given: target/identifier/fn
        locates: all ListenerCollection/_DistpatchDescriptors along with the ultimate
                 fn/wrapped fn stored in that collection, *or* some way to remove
                 the item  (which would have to point to the item anyway)
        used by: the remove feature

        (target, identifier, fn) -> {
                                            listenercollection -> listener_fn
                                            listenercollection -> listener_fn
                                            listenercollection -> listener_fn
                                        }

        listener_fn -> {
                            listenercollection   -> (target, identifier, fn)
                            listenercollection   -> (target, identifier, fn)
                            listenercollection   -> (target, identifier, fn)
                        }

        {
            ref(listenercollection) ->
        }

        """

    @classmethod
    def _stored_in_collection_multi(cls, newowner, oldowner, elements):
        if not elements:
            return

        old_listener_to_key = cls._collection_to_key[oldowner]

        if newowner in cls._collection_to_key:
            new_listener_to_key = cls._collection_to_key[newowner]
        else:
            new_listener_to_key = cls._collection_to_key[newowner] = {}

        for listen_fn in elements:
            key = old_listener_to_key[listen_fn]
            dispatch_reg = cls._key_to_collection[key]
            if newowner in dispatch_reg:
                assert dispatch_reg[newowner] is listen_fn
            else:
                dispatch_reg[newowner] = listen_fn

            new_listener_to_key[listen_fn] = key

    def _stored_in_collection(self, owner):
        key = (id(self.target), self.identifier, self.fn)
        dispatch_reg = self._key_to_collection[key]
        if owner in dispatch_reg:
            assert dispatch_reg[owner] is self._listen_fn
        else:
            dispatch_reg[owner] = self._listen_fn

        if owner in self._collection_to_key:
            listener_to_key = self._collection_to_key[owner]
        else:
            listener_to_key = self._collection_to_key[owner] = {}

        listener_to_key[self._listen_fn] = key

    def _removed_from_collection(self, owner):
        key = (id(self.target), self.identifier, self.fn)

        dispatch_reg = self._key_to_collection[key]

        dispatch_reg.pop(owner, None)

        if owner in self._collection_to_key:
            listener_to_key = self._collection_to_key[owner]
            listener_to_key.pop(self._listen_fn)

    def with_wrapper(self, fn_wrap):
        if fn_wrap is self._listen_fn:
            return self
        else:
            return _EventKey(
                self.target,
                self.identifier,
                self.fn,
                self.dispatch_target,
                _fn_wrap=fn_wrap,
                #_dispatch_reg=self.dispatch_reg
                )

    def with_dispatch_target(self, dispatch_target):
        if dispatch_target is self.dispatch_target:
            return self
        else:
            return _EventKey(
                self.target,
                self.identifier,
                self.fn,
                dispatch_target,
                _fn_wrap=self.fn_wrap,
                #_dispatch_reg=self.dispatch_reg
                )

    def listen(self, *args, **kw):
        self.dispatch_target.dispatch._listen(self, *args, **kw)

    def remove(self):
        getattr(self.dispatch_target.dispatch, self.identifier).remove(self)

    def base_listen(self, propagate=False, insert=False,
                            named=False):

        target, identifier, fn = \
            self.dispatch_target, self.identifier, self._listen_fn

        dispatch_descriptor = getattr(target.dispatch, identifier)

        fn = dispatch_descriptor._adjust_fn_spec(fn, named)
        self = self.with_wrapper(fn)

        if insert:
            dispatch_descriptor.\
                    for_modify(target.dispatch).insert(self, propagate)
        else:
            dispatch_descriptor.\
                    for_modify(target.dispatch).append(self, propagate)

    @property
    def _listen_fn(self):
        return self.fn_wrap or self.fn

    def append_value_to_list(self, owner, list_, value):
        self._stored_in_collection(owner)
        list_.append(value)

    def append_to_list(self, owner, list_):
        self._stored_in_collection(owner)
        list_.append(self._listen_fn)

    def remove_from_list(self, owner, list_):
        self._removed_from_collection(owner)
        list_.remove(self._listen_fn)

    def prepend_to_list(self, owner, list_):
        self._stored_in_collection(owner)
        list_.insert(0, self._listen_fn)

    def conditional_prepend(self, owner, list_):
        if self._listen_fn not in list_:
            self.prepend_to_list(owner, list_)
            return True
        else:
            return False

    def conditional_append(self, owner, list_):
        if self.fn not in list_:
            self.append_to_list(owner, list_)
            return True
        else:
            return False

    @classmethod
    def clear_list(cls, owner, list_):
        list_[:] = []

    @classmethod
    def clear_set(cls, owner, set_):
        set_.clear()


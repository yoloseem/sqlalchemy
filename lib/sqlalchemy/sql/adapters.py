from . import visitors, selectable, elements
from .. import util


class AliasedRow(object):
    """Wrap a RowProxy with a translation map.

    This object allows a set of keys to be translated
    to those present in a RowProxy.

    """
    def __init__(self, row, map):
        # AliasedRow objects don't nest, so un-nest
        # if another AliasedRow was passed
        if isinstance(row, AliasedRow):
            self.row = row.row
        else:
            self.row = row
        self.map = map

    def __contains__(self, key):
        return self.map[key] in self.row

    def has_key(self, key):
        return key in self

    def __getitem__(self, key):
        return self.row[self.map[key]]

    def keys(self):
        return self.row.keys()


class ClauseAdapter(visitors.ReplacingCloningVisitor):
    """Clones and modifies clauses based on column correspondence.

    E.g.::

      table1 = Table('sometable', metadata,
          Column('col1', Integer),
          Column('col2', Integer)
          )
      table2 = Table('someothertable', metadata,
          Column('col1', Integer),
          Column('col2', Integer)
          )

      condition = table1.c.col1 == table2.c.col1

    make an alias of table1::

      s = table1.alias('foo')

    calling ``ClauseAdapter(s).traverse(condition)`` converts
    condition to read::

      s.c.col1 == table2.c.col1

    """
    def __init__(self, selectable, equivalents=None,
                        include=None, exclude=None,
                        include_fn=None, exclude_fn=None,
                        adapt_on_names=False):
        self.__traverse_options__ = {'stop_on': [selectable]}
        self.selectable = selectable
        if include:
            assert not include_fn
            self.include_fn = lambda e: e in include
        else:
            self.include_fn = include_fn
        if exclude:
            assert not exclude_fn
            self.exclude_fn = lambda e: e in exclude
        else:
            self.exclude_fn = exclude_fn
        self.equivalents = util.column_dict(equivalents or {})
        self.adapt_on_names = adapt_on_names

    def _corresponding_column(self, col, require_embedded,
                              _seen=util.EMPTY_SET):
        newcol = self.selectable.corresponding_column(
                                    col,
                                    require_embedded=require_embedded)
        if newcol is None and col in self.equivalents and col not in _seen:
            for equiv in self.equivalents[col]:
                newcol = self._corresponding_column(equiv,
                                require_embedded=require_embedded,
                                _seen=_seen.union([col]))
                if newcol is not None:
                    return newcol
        if self.adapt_on_names and newcol is None:
            newcol = self.selectable.c.get(col.name)
        return newcol

    magic_flag = False
    def replace(self, col):
        if not self.magic_flag and isinstance(col, selectable.FromClause) and \
            self.selectable.is_derived_from(col):
            return self.selectable
        elif not isinstance(col, elements.ColumnElement):
            return None
        elif self.include_fn and not self.include_fn(col):
            return None
        elif self.exclude_fn and self.exclude_fn(col):
            return None
        else:
            return self._corresponding_column(col, True)


class ColumnAdapter(ClauseAdapter):
    """Extends ClauseAdapter with extra utility functions.

    Provides the ability to "wrap" this ClauseAdapter
    around another, a columns dictionary which returns
    adapted elements given an original, and an
    adapted_row() factory.

    """
    def __init__(self, selectable, equivalents=None,
                        chain_to=None, include=None,
                        exclude=None, adapt_required=False):
        ClauseAdapter.__init__(self, selectable, equivalents, include, exclude)
        if chain_to:
            self.chain(chain_to)
        self.columns = util.populate_column_dict(self._locate_col)
        self.adapt_required = adapt_required

    def wrap(self, adapter):
        ac = self.__class__.__new__(self.__class__)
        ac.__dict__ = self.__dict__.copy()
        ac._locate_col = ac._wrap(ac._locate_col, adapter._locate_col)
        ac.adapt_clause = ac._wrap(ac.adapt_clause, adapter.adapt_clause)
        ac.adapt_list = ac._wrap(ac.adapt_list, adapter.adapt_list)
        ac.columns = util.populate_column_dict(ac._locate_col)
        return ac

    adapt_clause = ClauseAdapter.traverse
    adapt_list = ClauseAdapter.copy_and_process

    def _wrap(self, local, wrapped):
        def locate(col):
            col = local(col)
            return wrapped(col)
        return locate

    def _locate_col(self, col):
        c = self._corresponding_column(col, True)
        if c is None:
            c = self.adapt_clause(col)

            # anonymize labels in case they have a hardcoded name
            if isinstance(c, elements.Label):
                c = c.label(None)

        # adapt_required used by eager loading to indicate that
        # we don't trust a result row column that is not translated.
        # this is to prevent a column from being interpreted as that
        # of the child row in a self-referential scenario, see
        # inheritance/test_basic.py->EagerTargetingTest.test_adapt_stringency
        if self.adapt_required and c is col:
            return None

        return c

    def adapted_row(self, row):
        return AliasedRow(row, self.columns)

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['columns']
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.columns = util.PopulateDict(self._locate_col)

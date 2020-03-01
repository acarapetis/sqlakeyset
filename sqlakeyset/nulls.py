"""Code handling NULL sorting, including vendor-specific details."""

from warnings import warn
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import case
from sqlalchemy import cast

# The SQL standard doesn't specify whether nulls should be sorted first or
# last; and unfortunately vendors have chosen different conventions:
#
# DBMS        Changeable  Default(ASC)    Default(DESC)
# Oracle      Y           LAST            FIRST
# Pg          Y           LAST            FIRST
# MySQL       N           FIRST           LAST
# MSSQL       N           FIRST           LAST
# SQLite      N*          FIRST           LAST
# *SQLite gained support for NULLS FIRST/NULLS LAST control in late 2019.
#
# Since we are attempting to page queries without changing their order, our
# only choice is to work out which DBMS we're dealing with and build our
# conditions accordingly.

NULLS_LAST_FOR_ASC_BY_DEFAULT = {
    'postgresql': True,
    'oracle': True,
    'mysql': False,
    'mssql': False,
    'sqlite': False,
}

def is_nulls_last_default(dialect=None):
    v = NULLS_LAST_FOR_ASC_BY_DEFAULT.get(dialect and dialect.name or None)
    if v is None:
        warn(f"SQL dialect {dialect.name} is not known to sqlakeyset. "
             "Assuming the same behaviour as MySQL/SQLite/MSSQL. "
             "If any of your sort columns are nullable, "
             "you may get results in the wrong order.")
        v = False
    return v


# To avoid comparing nulls, we expand each column c into two columns:
# the flag column ( c is null ? 1 : 0 )
# and the value column ( c is null ? PLACEHOLDER : c ).
# Here the PLACEHOLDER cannot be null, and must be the same type as c;
# so we use cast('0', c.type).

def build_comparison(columnexpr, value, dialect,
                     nullslast=None, ascending=True):
    if nullslast is None:
        nullslast = is_nulls_last_default(dialect)

    # If the column has a custom type, apply the custom
    # preprocessing to the comparison value.
    try:
        processor = columnexpr.type.process_bind_param
    except AttributeError:
        processor = lambda v, d: v
    value = processor(value, dialect)

    BLANK = cast('0', columnexpr.type)
    null, notnull = (1, 0) if nullslast else (0, 1)
    flag_col = case([(columnexpr == None, null)], else_=notnull)
    flag_val = null if value is None else notnull
    val_col = case([(columnexpr == None, BLANK)], else_=columnexpr)
    val_val = BLANK if value is None else value
    col, val = [flag_col, val_col], [flag_val, val_val]

    if not ascending:
        col, val = val, col
    return col, val



def _might_be_nullable(x):
    try:
        if not x.nullable or x.property.columns[0].nullable:
            return True
    except (AttributeError, IndexError, KeyError):
        pass
    return False

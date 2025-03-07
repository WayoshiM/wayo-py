import re
from functools import reduce
from datetime import datetime, timedelta
from operator import attrgetter, or_
from typing import Any, Union, Optional, TypeVar

import polars as pl
import polars.selectors as cs
from cachetools.func import lfu_cache
from datetime_matcher import DatetimeMatcher

from util import SCHEDULER_TZ

_T = TypeVar('T')
_HashableCollection = Union[frozenset[_T], tuple[_T]]


@lfu_cache(maxsize=64)
def build_flag_expr(col_name, flags: frozenset[int]):
    sub_expr = []
    if 0 in flags:
        sub_expr.append(pl.col(col_name) == 0)
    if flags != {0}:
        sub_expr.append((pl.col(col_name) & pl.repeat(reduce(or_, flags - {0}), pl.col(col_name).len())).cast(pl.Boolean))
    return pl.any_horizontal(sub_expr) if len(sub_expr) > 1 else sub_expr[0]


_ppp_newline = re.compile('(?:^ | (\n) | $)')


def pretty_print_polars(df: pl.DataFrame | pl.Series):
    return _ppp_newline.sub(r'\1', str(df))


_number_logic_to_str = {
    '=': '',
    '!=': 'not',
    '>': 'greater than',
    '<': 'less than',
    '<=': 'at most',
    '>=': 'at least',
    '(': 'greater than',
    '[': 'at least',
    ')': 'less than',
    ']': 'at most',
}
_date_logic_to_str = {
    '=': 'on',
    '!=': 'not on',
    '>': 'later than',
    '<': 'earlier than',
    '<=': 'on or before',
    '>=': 'on or after',
    '(': 'later than',
    '[': 'on or after',
    ')': 'earlier than',
    ']': 'on or before',
}
_dt_q_remapping = {
    'Y': 'YEAR',
    'D': 'DAY',
    'M': 'MONTH',
    'DOW': 'WEEKDAY',
    'WKDAY': 'WEEKDAY',
}
# as of polars 0.15, weekday mapping went from 0-6 to 1-7
_wkday_mapping = {
    'M': '1',
    'MON': '1',
    'T': '2',
    'TUE': '2',
    'W': '3',
    'WED': '3',
    'R': '4',
    'THU': '4',
    'F': '5',
    'FRI': '5',
}
_wkday_backmapping = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
_month_mapping = {
    'JAN': '1',
    'FEB': '2',
    'MAR': '3',
    'APR': '4',
    'MAY': '5',
    'JUN': '6',
    'JUL': '7',
    'AUG': '8',
    'SEP': '9',
    'OCT': '10',
    'NOV': '11',
    'DEC': '12',
}
_month_backmapping = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December',
]
NUM_TO_MULT = {
    '0': 'duds',
    '1': 'singles',
    '2': 'doubles',
    '3': 'triples',
    '4': 'quadruples',
    '5': 'quintuples',
    '6': 'sextuples',
    '7': 'septuples',
    '8': 'octuples',
    '9': 'nonuples',
    '10': 'decuples',
    '11': 'undecuples',
    '12': 'dodecuples',
}
NUM_TO_MULT |= {int(k): v for k, v in NUM_TO_MULT.items()}


def build_int_expression(base_expr, conds, date_hybrid=False, mult_hybrid=False, expr_only=False):
    cond_builder = []
    desc_builder = []
    plural = False

    str_converter = (lambda s: _date_logic_to_str[s] + ' a') if date_hybrid else (lambda s: _number_logic_to_str[s])
    num_converter = (lambda n: NUM_TO_MULT[n]) if mult_hybrid else lambda n: n

    for c in conds:
        if m := re.fullmatch('([\[\(])(\d+),\s*(\d+)([\)\]])', c):
            b1, start, end, b2 = m.groups()
            start = int(start)
            end = int(end)
            left = 'left' if b1 == '[' else None
            right = 'right' if b2 == ']' else None
            closed = 'both' if left and right else left if left else right if right else 'none'
            cond_builder.append(base_expr.is_between(start, end, closed=closed))
            desc_builder.append(f'{str_converter(b1)} {num_converter(start)} and {str_converter(b2)} {num_converter(end)}')
            plural |= start != 1 or end != 1
        elif re.fullmatch('\d+(,\s*\d+)+', c):
            c_strs = re.split(',\s*', c)
            ns = [int(i) for i in c_strs]
            cond_builder.append(base_expr.is_in(ns))
            desc_builder.append('one of ' + (', '.join([num_converter(n) for n in c_strs] if mult_hybrid else c_strs)))
            plural |= ns != [1]
        elif m := re.fullmatch('=?\s*(\d+)', c):
            n = int(m.group(1))
            cond_builder.append(base_expr == n)
            desc_builder.append(f'{num_converter(n)}')
            plural |= n != 1
        elif m := re.fullmatch('(!=|>|<|<=|>=)\s*(\d+)', c):
            op, n = m.groups()
            cond_builder.append(eval(f'base_expr {c}'))
            desc_builder.append(f'{str_converter(op)} {num_converter(n)}')
            plural |= int(n) != 1
        else:
            raise ValueError(f'Malformed number expression: {c}')

    if len(conds) > 1:
        return (
            pl.any_horizontal(cond_builder)
            if expr_only
            else (pl.any_horizontal(cond_builder), ', or '.join(desc_builder), plural)
        )
    else:
        return cond_builder[0] if expr_only else (cond_builder[0], desc_builder[0], plural)


_DTM = DatetimeMatcher()


# extract_dates in ^ this third-party library was iffy for me
# but the match functionality taking care of datetime formats under the hood
# was very useful, even though it's strict matching
def build_date_expression(base_expr, conds, dateFormat):
    cond_builder = []
    desc_builder = []
    df = lambda d: datetime.strptime(d, dateFormat).date()

    for c in conds:
        if m := _DTM.match(f'([\[\(])({dateFormat}),\s*({dateFormat})([\)\]])', c):
            b1, b2 = m.group(1, 4)
            start, end = [df(d) for d in m.group(2, 3)]
            left = 'left' if b1 == '[' else None
            right = 'right' if b2 == ']' else None
            closed = 'both' if left and right else left if left else right if right else 'none'
            cond_builder.append(base_expr.is_between(start, end, closed=closed))
            desc_builder.append(f'{_date_logic_to_str[b1]} {m.group(2)} and {_date_logic_to_str[b2]} {m.group(3)}')
        elif m := _DTM.match(f'^=?\s*({dateFormat})$', c):
            d = df(m.group(1))
            cond_builder.append(base_expr == d)
            desc_builder.append(m.group(1))
        elif m := _DTM.match(f'({dateFormat})(?:,\s*({dateFormat}))+', c):
            cond_builder.append(base_expr.is_in([df(d) for d in m.groups()]))
            desc_builder.append('on one of ' + (', '.join(m.groups())))
        elif m := _DTM.match(f'(!=|>|<|<=|>=)\s*({dateFormat})', c):
            op, ds = m.groups()
            d = df(ds)
            cond_builder.append(eval(f'base_expr {op} d'))
            desc_builder.append(f'{_date_logic_to_str[op]} {ds}')
        else:
            raise ValueError(f'Malformed date expression: {c}')

    if len(conds) > 1:
        return pl.any_horizontal(cond_builder), ', or '.join(desc_builder)
    else:
        return cond_builder[0], desc_builder[0]


def build_dt_q_expression(col, dt_q, e):
    dt_q = _dt_q_remapping.get(dt_q, dt_q)
    special = dt_q in ('MONTH', 'WEEKDAY')

    if special:
        if dt_q == 'MONTH':
            mapping = _month_mapping
            backmapping = lambda cd: re.sub('([1-9]|1[012])', lambda m: _month_backmapping[int(m.group()) - 1], cd)
        else:
            mapping = _wkday_mapping
            backmapping = lambda cd: re.sub('[0-6]', lambda m: _wkday_backmapping[int(m.group())], cd)
        try:
            e = [re.sub('[A-Z]+', lambda m: mapping[m.group()], ee) for ee in e]
        except KeyError as ke:
            raise ValueError(f'Invalid {dt_q} string: {ke}')
    else:
        backmapping = lambda cd: cd

    f, cd, _ = build_int_expression((attrgetter(dt_q.lower())(pl.col(col).dt))(), e, special)
    cd = f'{dt_q} of DATE is ' + backmapping(cd)
    return f, cd


@lfu_cache(maxsize=64)
def build_lineup_expr(
    pg_query: Union[_HashableCollection[Any], str],
    slots: _HashableCollection[int | str],
    flags: Optional[_HashableCollection[int]],
    freqs: Optional[_HashableCollection[int]],
):
    e = [
        pl.col(f'PG{s}').cast(str).str.contains(f'(?i){pg_query}')
        if type(pg_query) == str
        else pl.col(f'PG{s}_p').is_in([str(pg) for pg in pg_query])
        for s in slots
    ]

    if flags:
        e = [ee & build_flag_expr(f'PG{s}_f', frozenset(flags)) for ee, s in zip(e, slots)]

    if freqs:
        return pl.sum_horizontal(ee.cast(pl.UInt8) for ee in e).is_in(tuple(freqs))
    else:
        return pl.any_horizontal(e)


def transform_str_to_dts(e, dateFormat):
    return [
        re.sub(r'(\b\d\b|[A-Z]+)', lambda m: '0' + m.group() if m.group().isnumeric() else m.group().title(), ee)
        if ee not in ('TODAY', 'YESTERDAY')
        else (
            datetime.now(tz=SCHEDULER_TZ).date()
            if ee == 'TODAY'
            else datetime.now(tz=SCHEDULER_TZ).date() - timedelta(days=1)
        ).strftime(dateFormat)
        for ee in e
    ]

import bisect
import enum
import io
import itertools
import operator
import pickle
import re
import string
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from random import choice, choices, random, sample, shuffle
from operator import attrgetter
from functools import partial, reduce
from typing import *

import numpy as np
import openpyxl
import portion as P
import polars as pl
import texttable
from cachetools import LFUCache, cachedmethod
from cachetools.keys import hashkey
from cachetools.func import lfu_cache

Portion = NewType('Portion', P.Interval)

from pg import CURRENT_SEASON, MAX_PG_NAME_LEN, PG, PGPlaying

MISSING_PG = '??????????'
ANY_SLOT = frozenset({1, 2, 3, 4, 5, 6})
FIRST_HALF = frozenset({1, 2, 3})
SECOND_HALF = frozenset({4, 5, 6})
ANY_FLAG = frozenset()
ANY_FREQ = frozenset()

from util import PLAYING_FLAGS, SORT_PROD
from util_expr import build_flag_expr as has_any_flags


_pf_bit_mapping = {pf: len(PLAYING_FLAGS) - 1 - PLAYING_FLAGS.index(pf) for pf in ('^', '?', 'MDG')}


def _pf_bit(pf):
    return _pf_bit_mapping[pf]


Q_FLAG = frozenset((len(PLAYING_FLAGS) - PLAYING_FLAGS.index('?'),))
U_FLAG = frozenset((len(PLAYING_FLAGS) - PLAYING_FLAGS.index('^'),))
QU_FLAGS = Q_FLAG | U_FLAG
ALL_FLAGS_BUT_GUESS = frozenset(range(0, len(PLAYING_FLAGS))) - Q_FLAG
ALL_FLAGS_BUT_UNCERTAIN = ALL_FLAGS_BUT_GUESS - U_FLAG

FLAG_INFO = OrderedDict(
    {
        'C': 'non-car for a non-restored car (NCFAC), or a car game for a boat. Shows up as `car` or `boat` in output.',
        'T': '3+ multiprizer for trips, or in the one instance of the all-Plinko show, Plinko for a trip.',
        '&': "any playing for 2 or more cars that isn't It's Optional or Triple Play. Shows up as `cars` in output.",
        '*': 'A playing with rule changes just for that playing (such as a Big Money Week version of a casher, or 35th Anniversary Plinko), a LMAD Mashup game, or in two syndicated cases, a "vintage car reproduction". Note for MDS shows, an increase on the top prize on cashers was so common that these instances are not denoted with this flag.',
        '@': 'A playing for a restored car, or a 4-prizer played for LA sports season tickets.',
        'R': 'A playing for really unusual prize(s).',
        '$': "Mainly a non-cash game played for cash, hence the use of the dollar sign here. In syndicated, Double Prices was played twice in one segment, for two prizes. In a couple instances instead, a car game for a trailer, or in the one instance of the all-Plinko show, Plinko for two regular prizes.",
        '^': 'The slotting of the game is uncertain. Some old records are incomplete and slotted by best guess.',
        '?': 'The identity of the game is uncertain. Some old records are incomplete and this is a best guess on what the game was. Most often it is known which set of two or three games occurred within a small subset of shows, with no further certainty.',
        'M': 'This was the Million Dollar Game in a Drew MDS (primetime) in 2008. Shows up as `MDG` in output.',
    }
)


@enum.unique
class SlotCertainty(enum.IntFlag):
    CERTAIN = 0
    SLOT = 2 ** _pf_bit('^')
    GAME = 2 ** _pf_bit('?')


UNAIRED_DUPLICATES = ['0013R', '58XXD']


pct_chance = lambda pct: random() < pct / 100


class ConflictSheet:
    _MAX_CACHE = 64
    _CACHE_GETTER = attrgetter('cache')

    def __init__(
        self, load_func: Callable[[], io.BytesIO], save_func: Callable[[io.BytesIO], None], override_pickle: bool = False
    ):
        self.load_func = load_func
        self.save_func = save_func
        self.cache = LFUCache(self._MAX_CACHE)
        try:
            if override_pickle:
                raise ValueError
            self.load_pickle()
            self.excel_fp = None
        except:
            self.load_excel()
            self._df_dict = {}
            self.initialize()

    def get(self, time: str):
        return self._df_dict[time]

    def is_ready(self):
        return bool(self._df_dict)

    def gen_sheet(self, pg_playings: Sequence[PGPlaying], endpoints: Portion, seasonText: str, time: str):
        assert len(pg_playings) == 6
        pgs = [pgp.pg for pgp in pg_playings]

        header = [seasonText, 'SLF', 'TOT', 'SL%']
        header[1:1] = [pg.sheet_abbr if pg else '' for pg in pgs]

        ttable = texttable.Texttable(max_width=0)
        ttable.header(header)
        ttable.set_cols_align('lrrrrrrrrr')
        ttable.set_header_align('c' + 'r' * 9)
        ttable.set_cols_dtype('tiiiiiiiif')
        ttable.set_precision(1)

        sub_slots_df = self.endpoint_sub(endpoints, time, q=self.slot_table(time, 'S'))
        # permutation must be done to match both possible orders of pg1/pg2, so do the more costly operation once
        pg_pair_perms = set(itertools.permutations([str(pg) for pg in pgs], 2))

        pg_pair_dict = Counter()
        for r in self.endpoint_sub(endpoints, time).select(pl.col('^PG[1-6]_p$')).collect().rows():
            for pg1, pg2 in set(itertools.combinations(r, 2)) & pg_pair_perms:
                pg_pair_dict[pg1, pg2] += 1
                pg_pair_dict[pg2, pg1] += 1

        nums = np.zeros((6, 8), dtype=int)
        hundo = False
        for i, pgp in enumerate(pg_playings, 1):
            row = [pgp]
            pg = pgp.pg

            pg_conf = [pg_pair_dict[str(pg), str(pgs[j - 1])] if i != j else '-' for j in range(1, 7)]
            nums[i - 1, :6] = [i if type(i) is int else 0 for pgc in pg_conf]
            row.extend(pg_conf)

            ssd_pg = (
                sub_slots_df.filter((pl.col('PG') == str(pg)) & (pl.col('flag') < SlotCertainty.SLOT))
                .select('^PG\d$')
                .collect()
                .sum()
                .fill_null(0)
            )

            nums[i - 1, 6] = slf = ssd_pg[0, i - 1]
            nums[i - 1, 7] = tot = sum(ssd_pg.row(0))
            hundo |= slf == tot
            row.extend([slf, tot, 100.0 * slf / tot if tot else '----'])

            ttable.add_row(row)

        ttable.set_cols_width(
            [max(MAX_PG_NAME_LEN, len(seasonText), max(len(str(pgp)) for pgp in pg_playings))]
            + [max(3, 1 + int(np.log10(n.max()))) for n in nums.T]
            + [5 if hundo else 4]
        )

        return ttable.draw()

    def _pick_slot(self, pg, initial_slots):
        slots = copy(initial_slots)
        if pg in PG.partition_table['NO_OPENING_ACT']:
            slots -= {1, 2}
        elif pg in PG.partition_table['NO_FIRST']:
            slots -= {1}
        if not slots:
            slots = copy(initial_slots)
        return choice(tuple(slots))

    def _pick_game(self, pg_group):
        t = tuple(pg_group)

    def gen_lineup(self, pg_sample: Set[PG], half_hour: bool = False):
        nPGs = [None] * (3 if half_hour else 6)
        non_car = [None] * (3 if half_hour else 6)
        unused_slots = set(range(1, 7))
        unused_fees = ['GP', 'SP']
        halves = [{1, 2, 3}, {4, 5, 6}]
        unused_fee_halves = [0, 1]

        # decide on cash or no cash.
        casher = choice(tuple(pg_sample & PG.partition_table['CASH'])) if pct_chance(70) else None
        cash_type = None if not casher else 'SP' if casher in PG.partition_table['SP/CASH'] else 'GP'
        if casher:
            slot = self._pick_slot(casher, unused_slots)
            nPGs[slot - 1] = casher
            unused_slots.remove(slot)
            pg_sample -= PG.partition_table[f'{cash_type}']
            if casher in PG.partition_table['BAILOUT']:
                pg_sample -= PG.partition_table['BAILOUT']
            unused_fees.remove(cash_type)
            unused_fee_halves.remove((slot - 1) // 3)
            if slot == 1 and pct_chance(98):
                pg_sample.discard(PG.GoldenRoad)

        # now decide cars.
        total_car_count = 3 if pct_chance(2) else 2
        for ttc in range(total_car_count):
            do_non_car = pct_chance(4)
            car_sample = pg_sample & (PG.partition_table['NON-CAR'] if do_non_car else PG.partition_table['CAR'])

            respect_halves = pct_chance(93)
            slot_choices = copy(unused_slots)
            if ttc == 0:
                slot_choices &= halves[0]
                if 0 not in unused_fee_halves and respect_halves:
                    car_sample -= PG.partition_table['FEE']
            if ttc == 1:
                slot_choices &= halves[1]
                if 1 not in unused_fee_halves and respect_halves:
                    car_sample -= PG.partition_table['FEE']
            if nPGs[2] in PG.partition_table['CAR'] and pct_chance(99.5):
                slot_choices -= {4}

            car = choice(tuple(car_sample))

            if car == PG.GoldenRoad and not nPGs[0] and pct_chance(95):
                slot = 1
            else:
                slot = self._pick_slot(car, slot_choices)
            nPGs[slot - 1] = car
            unused_slots.remove(slot)
            non_car[slot - 1] = do_non_car
            car_type = 'SP' if car in PG.partition_table['SP/CAR'] else 'GP' if car in PG.partition_table['GP/CAR'] else None
            if car_type:
                pg_sample -= PG.partition_table[f'{car_type}']
                if respect_halves:
                    unused_fees.remove(car_type)
                    unused_fee_halves.remove((slot - 1) // 3)
            else:
                pg_sample.remove(car)

        if len(unused_fees) == 2 or pct_chance(95):
            shuffle(unused_fees)
            shuffle(unused_fee_halves)
            # fill in unused fees with regular fees, respecting halves most of the time.
            for fee, half in zip(unused_fees, unused_fee_halves):
                sample = pg_sample & PG.partition_table[f'REG. {fee}']
                if sample:
                    mp = choice(tuple(sample))
                    respect_halves = pct_chance(95)
                    slot = self._pick_slot(mp, unused_slots & halves[half] if respect_halves else unused_slots)
                    nPGs[slot - 1] = mp
                    unused_slots.remove(slot)
                    pg_sample -= PG.partition_table[f'REG. {fee}']

        # decide on a 4 prizer or not.
        do_4p = (
            bool(pg_sample & PG.partition_table['4 PRIZER'])
            and not (set(nPGs) & {PG.MoreOrLess, PG.FortuneHunter})
            and pct_chance(27)
        )
        if do_4p:
            mp = choice(tuple(pg_sample & PG.partition_table['4 PRIZER']))
            slot = self._pick_slot(mp, unused_slots)
            nPGs[slot - 1] = mp
            unused_slots.remove(slot)
            pg_sample -= PG.partition_table['4 PRIZER']

        # decide on a 3 prizer or not.
        do_3p = bool(pg_sample & PG.partition_table['3 PRIZER']) and pct_chance(3 if do_4p else 30)
        if do_3p:
            mp = choice(tuple(pg_sample & PG.partition_table['3 PRIZER']))
            slot = self._pick_slot(mp, unused_slots)
            nPGs[slot - 1] = mp
            unused_slots.remove(slot)
            pg_sample -= PG.partition_table['3 PRIZER']

        # decide on a 2 prizer or not.
        do_2p = bool(pg_sample & PG.partition_table['2 PRIZER']) and pct_chance(
            0.5 if do_4p and do_3p else 40 if do_3p else 75
        )
        if do_2p:
            mp = choice(tuple(pg_sample & PG.partition_table['2 PRIZER']))
            slot = self._pick_slot(mp, unused_slots)
            nPGs[slot - 1] = mp
            unused_slots.remove(slot)
            pg_sample -= PG.partition_table['2 PRIZER']

        if unused_slots:
            # decide on a 1+ prizer or not.
            do_1plusp = bool(pg_sample & PG.partition_table['1+ PRIZER']) and pct_chance(
                0.25 if (do_4p + do_3p + do_2p >= 2) else 10 if (do_4p + do_3p >= 1) else 25 if do_2p and do_3p else 65
            )
            if do_1plusp:
                mp = choice(tuple(pg_sample & PG.partition_table['1+ PRIZER']))
                slot = self._pick_slot(mp, unused_slots)
                nPGs[slot - 1] = mp
                unused_slots.remove(slot)
                pg_sample -= PG.partition_table['1+ PRIZER']

        while unused_slots:
            # fill out remainder of lineup with 1 prizers.
            mp = choice(tuple(pg_sample & PG.partition_table['1 PRIZER']))
            slot = self._pick_slot(mp, unused_slots)
            nPGs[slot - 1] = mp
            unused_slots.remove(slot)
            pg_sample.remove(mp)

        return nPGs, non_car

    @cachedmethod(_CACHE_GETTER, key=partial(hashkey, 'ep_sub'))
    def endpoint_sub(self, endpoints: Portion, time: str, *, q: Optional[pl.LazyFrame] = None):
        if q is None:
            q = self._df_dict[time].lazy()
        if not endpoints:
            return q
        elif type(endpoints.lower) is not int:
            return q.filter(pl.col('AIRDATE').is_in(pl.date_range(endpoints.lower, endpoints.upper, '1d')))
        elif time != 'primetime':
            return q.filter(pl.col('S').is_in(list(P.iterate(endpoints, step=1))))
        else:
            return q

    @cachedmethod(_CACHE_GETTER, key=partial(hashkey, 'cc'))
    def concurrence_query(
        self, endpoints: Portion, time: str, pgQueries: Tuple[PG], pgFlags: tuple[Optional[frozenset[int]]]
    ):
        q = self.endpoint_sub(endpoints, time)

        pgs = [str(pg) for pg in pgQueries]
        pg_end_label = 3 if time == 'syndicated' else 6

        if not all(pgFlags):
            q = q.with_columns(pl.concat_list(pl.col(f'^PG[1-{pg_end_label}]_p$')).alias('PG_a'))

        for pg, pgf in zip(pgs, pgFlags):
            if pgf:
                exprs = [(pl.col(f'PG{i}_p') == pg) & (has_any_flags(f'PG{i}_f', pgf)) for i in range(1, pg_end_label + 1)]
                q = q.filter(pl.any(exprs))
            else:
                q = q.filter(pl.col('PG_a').arr.contains(pg))

        return q

    @cachedmethod(_CACHE_GETTER, key=partial(hashkey, 'slots'))
    def slot_table(self, time: str, by: Optional[str] = None):
        q = self._df_dict[time].lazy()

        vc_subset = [[f'PG{i}_p', f'PG{i}_f'] for i in range(1, 4 if time == 'syndicated' else 7)]
        if by:
            for vc in vc_subset:
                vc.insert(0, by)

        gs = [
            q.groupby(vc).agg(pl.count()).rename({f'PG{i}_p': 'PG', f'PG{i}_f': 'flag', 'count': f'PG{i}'})
            for i, vc in enumerate(vc_subset, 1)
        ]
        return reduce(
            lambda g1, g2: g1.join(g2, on=[by, 'PG', 'flag'] if by else ['PG', 'flag'], how='outer'), gs
        ).with_columns(
            [
                pl.col('PG').cast(pl.Categorical).cat.set_ordering('lexical'),
                pl.col('^PG\d$').fill_null(strategy='zero'),
            ]
        )

    def load_excel(self, fn='Price_is_Right_Frequency.xlsx'):
        self.excel_fp = self.load_func(fn)

    def save_excel(self, fn='Price_is_Right_Frequency.xlsx'):
        self.save_func(self.excel_fp, fn)
        self._reset_excel()

    def _reset_excel(self):
        self.excel_fp.seek(0)

    def load_pickle(self, fn='df_dict.pickle'):
        with self.load_func(fn) as f:
            self._df_dict = pickle.load(f)
        with self.load_func('cs_notes.pickle') as f:
            self.notes = pickle.load(f)

    def update(self, prodNumber, pgps, append, airdate, intended_date, notes):
        if not self.excel_fp:
            self.load_excel()

        f_book = openpyxl.load_workbook(self.excel_fp)

        isPrimetime = prodNumber.endswith('SP')

        f_sheet = f_book['Calendar']
        EXCEL_COLORS = [f_sheet[f'N{i}'].fill for i in range(2, 10)]
        EMPTY_FILL = f_sheet['A1'].fill
        if isPrimetime:
            f_sheet = f_book['Primetime']

        if not append:
            row_idx, _, retro_ts = (
                self._df_dict['primetime' if isPrimetime else 'daytime']
                .select(pl.col('^(PROD|INT. DATE)$'))
                .with_row_count()
                .row(by_predicate=pl.col('PROD') == prodNumber)
            )

            idx = 2 + row_idx
            if not isPrimetime:
                idx += self._df_dict['unaired'].filter(pl.col('INT. DATE') < retro_ts).height
        else:
            if isPrimetime:
                idx = 2 + self._df_dict['primetime'].height
            else:
                sorted_index = [
                    SORT_PROD(i)
                    for i in self._df_dict['daytime'].select(pl.col('PROD')).to_series().to_list()
                    if i not in UNAIRED_DUPLICATES
                ]
                idx = (
                    2
                    + len(UNAIRED_DUPLICATES)
                    + self._df_dict['unaired'].height
                    + bisect.bisect(sorted_index, prodNumber[4] + prodNumber[:4], lo=len(sorted_index) - 250)
                )

            f_sheet.insert_rows(idx)

            for c in 'ABCDEFGHIJKL':
                if isPrimetime and c == 'K':
                    continue
                # going into private attributes is not so kosher, but it works! and the library should expose this publicly!
                f_sheet[f'{c}{idx}']._style = copy(f_sheet[f'{c}{idx-1}']._style)
                f_sheet[f'{c}{idx}'].fill = copy(EMPTY_FILL)
                if f_sheet[f'{c}{idx}'].font.color:
                    f_sheet[f'{c}{idx}'].font.color.rgb = 'FF000000'

            f_sheet[f'A{idx}'] = prodNumber
            if not isPrimetime:
                f_sheet[f'B{idx}'] = CURRENT_SEASON
                # f_sheet[f'C{idx}'] = 1 + f_sheet[f'C{idx-1}'].value
            else:
                f_sheet[f'D{idx}'] = 'TPIR@N – '
            for c in 'BC' if isPrimetime else 'DE':
                f_sheet[f'{c}{idx}'] = airdate

        if pgps:
            for c, pgp in zip('EFGHIJ' if isPrimetime else 'GHIJKL', pgps):
                f_sheet[f'{c}{idx}'] = str(pgp.pg) if append else pgp.pg_str
                if not append or pgp.flag:
                    if pgp.flag == 2 ** _pf_bit('?'):
                        f_sheet[f'{c}{idx}'] = '*' + f_sheet[f'{c}{idx}'].value + '*'
                    else:
                        f_sheet[f'{c}{idx}'].fill = copy(EXCEL_COLORS[int(np.log2(pgp.flag))] if pgp.flag else EMPTY_FILL)
        else:
            assert not append
            if airdate:
                c = 'B' if isPrimetime else 'D'
                f_sheet[f'{c}{idx}'] = airdate
            if intended_date:
                c = 'C' if isPrimetime else 'E'
                f_sheet[f'{c}{idx}'] = intended_date
            # allow for unsetting notes if empty string
            if notes is not None:
                c = 'D' if isPrimetime else 'F'
                f_sheet[f'{c}{idx}'] = notes

        self.excel_fp = io.BytesIO()
        f_book.save(self.excel_fp)
        self._reset_excel()

    def _fill_in_flags(self, era, fs, col, col_adjust, ec):
        flags = [list(itertools.repeat(0, self._df_dict[era].height)) for _ in range(1, col_adjust + 2)]
        for i, ep in enumerate(
            fs.iter_rows(min_row=2, max_row=self._df_dict[era].height + 1, min_col=col, max_col=col + col_adjust)
        ):
            for s, cell in enumerate(ep, -(col_adjust + 1)):
                if cell.value == '-':
                    flags[s][i] = None
                else:
                    if cell.fill.start_color.rgb in ec:  # and not dff.iloc[i][f'PG{s}'].flag:
                        if cell.value[0] == '*':
                            flags[s][i] |= 2 ** _pf_bit('?')
                        else:
                            flags[s][i] |= 2 ** (ec[cell.fill.start_color.rgb])
                    if cell.font.color and cell.font.color.rgb != 'FF000000':
                        try:
                            flags[s][i] |= 2 ** (ec[cell.font.color.rgb])
                        except KeyError:
                            flags[s][i] |= 2 ** _pf_bit('MDG')

        self._df_dict[era].hstack(
            pl.from_dict(
                {f'PG{d}_f': f for d, f in enumerate(flags, 1)},
                schema={f'PG{d}_f': pl.UInt16 for d in range(1, col_adjust + 2)},
            ),
            in_place=True,
        )

    def _reset_caches(self):
        self.endpoint_sub.cache_clear(self)
        self.concurrence_query.cache_clear(self)
        self.slot_table.cache_clear(self)

    def initialize(self):
        self._df_dict.clear()
        self._reset_caches()

        # create initial sheets.
        for era, sheet, col_include in zip(
            ('daytime', 'primetime', 'syndicated'),
            ('Calendar', 'Primetime', 'Syndication'),
            (lambda i: i != 2, lambda i: i < 10, lambda i: i < 5),
        ):
            q = (
                pl.read_excel(
                    self.excel_fp,
                    sheet_name=sheet,
                    xlsx2csv_options={'ignore_formats': 'float'},
                    read_csv_options={
                        'columns': [i for i in range(12) if col_include(i)],
                        'infer_schema_length': 0,
                        'null_values': '-',
                    },
                )
                .head(-1)  # remove placeholder line of -'s
                .lazy()
            )

            wc = []
            if era != 'primetime':
                wc.append(pl.col('S').cast(pl.UInt8))
            if era != 'syndicated':
                # for some reason some dates are reformatting when going through read_excel, I just take care of it here.
                wc.extend(
                    [
                        pl.when(pl.col(d).str.contains('/'))
                        .then(pl.col(d).str.strptime(pl.Date, '%m/%d/%Y', strict=False))
                        .otherwise(pl.col(d).str.strptime(pl.Date, '%m-%d-%y', strict=False))
                        for d in ('AIRDATE', 'INT. DATE')
                    ]
                )

            # collect here to get height of dataframe for flags step.
            self._df_dict[era] = q.with_columns(wc).collect()

        # lookup actual PG. (remove * on uncertain but not permanently yet)
        for era, df in self._df_dict.items():
            self._df_dict[era] = self._df_dict[era].with_columns(
                [
                    pl.col(f'PG{d}').str.strip('*').apply(lambda pg: str(PG.lookup_table.get(pg))).alias(f'PG{d}_p')
                    for d in range(1, 4 if era == 'syndicated' else 7)
                ]
            )

        # fill in flags. (here we use * to mark uncertain)
        with ThreadPoolExecutor() as executor:
            # read background colors
            f_book = openpyxl.load_workbook(self.excel_fp, read_only=True)
            f_sheet = f_book['Calendar']
            EXCEL_COLORS = {f_sheet[f'N{i}'].fill.start_color.rgb: i - 2 for i in range(2, 10)}

            # read notes
            self.notes = '\n'.join('-' + f_sheet[f'AA{i}'].value for i in range(2, 9))

            # setup and run in parallel
            df_args = [
                ('daytime', f_sheet, 7, 5, EXCEL_COLORS),
                ('primetime', f_book['Primetime'], 5, 5, EXCEL_COLORS),
                ('syndicated', f_book['Syndication'], 3, 2, EXCEL_COLORS),
            ]
            futures = [executor.submit(self._fill_in_flags, *dfa) for dfa in df_args]
            (f.result() for f in futures)

        self._reset_excel()

        # construct PGPlayings and overwrite appropriate columns at this time
        for era, df in self._df_dict.items():
            # I could not get this to work with struct. The is_not_null() did not work (even on a specific struct field).
            self._df_dict[era] = (
                self._df_dict[era]
                .with_columns(
                    [
                        pl.when(pl.col(f'PG{d}_f') > 0)
                        .then(pl.concat_list([pl.col(f'PG{d}').str.strip('*'), pl.col(f'PG{d}_f')]))
                        .otherwise(None)
                        .alias(f'pgplaying{d}')
                        for d in range(1, 4 if era == 'syndicated' else 7)
                    ]
                )
                .with_columns(
                    [
                        pl.when(pl.col(f'pgplaying{d}').is_not_null())
                        .then(pl.col(f'pgplaying{d}').apply(lambda l: str(PGPlaying(l[0], int(l[1])))))
                        .otherwise(pl.col(f'PG{d}'))
                        .alias(f'PG{d}')
                        for d in range(1, 4 if era == 'syndicated' else 7)
                    ]
                )
                .select(pl.exclude('^pgplaying\d$'))
            )

        # build string cache / cat
        for era, df in self._df_dict.items():
            self._df_dict[era] = self._df_dict[era].with_columns(pl.col(pl.Utf8).cast(pl.Categorical))

        # split up unaired
        self._df_dict['unaired'] = self._df_dict['daytime'].filter(pl.col('AIRDATE').is_null())
        self._df_dict['daytime'] = self._df_dict['daytime'].filter(pl.col('AIRDATE').is_not_null())

        # maintain meta column of show number
        for era, df in self._df_dict.items():
            self._df_dict[era] = self._df_dict[era].with_row_count('PG_n', 1)

        # save files to quick load on bot restarts later
        with io.BytesIO() as dfd_ip:
            pickle.dump(self._df_dict, dfd_ip)
            dfd_ip.seek(0)
            self.save_func(dfd_ip, 'df_dict.pickle')
        with io.BytesIO() as dfd_ip:
            pickle.dump(self.notes, dfd_ip)
            dfd_ip.seek(0)
            self.save_func(dfd_ip, 'cs_notes.pickle')

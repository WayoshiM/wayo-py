import bisect
import enum
import io
import itertools
import operator
import pickle
import re
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from random import choice, choices, random, sample, shuffle
from operator import attrgetter, or_
from functools import partial, reduce
from string import ascii_uppercase
from typing import *

import numpy as np
import openpyxl
import portion as P
import polars as pl
import polars.selectors as cs
import texttable
import xlsxwriter
from cachetools import LFUCache, cachedmethod
from cachetools.keys import hashkey
from cachetools.func import lfu_cache

Portion = NewType('Portion', P.Interval)

from pg import CURRENT_SEASON, MAX_PG_NAME_LEN, PG, PGPlaying, FLAG_STRS

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
        'C': 'A playing of a non-car game for a non-restored car (NCFAC), or a car game for a boat. Shows up as `car` or `boat` in wayo.py output.',
        'T': '3+ multiprizer for trips or a car/cash game for trip(s).',
        '&': "Any playing for 2 or more cars that isn't It's Optional or Triple Play. Shows up as `cars` in wayo.py output.",
        '*': 'A playing with rule changes just for that playing (such as a Big Money Week version of a casher, or 35th Anniversary Plinko), a LMAD Mashup game, or in two syndicated cases, a "vintage car reproduction". Note for MDS shows, an increase on the top prize on cashers was so common that these instances are not denoted with this flag.',
        '@': 'A playing for a restored car, or a 4-prizer played for LA sports season tickets.',
        'R': 'A playing for really unusual prize(s).',
        '$': 'Mainly a non-cash game played for cash, hence the use of the dollar sign in wayo.py output. In syndicated, Double Prices was played twice in one segment, for two prizes. In a couple instances instead, a car game for a trailer, or in the one instance of the all-Plinko show, Plinko for two regular prizes.',
        '^': 'The slotting of the game is uncertain. Some old records are incomplete and slotted by best guess.',
        '?': 'The identity of the game is uncertain. Some old records are incomplete and this is a best guess on what the game was. Most often it is known which set of two or three games occurred within a small subset of shows, with no further certainty.',
        'M': 'The Million Dollar Game in a Drew MDS (primetime) in 2008. Shows up as `MDG` in wayo.py output.',
    }
)

COMPENDIUM_NOTES = """- 0013D never aired, the show was replaced with 0013R (also known as 0013D(R)).
- 2944D has no airdate.
- 5811D never aired, it was replaced by 58XXD with an identical lineup.
- Original 1513K was renamed 1513X and replaced with a new 1513K with an identical lineup.
- 4512K and 0294L were broadcast in primetime, but both were originally meant for daytime.
- Several shows in early Seasons 38 and 39 were broadcast in the afternoon as soap opera "replacements."
- A couple midseason episode replacements in S48 had end-of-season production numbers, 9221-22K. But due to the COVID19 pandemic, the season ended abruptly at 9172K. In season 49, 9223-25K were skipped."""


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
        self,
        load_func: Callable[[], io.BytesIO],
        save_func: Callable[[io.BytesIO], None],
    ):
        self.load_func = load_func
        self.save_func = save_func
        self.cache = LFUCache(self._MAX_CACHE)
        self._df_dict = OrderedDict()
        self.initialize()
        self.notes = COMPENDIUM_NOTES

    def get(self, time: str):
        return self._df_dict[time]

    def is_ready(self):
        return bool(self._df_dict)

    def gen_sheet(
        self,
        pg_playings: Sequence[PGPlaying],
        endpoints: Portion,
        seasonText: str,
        time: str,
    ):
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

        sub_slots_df = self.endpoint_sub(endpoints, time, table='slots')
        # permutation must be done to match both possible orders of pg1/pg2, so do the more costly operation once
        pg_pair_perms = set(itertools.permutations([str(pg) for pg in pgs], 2))

        pg_pair_dict = Counter()
        for r in self.endpoint_sub(endpoints, time).select(cs.matches(r'^PG\d_p$')).collect().rows():
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
                sub_slots_df.filter(pl.col('flag') < SlotCertainty.SLOT, PG=str(pg))
                .select(cs.matches(r'PG\d$').sum().fill_null(0))
                .collect()
            )

            nums[i - 1, 6] = slf = ssd_pg[0, i - 1]
            nums[i - 1, 7] = tot = sum(ssd_pg.row(0))
            hundo |= slf == tot and tot
            row.extend([slf, tot, 100.0 * slf / tot if tot else '----'])

            ttable.add_row(row)

        ttable.set_cols_width(
            [
                max(
                    MAX_PG_NAME_LEN,
                    len(seasonText),
                    max(len(str(pgp)) for pgp in pg_playings),
                )
            ]
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
                    slot = self._pick_slot(
                        mp,
                        unused_slots & halves[half] if respect_halves else unused_slots,
                    )
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
    def endpoint_sub(self, endpoints: Portion, time: str, *, table: str = 'df'):
        if table == 'df':
            q = self._df_dict[time].lazy()
        else:  # slot
            q = self.slot_table(time, 'S' if time != 'primetime' else None)

        if not endpoints:
            return q
        elif type(endpoints.lower) is not int:
            if table != 'df':
                raise ValueError('Slots table does not support date start/end at this time.')
            return q.filter(pl.col('AIRDATE').is_between(endpoints.lower, endpoints.upper))
        elif time != 'primetime':
            return q.filter(pl.col('S').is_in(list(P.iterate(endpoints, step=1))))
        else:
            return q

    @cachedmethod(_CACHE_GETTER, key=partial(hashkey, 'cc'))
    def concurrence_query(
        self,
        endpoints: Portion,
        time: str,
        pgQueries: Tuple[PG],
        pgFlags: tuple[Optional[frozenset[int]]],
    ):
        q = self.endpoint_sub(endpoints, time)

        pgs = [str(pg) for pg in pgQueries]
        pg_end_label = 3 if time == 'syndicated' else 6

        for pg, pgf in zip(pgs, pgFlags):
            if pgf:
                exprs = [(pl.col(f'PG{i}_p') == pg) & (has_any_flags(f'PG{i}_f', pgf)) for i in range(1, pg_end_label + 1)]
                q = q.filter(pl.any_horizontal(exprs))
            else:
                q = q.filter(pl.any_horizontal(cs.matches(f'^PG[1-{pg_end_label}]_p$') == pg))

        return q

    @cachedmethod(_CACHE_GETTER, key=partial(hashkey, 'slots'))
    def slot_table(self, time: str, by: Optional[str] = None):
        q = self._df_dict[time]

        vc_subset = [[f'PG{i}_p', f'PG{i}_f'] for i in range(1, 4 if time == 'syndicated' else 7)]
        coalesce_exprs = [
            pl.coalesce('PG', 'PG_right').alias('PG'),
            pl.coalesce('flag', 'flag_right').alias('flag'),
        ]
        if by:
            for vc in vc_subset:
                vc.insert(0, by)
            coalesce_exprs.insert(0, pl.coalesce(by, f'{by}_right').alias(by))

        return (
            reduce(
                lambda g1, g2: g1.join(g2, on=[by, 'PG', 'flag'] if by else ['PG', 'flag'], how='outer').select(
                    *coalesce_exprs, cs.matches(r'^PG\d$')
                ),
                (
                    q.group_by(vc).count().rename({f'PG{i}_p': 'PG', f'PG{i}_f': 'flag', 'count': f'PG{i}'})
                    for i, vc in enumerate(vc_subset, 1)
                ),
            )
            .with_columns(cs.matches(r'^PG\d$').fill_null(0))
            .lazy()
        )

    def save_excel(self, fn='Price_is_Right_Frequency.xlsx'):
        self.save_func(self.write_excel(), fn)

    def write_excel(self):
        # coud factor these static methods & variables out, but ultimately a minor concern
        general_format = {
            'font_name': 'Franklin Gothic Medium',
            'font_size': 12,
            'align': 'center',
            'border': 1,
        }
        date_format = dict(num_format='m/d/yyyy', **general_format)
        bg_colors = [
            '#FF0000',
            '#3399FF',
            '#00FA00',
            '#FFFF00',
            '#FF6600',
            '#FF99FF',
            '#CC99FF',
            '#BFBFBF',
            '#8F8F8F',
            '#CD7F32',
        ]

        def gen_cf_list(col, time):
            bit_cond = (
                (lambda b: b < 9)
                if time in ('daytime', 'unaired')
                else lambda b: (b < 7 if time == 'syndicated' else (lambda b: not (7 <= b <= 8)))
            )
            l = [
                {
                    'type': 'formula',
                    'criteria': f'={col}2={2**bit}',
                    'format': {'bg_color': bgc},
                }
                for bit, bgc in enumerate(bg_colors)
                if bit_cond(bit)
            ]
            if time != 'syndicated':
                l.append(
                    {
                        'type': 'formula',
                        'criteria': f'={col}2=9',
                        'format': {
                            'bg_color': bg_colors[0],
                            'font_color': bg_colors[3],
                        },
                    }
                )
                l.append(
                    {
                        'type': 'formula',
                        'criteria': f'={col}2=72',
                        'format': {
                            'bg_color': bg_colors[6],
                            'font_color': bg_colors[3],
                        },
                    }
                )
            if time == 'primetime':
                l.append(
                    {
                        'type': 'formula',
                        'criteria': f'={col}2=513',
                        'format': {
                            'bg_color': bg_colors[0],
                            'font_color': bg_colors[-1],
                        },
                    }
                )
            return l

        # start of actual work

        excel_fp = io.BytesIO()

        with xlsxwriter.Workbook(excel_fp) as wb:
            for era in self._df_dict.keys():
                df_out = (
                    self._df_dict[era]
                    .lazy()
                    .drop('PG_n')
                    .with_columns(
                        pl.when(pl.col(f'PG{slot}_f') > 0)
                        .then(pl.col(f'PG{slot}').cast(str).str.replace(r' \(.+?\)$', ''))
                        .otherwise(pl.col(f'PG{slot}'))
                        for slot in range(1, 4 if era == 'syndicated' else 7)
                    )
                )

                if era != 'syndicated':
                    df_out = df_out.with_columns(cs.matches('^PG[4-6]$').fill_null('-'))

                df_out = df_out.collect()
                flag_cols = [f'PG{s}_f' for s in range(1, 4 if era == 'syndicated' else 7)]

                df_out.write_excel(
                    wb,
                    era.title(),
                    column_formats={
                        ~cs.temporal(): general_format,
                        cs.temporal(): date_format,
                    },
                    header_format=general_format,
                    conditional_formats={
                        cs.matches(r'^PG\d$'): gen_cf_list(ascii_uppercase[df_out.columns.index(flag_cols[0])], era)
                    },
                    hidden_columns=cs.contains('_'),
                    row_heights=22,
                    column_widths={
                        'PROD': 70,
                        'S': 28,
                        cs.temporal(): 97,
                        'SPECIAL' if era == 'primetime' else 'NOTES': 274,
                        cs.starts_with('PG'): 149,
                        # for some reason the selector above is not capturing this as of 1.9.0. override
                        '_PROD': 0,
                    },
                    freeze_panes='A2',
                    autofilter=False,
                    hide_gridlines=True,
                )

            key_df = pl.from_dict(
                {
                    'KEY': FLAG_INFO.values(),
                    'FLAG': [2**b for b in range(len(FLAG_INFO))],
                }
            )
            key_format = dict(text_wrap=True, **general_format) | {'align': 'left'}
            key_df.write_excel(
                wb,
                'Key',
                header_format=general_format,
                column_formats={'KEY': key_format},
                autofilter=False,
                hide_gridlines=True,
                column_widths={'KEY': 690, 'FLAG': 0},
                conditional_formats={
                    'KEY': [
                        {
                            'type': 'formula',
                            'criteria': f'=B2={2**bit}',
                            'format': {'bg_color': bgc},
                        }
                        for bit, bgc in enumerate(bg_colors)
                    ]
                },
            )

        return excel_fp

    def update(self, prodNumber, pgps, append, airdate, intended_date, notes):
        era = 'primetime' if prodNumber.endswith('SP') else 'daytime'
        row = (
            {
                'PROD': prodNumber,
                'AIRDATE': airdate,
                'INT. DATE': intended_date,
                'NOTES' if era == 'daytime' else 'SPECIAL': notes,
            }
            if append
            else self._df_dict[era].row(by_predicate=pl.col('PROD') == prodNumber, named=True)
        )
        if era == 'daytime' and append:
            row['S'] = CURRENT_SEASON

        if pgps:
            for slot, pgp in enumerate(pgps, 1):
                row[f'PG{slot}'] = str(pgp) if pgp else None
                row[f'PG{slot}_p'] = str(pgp.pg) if pgp else None
                row[f'PG{slot}_f'] = pgp.flag if pgp else None

        if not append:
            if airdate:
                row['AIRDATE'] = airdate
            if intended_date:
                row['INT. DATE'] = intended_date
            if not pgps:  # allow None -> null
                row['NOTES' if era == 'daytime' else 'SPECIAL'] = notes
        else:
            row['PG_n'] = 99999  # filler

        if era == 'daytime':
            row['_PROD'] = row['PROD'][-1] + row['PROD'][:-1]

        df_row = pl.from_dict(row, schema=self._df_dict[era].schema)

        if era == 'primetime':
            if append:
                self._df_dict[era].extend(df_row.select(self._df_dict[era].columns))
            else:
                self._df_dict[era] = (
                    self._df_dict[era]
                    .filter(pl.col('PROD') != prodNumber)
                    .extend(df_row.select(self._df_dict[era].columns))
                    .sort('PG_n')
                )
        else:
            self._df_dict[era] = (
                self._df_dict[era]
                .drop('PG_n')
                .filter(pl.col('PROD') != prodNumber)
                .merge_sorted(df_row.drop('PG_n'), '_PROD')
                .with_row_index('PG_n', 1)
            )

        self._reset_caches()

    def _reset_caches(self):
        self.endpoint_sub.cache_clear(self)
        self.concurrence_query.cache_clear(self)
        self.slot_table.cache_clear(self)

    def initialize(self, fn='Price_is_Right_Frequency.xlsx'):
        self._df_dict.clear()
        self._reset_caches()

        excel_fp = self.load_func(fn)

        for era in ('daytime', 'primetime', 'syndicated', 'unaired'):
            q = pl.read_excel(
                excel_fp,
                sheet_name=era.title(),
                # to do: update to calamine / fastexcel
                engine='xlsx2csv',
                engine_options={'ignore_formats': 'float'},
                read_options={
                    'row_index_name': 'PG_n',
                    'row_index_offset': 1,
                    'null_values': '-',
                },
            ).lazy()

            wc = [cs.ends_with('_f').cast(pl.UInt16)]
            if era != 'primetime':
                wc.append(pl.col('S').cast(pl.UInt8))
            if era != 'syndicated':
                # for some reason some dates are reformatting when going through read_excel, I just take care of it here.
                wc.extend(
                    [
                        pl.when(cs.ends_with('DATE').str.contains('/'))
                        .then(cs.ends_with('DATE').str.strptime(pl.Date, '%m/%d/%Y', strict=False))
                        .otherwise(cs.ends_with('DATE').str.strptime(pl.Date, '%m-%d-%y', strict=False))
                    ]
                )

            q = q.with_columns(wc).collect()

            q = (
                q.lazy()
                .with_columns(
                    pl.when(pl.col(f'PG{d}_f') > 0)
                    .then(
                        pl.format(
                            '{} ({})',
                            pl.col(f'PG{d}'),
                            pl.col(f'PG{d}_f').cast(pl.String).replace(FLAG_STRS),
                        )
                    )
                    .otherwise(pl.col(f'PG{d}'))
                    for d in range(1, 4 if era == 'syndicated' else 7)
                )
                .with_columns(
                    pl.when((pl.col(f'PG{d}').str.ends_with('car)')) & (pl.col(f'PG{d}_p').is_in(PG.CAR_BOATABLE_STRS)))
                    .then(pl.col(f'PG{d}').str.replace('car)', 'boat)', literal=True))
                    .otherwise(pl.col(f'PG{d}'))
                    for d in range(1, 4 if era == 'syndicated' else 7)
                )
            )

            if era == 'daytime':
                q = q.with_columns(
                    pl.col('PROD')
                    .str.replace_all(r'^(\w{4})(\w)$', '$2$1')
                    .str.replace('R', 'D')
                    .str.replace('XX', '11')
                    .alias('_PROD')
                )

            # 2025-03-07 update
            # Categorical with all the flag combos was getting finicky depending on polars version
            # and if a new flag combo is coming in or not, which is also column-dependent (new slotting)
            # let's scrap it, can actually now pl.Enum raw PG safely
            self._df_dict[era] = q.with_columns(
                (cs.matches(r'^PG\d_p$')).cast(pl.Enum([pg.sheetName for pg in PG]))
            ).collect()


if __name__ == '__main__':
    io
    from dropboxwayo import dropboxwayo

    con = ConflictSheet(
        lambda fn: io.BytesIO(dropboxwayo.download('/heroku/wayo-py/Price_is_Right_Frequency.xlsx')),
        lambda iob, fn: dropboxwayo.upload(iob.getvalue(), '/heroku/wayo-py/Price_is_Right_Frequency.xlsx'),
    )

    con.initialize()
    df_out = (
        con._df_dict['daytime']
        # .lazy()
        .drop('PG_n').with_columns(
            pl.when(pl.col(f'PG{slot}_f') > 0)
            .then(pl.col(f'PG{slot}').cast(str).str.replace(r' \(.+?\)$', ''))
            .otherwise(pl.col(f'PG{slot}'))
            for slot in range(1, 4 if 'daytime' == 'syndicated' else 7)
        )
    )

    print(cs.expand_selector(df_out, cs.contains('_')))

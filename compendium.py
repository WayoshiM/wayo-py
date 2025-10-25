import asyncio
import logging
import os
import re
from datetime import datetime, date
from functools import partial, reduce
from operator import attrgetter, or_
from typing import *

import aiofile
import polars as pl
import polars.selectors as cs
import portion as P
from cachetools import LFUCache, cachedmethod
from cachetools.func import lfu_cache
from cachetools.keys import hashkey
from more_itertools import chunked
from sortedcontainers import SortedDict, SortedSet

from dropboxwayo import dropboxwayo

Range = Union[range, Iterable[int]]

_log = logging.getLogger('wayo_log')

CURRENT_SEASON = 43
COMPENDIUM_NOTES = """- On November 2 1992 (S10, #1796), R1 (VANNA'S PREGNANT) was heavily edited due to Vanna's miscarriage. Despite this editing we know what the puzzle was, so it does remain in the compendium.
- On November 20 1998 (S16, #2980) and November 10 2003 (S21, #3946), there were clip shows celebrating the 3000th and 4000th show milestones. There are no actual puzzles that day and those dates are omitted entirely.
- On November 16 2005 (S23, #4338), R1 was edited out of the final cut to Hurricane Katrina. This row is omitted from this version of the compendium entirely, as unlike the PREGNANT puzzle we have no idea what the puzzle was.
- There are a relative handful of confirmed puzzles with incomplete data, or from shows where all the puzzles are not known. These are listed separately on <https://buyavowel.boards.net/page/compendiummisc>, outside the scope of this bot.
"""


class WheelCompendium:
    _MAX_CACHE = 64
    _CACHE_GETTER = attrgetter('cache')

    def __init__(self, *, loop=None, debug: bool = False):
        self._loop = loop
        self._debug = debug
        self.cache = LFUCache(self._MAX_CACHE)

        self.dfs = {t: None for t in ('syndicated', 'primetime', 'kids', 'daytime', 'au', 'gb')}
        self.df_choices = {t: None for t in ('syndicated', 'primetime')}
        self.df_sched = {t: None for t in ('syndicated', 'primetime')}
        self._internal_df_choices = {'syndicated': SortedDict(), 'primetime': None}
        self._df_schedsyn_dict = SortedDict()
        self._df_syndicated_dict = SortedDict()

        self.coverage = None
        self._coverage_dict = SortedDict()

        self.join_cache = {}

        self._cols = {
            'syndicated': [
                'S',
                'DATE',
                'EP',
                'E/S',
                'UNC',
                'ROUND',
                'PP',
                'RL',
                'PR',
                'PUZZLE',
                'CATEGORY',
                'CLUE/BONUS',
            ],
            'primetime': ['DATE', 'EP', 'HH', 'ROUND', 'PP', 'PUZZLE', 'CATEGORY'],
        }

        self.loaded = False

        self.sem = asyncio.Semaphore(4)

        self.sanity_checks = SortedSet()

        load = self.load(
            list(range(1, CURRENT_SEASON + 1))
            + [
                'primetime',
                'kids',
                'daytime',
                'au',
                'gb',
                'choicesprimetime',
                'schedprimetime',
            ]
            + [f'choices{s}0' for s in range(4, 6)]
            + [f'sched{s}0' for s in range(1, 6)]
        )
        if self._loop:
            self._loop.create_task(load)
        else:
            asyncio.run(load)

    @property
    def seasons(self):
        return self._df_syndicated_dict.keys()

    def check_dups(self, season, lf: pl.LazyFrame):
        dup = lf.drop('_lc').collect().is_duplicated()
        if dup.any():
            self.sanity_checks.add(
                f'{season} has entire duplicated rows. Check the <tr> tags:\n\n'
                + str(lf.select('DATE', 'RD', 'PUZZLE').collect().filter(dup).unique())
                + '\n'
            )
        # else:
        # dup2 = lf.select('DATE', 'RD').collect().is_duplicated()
        # if dup2.any():
        # self.sanity_checks.add(f'{season} has multiple same rounds within a DATE. Double-check typos:\n\n' + str(lf.select('DATE', 'RD').collect().filter(dup).unique()) + '\n')

    async def load(self, pages: Collection[int | str]):
        self.loaded = False
        _log.info('start loading wc at ' + str(datetime.now()))

        changed_cov = await asyncio.gather(*(self._load_season(p) for p in pages))
        # changed_cov = [await self._load_season(p) for p in pages]
        changed_syndicated = {p for p in pages if type(p) is int}
        choices_update = False

        if changed_syndicated:
            self.dfs['syndicated'] = pl.concat(self._df_syndicated_dict.values()).collect().lazy()
            choices_update = any(s >= 35 for s in changed_syndicated)
            self.check_dups('syndicated', self.dfs['syndicated'])

        for k in self.dfs.keys():
            if k in pages:
                self.dfs[k] = self.dfs[k].collect().lazy()
                self.check_dups(k, self.dfs[k])

        self.df_sched['syndicated'] = pl.concat(self._df_schedsyn_dict.values())
        self.df_sched['syndicated'] = (
            self.dfs['syndicated']
            .select('S', 'EP', 'E/S')
            .unique(maintain_order=True)
            .join(self.df_sched['syndicated'], 'EP')
            .select('S', 'DATE', 'DATE_STR', 'EP', 'E/S', 'RED', 'YELLOW', 'BLUE', 'THEME')
            .collect()
            .lazy()
        )

        for t in ('syndicated', 'primetime'):
            self.join_cache[f'sched_{t}'] = (
                self.dfs[t]
                .join(
                    self.df_sched[t].drop(cs.contains('DATE'), 'S', 'E/S', strict=False),
                    on='EP',
                    how='left',
                )
                .collect()
                .lazy()
            )

        # choice checks

        built_dfs = []

        for k, v in self._internal_df_choices.items():
            if not (choices_update if k == 'syndicated' else k in pages):
                continue

            if k == 'syndicated':
                cdfs = [
                    (
                        k2,
                        (pl.col('S').is_between(35, 40) if k2 == '4' else pl.col('S').is_between(41, 50)),
                        v2,
                    )
                    for k2, v2 in v.items()
                ]
            else:
                cdfs = [(None, None, v)]

            for k2, f, cdf in cdfs:
                df = (
                    (
                        self.dfs['syndicated']
                        .filter(f, RD='BR')
                        .select('S', 'DATE', 'EP', 'E/S', 'PUZZLE', 'CATEGORY', '_lc')
                        .join(cdf, on='DATE', how='outer')
                    )
                    if k == 'syndicated'
                    else (
                        self.dfs['primetime']
                        .filter(RD='BR')
                        .select('DATE', 'EP', 'HH', 'PUZZLE', 'CATEGORY', '_lc')
                        .join(cdf, on=['DATE', 'HH'], how='outer')
                    )
                )

                missing = df.filter(pl.col('C').is_null()).collect()
                if missing.height:
                    self.sanity_checks.add(
                        f'The following EPS have BRs in "{k}" but not in "BR choices" (double-check HTML formatting): '
                        + str(missing.get_column('EP').to_list())
                    )
                missing = df.filter(pl.col('S' if k == 'syndicated' else 'EP').is_null()).collect()
                if missing.height:
                    self.sanity_checks.add(
                        f'The following DATES have BRs in "BR choices" but not in "{k}" (double-check HTML formatting): '
                        + str(missing.get_column('DATE').to_list())
                    )

                mismatch = df.filter(
                    pl.concat_list(cs.matches(r'^CAT\d$').cast(str)).list.get(pl.col('C') - 1) != pl.col('CATEGORY')
                ).collect()
                if mismatch.height:
                    self.sanity_checks.add(
                        f'The following EPS have BRs in "BR choices" with incorrectly highlighted choice, compared to "{k}"): '
                        + str(mismatch.get_column('EP_right').to_list())
                    )

                built_dfs.append(
                    df.select(
                        pl.col(['S', 'DATE', 'EP', 'E/S'] if k == 'syndicated' else ['DATE', 'EP', 'HH']),
                        'PUZZLE',
                        pl.col('CATEGORY').alias('CHOSEN'),
                        pl.col('C').alias('POS'),
                        pl.col('CAT1').alias('CHOICE1'),
                        pl.col('CAT2').alias('CHOICE2'),
                        pl.col('CAT3').alias('CHOICE3'),
                        '_lc',
                    )
                )

            if choices_update and k == 'syndicated':
                self.df_choices['syndicated'] = pl.concat(built_dfs).collect().lazy()
            else:
                self.df_choices['primetime'] = built_dfs[0].collect().lazy()

            built_dfs.clear()

        if any(changed_cov):
            self._reset_coverage()
        _log.info('end loading wc at ' + str(datetime.now()))
        self.loaded = True

    def _reset_coverage(self):
        self.coverage = pl.from_dict({'S': self._coverage_dict.keys(), 'COV': self._coverage_dict.values()})
        self.coverage = self.coverage.with_columns(
            pl.when(~(pl.col('S').is_in([16, 21, 37, CURRENT_SEASON])))
            .then(pl.lit(195))
            .when(pl.col('S') == 37)
            .then(pl.lit(167))
            .otherwise(pl.when(pl.col('S') == CURRENT_SEASON).then(pl.col('COV')).otherwise(pl.lit(194)))
            .alias('MAX')
        )
        self.calc_coverage.cache_clear(self)

    @cachedmethod(_CACHE_GETTER, key=partial(hashkey, 'calc_cov'))
    def calc_coverage(self, seasons, doRange: bool = False):
        if seasons:
            if doRange:
                seasons = list(
                    P.iterate(
                        reduce(
                            or_,
                            [P.closed(*s) if len(s) > 1 else P.singleton(*s) for s in chunked(seasons, 2)],
                            P.empty(),
                        ),
                        step=1,
                    )
                )
            c_df = self.coverage.filter(pl.col('S').is_in(seasons))
        else:
            c_df = self.coverage

        if c_df.height > 1:
            c_df = pl.concat(
                [
                    c_df.with_columns(pl.col('S').cast(str)),
                    c_df.select(
                        [
                            pl.lit('ALL').alias('S'),
                            pl.col('COV').sum(),
                            pl.col('MAX').sum(),
                        ]
                    ),
                ]
            )

        return c_df.with_columns((100.0 * pl.col('COV') / pl.col('MAX')).round(1).alias('PCT'))

    async def _load_season(self, season: int | str) -> bool:
        is_syn = type(season) is int
        s_str = f's{season:02d}' if is_syn else season

        async with self.sem:
            try:
                if self._debug:
                    async with aiofile.async_open(
                        os.path.expanduser(f'~/Dropbox/heroku/wayo-py/compendium/{s_str}.csv')
                    ) as afp:
                        file = await afp.read()
                else:
                    file = await asyncio.to_thread(dropboxwayo.download, f'/heroku/wayo-py/compendium/{s_str}.csv')
            except Exception as e:
                # _log.warning(e)
                location = 'locally' if self._debug else 'from Dropbox'
                _log.warning(f'Could not download {s_str} {location}')
                return

        try:
            df = pl.read_csv(
                file.encode() if self._debug else file,
                dtypes={'EP': pl.UInt16, 'ROUND': pl.Categorical('lexical')},
                truncate_ragged_lines=True,
            ).lazy()
        except pl.exceptions.ComputeError as e:
            _log.error(f'season {season} could not load correctly')
            raise e

        if not is_syn and re.match(r'choices\d0', season):
            self._internal_df_choices['syndicated'][season[-2]] = df.select(
                'EP',
                pl.col('DATE').str.strptime(pl.Date, '%m/%d/%y'),
                pl.col('C').cast(pl.UInt8),
                cs.matches(r'^CAT\d?$').cast(pl.Categorical('lexical')),
            )
        if not is_syn and re.match(r'sched\d0', season):
            self._df_schedsyn_dict[season[-2]] = df.select(
                pl.col('DATE').str.strptime(pl.Date, '%m/%d/%y', strict=False),
                pl.coalesce(
                    pl.col('DATE').str.strptime(pl.Date, '%m/%d/%y', strict=False).dt.strftime('%b %d %Y'),
                    pl.col('DATE'),
                ).alias('DATE_STR'),
                ~cs.contains(('DATE', 'THEME')),
                pl.col('THEME').cast(pl.Categorical('lexical')),
            )
        if not is_syn and season == 'schedprimetime':
            self.df_sched['primetime'] = df.with_columns(pl.col('DATE').str.strptime(pl.Date, '%m/%d/%y')).lazy()
        elif season == 'choicesprimetime':
            self._internal_df_choices['primetime'] = df.select(
                pl.col('DATE').str.strptime(pl.Date, '%m/%d/%y'),
                'EP',
                pl.col('HH').cast(pl.Categorical),
                pl.col('C').cast(pl.UInt8),
                cs.matches(r'^CAT\d?$').cast(pl.Categorical('lexical')),
            )
        else:
            # meta search columns
            meta_exprs = [
                pl.col('PUZZLE').str.extract_all('[A-Z]').list.eval(pl.element().value_counts(), parallel=True).alias('_lc')
            ]

            if is_syn or season == 'primetime':
                # sanity checks
                date_one_to_one = (
                    df.group_by('DATE', maintain_order=True)
                    .agg(pl.col('EP').unique())
                    .filter(pl.col('EP').list.len() > 1)
                    .collect()
                )
                if date_one_to_one.height:
                    self.sanity_checks.add(f'{season}: at least one date has multiple episodes: \n{date_one_to_one}')

                ep_one_to_one = (
                    df.group_by('EP', maintain_order=True)
                    .agg(pl.col('DATE').unique())
                    .filter(pl.col('DATE').list.len() > 1)
                    .collect()
                )
                if ep_one_to_one.height:
                    self.sanity_checks.add(f'{season}: at least episode has multiple dates: \n{ep_one_to_one}')

            if is_syn:
                unique_dates = (
                    df.select(pl.col('DATE').str.strptime(pl.Date, '%m/%d/%y').unique())
                    .with_columns(pl.col('DATE').dt.weekday().alias('WD'))
                    .collect()
                )
                # as of polars 0.15, weekday is 1-7, not 0-6
                if unique_dates.select(pl.col('WD').unique()).to_series().max() >= 6:
                    weekend_dates = unique_dates.filter(pl.col('WD') >= 6).select('DATE')
                    if weekend_dates.height and not (
                        weekend_dates.height == 1 and weekend_dates.item() == date(2016, 11, 12)
                    ):
                        self.sanity_checks.add(
                            f'{season} has invalid (weekend) dates: ' + str(weekend_dates.to_series().to_list())
                        )
                if len(unique_dates) > 195:
                    self.sanity_checks.add(f'{season} has too many dates ({len(unique_dates)})')

            if is_syn:
                old_cov = self._coverage_dict.get(season, 0)
                new_cov = self._coverage_dict[season] = len(unique_dates)  # len(df.select(pl.col('EP').unique()))

                c_exprs = [
                    pl.lit(season).alias('S').cast(pl.UInt8),
                    # to do complicated groupbys, must be at least 32 byte int.
                    ((((pl.col('EP') - 1) % 195) + 1) if season <= 36 else pl.col('EP').rank('dense'))
                    .alias('E/S')
                    .cast(pl.UInt64),
                    pl.col('DATE').str.strptime(pl.Date, '%m/%d/%y'),
                    pl.col('UNC').fill_null('').cast(pl.Categorical('lexical')),
                ]

                if 11 <= season <= 12:
                    c_exprs.append(
                        (
                            pl.col('EXTRA').is_not_null()
                            & ~(
                                pl.col('CATEGORY').str.contains(
                                    "(MEGA|WHERE|CLUE|FILL|BLANK|NEXT|SLOGAN|WHERE|WHO|WHAT ARE WE|WHAT'S)"
                                )
                            )
                            & pl.col('BONUS').str.contains(r'^\w+$')
                        ).alias('RL')
                    )
                else:
                    c_exprs.append(pl.lit(False).alias('RL'))

                if season >= 21 or season == 15:
                    c_exprs.append(pl.col('EXTRA').str.contains(r'\*').fill_null(False).alias('PP'))
                else:
                    c_exprs.append(pl.lit(False).alias('PP'))

                if 15 <= season <= 17:
                    c_exprs.append(pl.col('EXTRA').str.ends_with("'").fill_null(False).alias('PR'))
                else:
                    c_exprs.append(pl.lit(False).alias('PR'))

                if season >= 33:
                    w = pl.when(pl.col('EXTRA').str.ends_with('^'))
                    c_exprs.extend(
                        [
                            w.then(pl.col('CATEGORY')).otherwise(pl.lit('')).alias('CLUE/BONUS'),
                            w.then(pl.lit('CROSSWORD'))
                            .otherwise(pl.col('CATEGORY'))
                            .alias('CATEGORY')
                            .cast(pl.Categorical('lexical')),
                        ]
                    )
                else:
                    c_exprs.extend(
                        [
                            pl.col('CATEGORY').cast(pl.Categorical('lexical')),
                            pl.col('BONUS').fill_null('').alias('CLUE/BONUS'),
                        ]
                    )

                self._df_syndicated_dict[season] = (
                    df.with_columns(c_exprs)
                    .select(pl.col(*self._cols['syndicated']))
                    .rename({'ROUND': 'RD', 'UNC': 'UC'})
                    .with_columns(meta_exprs)
                )

                return old_cov != new_cov
            else:
                match season:
                    case 'primetime':
                        c_exprs = [
                            pl.col('DATE').str.strptime(pl.Date, '%m/%d/%y'),
                            pl.col('HH').cast(pl.Categorical),
                            pl.col('EXTRA').str.contains(r'\*').fill_null(False).alias('PP'),
                            pl.col('CATEGORY').cast(pl.Categorical('lexical')),
                        ]

                        self.dfs[season] = (
                            df.with_columns(c_exprs)
                            .select(pl.col(*self._cols['primetime']))
                            .rename({'ROUND': 'RD'})
                            .with_columns(meta_exprs)
                        )
                    case 'kids':
                        self.dfs[season] = df.rename({'ROUND': 'RD'}).with_columns(meta_exprs)
                    case 'gb':
                        self.dfs[season] = (
                            df.with_columns(pl.col('EXTRA').str.ends_with("'").fill_null(False))
                            .rename({'ROUND': 'RD', 'EXTRA': 'PR'})
                            .with_columns(meta_exprs)
                        )
                    case 'daytime' | 'au':
                        c_exprs = [
                            pl.col('CATEGORY').cast(pl.Categorical('lexical')),
                            pl.col('BONUS').fill_null(''),
                        ]
                        self.dfs[season] = df.with_columns(c_exprs).rename({'ROUND': 'RD'}).with_columns(meta_exprs)

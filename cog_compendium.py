import asyncio
import logging
import operator
import re
import string
import traceback
from datetime import *
from functools import reduce
from itertools import product, repeat
from math import ceil
from operator import attrgetter
from typing import List, Optional, Union, Literal

import discord
import discord.ui as dui
import polars as pl
import polars.selectors as cs
import portion as P
from discord import app_commands
from discord.ext import commands
from more_itertools import chunked, split_into, value_chain
from sortedcontainers import SortedSet

from compendium import COMPENDIUM_NOTES, CURRENT_SEASON, WheelCompendium
from util_compendium import (
    CompendiumDownloader,
    build_puzzle_search_expr,
    build_choices_search_expr,
    build_sched_search_expr,
    COL_NAME_REMAPPING,
)
from util import (
    NONNEGATIVE_INT,
    POSITIVE_INT,
    SCHEDULER_TZ,
    add_separator_lines,
    logic_expression,
    season_portion_str_2,
    send_long_mes,
    TimeConverter,
)
from util_expr import pretty_print_polars as ppp, NUM_TO_MULT

_log = logging.getLogger('wayo_log')

SEASON_RANGE = commands.Range[int, 1, CURRENT_SEASON]
PAGE_TYPE = SEASON_RANGE | Literal['primetime', 'kids', 'daytime', 'au', 'gb']


def gen_compendium_submes(df: pl.DataFrame, time: str) -> str:
    if time == 'sched':
        q = df.lazy()

        if 'THEME' in df.columns:
            drop = ['DATE']

            if df.select((pl.col('THEME') == '').all()).item():
                drop.append('THEME')
            else:
                q = q.with_columns(pl.col('THEME').fill_null(''))
            q = q.drop(drop).rename({'DATE_STR': 'DATE'}).with_columns(pl.col('EP').cast(str).str.zfill(4))
        else:
            q = q.with_columns(pl.col('DATE').dt.strftime('%b %d %Y'))
    else:
        q = df.lazy().drop(cs.starts_with('_'))

        if any(v == pl.Boolean for v in df.schema.values()):
            sel = [cs.by_dtype(pl.Boolean).not_().all()]
            if time == 'syndicated':
                # CLUE/BONUS all empty check
                sel.append((cs.contains(('CLUE', 'BONUS')) == '').all())
                # other non-bool col checks
                sel.append((pl.col('UC') == '').all())
            if time in ('syndicated', 'primetime'):
                for col in ('RED', 'YELLOW', 'BLUE', 'THEME'):
                    if col in df.columns:
                        sel.append((pl.col(col) == '').all())

            empty = q.clone().select(sel).collect()

            exclude = [col for r, col in zip(empty.row(0), empty.columns) if r]
            if exclude:
                q = q.drop(exclude)

            c_exprs = [
                pl.when(pl.col(col)).then(pl.lit(col)).otherwise(pl.lit('')).alias(col)
                for col, dtype in q.schema.items()
                if isinstance(dtype, pl.Boolean) and col not in exclude
            ]
            if c_exprs:
                q = q.with_columns(c_exprs)

        wc = []
        if time in ('syndicated', 'primetime'):
            wc.append(pl.col('DATE').dt.strftime('%b %d %Y'))
        if time == 'syndicated':
            wc.extend([pl.col('EP').cast(str).str.zfill(4), pl.col('E/S').cast(str).str.zfill(3)])

        if wc:
            q = q.with_columns(wc)

    return ppp(q.collect())


def compendium_admin_check(ctx):
    return ctx.author.id in {
        149230437850415104,
        688572894036754610,
        314626694650527744,
        186628465376624640,
        542389298872451081,
        150096484908531712,
    }


class TimeFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    time: TimeConverter = commands.flag(aliases=['version'], default='syndicated')


class SearchFlags(TimeFlags):
    logicExpr: logic_expression = commands.flag(aliases=['logic'], default='all')
    conditions: List[str] = commands.flag(
        name='condition', aliases=['cond'], default=lambda ctx: ['rd;br'] if ctx.command.name == 'play' else []
    )
    aggregation: str = commands.flag(aliases=['agg'], default='None')
    random: NONNEGATIVE_INT = commands.flag(aliases=['r'], default=0)
    dateFormat: str = commands.flag(aliases=['format'], default='%m/%d/%y')


# class FrequencyFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
#     regex: str.upper = commands.flag(aliases=['r'], default=None)
#     start: SEASON_RANGE = commands.flag(default=CURRENT_SEASON - 4)
#     end: SEASON_RANGE = commands.flag(default=CURRENT_SEASON)
#     by: POSITIVE_INT = commands.flag(default=1)
#     puzzler: bool = commands.flag(aliases=['p'], default=None)


UNIQUE_LETTER_REGEX = fr'^(?:([{string.ascii_uppercase}])(?!.*\1)){{0,26}}$'


def unique_letters(s):
    if re.match(UNIQUE_LETTER_REGEX, s, re.I):
        return s.upper()
    elif s.lower() in ('nothing', 'none'):
        return ''
    else:
        raise ValueError('Free letters must be unique and all of A-Z.')


class PlayFlags(SearchFlags):
    freeLetters: unique_letters = commands.flag(aliases=['free'], default='default')
    consonants: commands.Range[int, 0, 21] = commands.flag(aliases=['c'], default='default')
    vowels: commands.Range[int, 0, 5] = commands.flag(aliases=['v'], default='default')
    hideMeta: Literal['none', 'date', 'round', 'all'] = commands.flag(aliases=['hide'], default='none')
    singlePlayer: bool = commands.flag(name='single_player', aliases=['single'], default=False)


class CompendiumCog(commands.Cog, name='Compendium'):
    """https://buyavowel.boards.net/page/compendiumindex"""

    def __init__(self, bot):
        self.bot = bot
        self._debug = _log.getEffectiveLevel() == logging.DEBUG
        self.wc = None
        self.wcd = CompendiumDownloader(bot.asession)

    async def cog_load(self):
        self.guint_user = await self.bot.fetch_user(688572894036754610)
        self.cstaff_channel = await self.bot.fetch_channel(1025933082194354358)

    async def cog_before_invoke(self, ctx):
        if not (self.wc and all(v is not None for v in self.wc.dfs.values())):
            try:
                self.wc = WheelCompendium(loop=self.bot.loop, debug=self._debug)

                m = await ctx.send('`I am loading the compendium, just a moment...`')
                while not self.wc.loaded:
                    await asyncio.sleep(1)
                await m.delete()

                if self.wc.sanity_checks:
                    await self.send_sanity()
            except ValueError as e:
                self.wc = None
                await ctx.send('`Loading the Wheel Compendum failed. Try to have a verified user refresh.`')
                _log.error(f'loading wc failed! {e}')

    async def send_sanity(self):
        s = 'There are some inconsistencies when loading, please fix ASAP:\n\n'
        s += '\n'.join(f'- {sc}' for sc in self.wc.sanity_checks)
        await send_long_mes(self.cstaff_channel, s, fn='sanity_check')
        self.wc.sanity_checks.clear()

    # bases

    @commands.hybrid_group(aliases=['wc'], case_insensitive=True)
    async def wheelcompendium(self, ctx):
        """Commands related to the Buy a Vowel boards' compendium of all known Wheel of Fortune puzzles."""
        if not ctx.invoked_subcommand:
            await ctx.send('Invalid subcommand (see `help wheelcompendium`).')

    # end bases

    @wheelcompendium.command(aliases=['r'], with_app_command=False)
    @commands.check(compendium_admin_check)
    async def refresh(self, ctx, pages: commands.Greedy[PAGE_TYPE]):
        """Redownload compendium data from the given syndicated seasons and update wayo.py's version of the compendium.

        Primetime also downloads the BR choices and does a sanity check. Same for syndicated if any season > 35.

        Only Wayoshi, dftackett, 9821, Kev347, and Thetrismix can currently run this command."""
        if not pages:
            raise ValueError('Invalid page provided.')
        elif len(pages) > 5:
            raise ValueError('Currently, must provide between 1 and 5 pages at a time, for load management purposes.')

        await ctx.message.add_reaction('🚧')
        try:
            # this could be done more programmatically, but fine for now
            syn_seasons = {s for s in pages if type(s) is int}
            if syn_seasons & {*range(1, 11)}:
                pages.append('sched10')
            if syn_seasons & {*range(11, 21)}:
                pages.append('sched20')
            if syn_seasons & {*range(21, 31)}:
                pages.append('sched30')
            if syn_seasons & {*range(31, 41)}:
                pages.append('sched40')
            if syn_seasons & {*range(35, 41)}:
                pages.append('choices40')
            if syn_seasons & {*range(41, 51)}:
                pages.append('sched50')
                pages.append('choices50')

            await asyncio.gather(*(self.wcd.dl_page(p) for p in pages))

            if 'primetime' in pages:
                pages.append('choicesprimetime')
                pages.append('schedprimetime')

            await self.wc.load(pages)

            await ctx.message.remove_reaction('🚧', ctx.bot.user)
            if self.wc.sanity_checks:
                await self.send_sanity()
                await ctx.message.add_reaction('⚠')
            else:
                await ctx.message.add_reaction('✅')
        except Exception as e:
            await ctx.send(f'Error refreshing: {e}')
            await ctx.message.remove_reaction('🚧', ctx.bot.user)
            await ctx.message.add_reaction('❌')
            if self._debug:
                traceback.print_exception(e)
            return

        try:
            for channel in (self.cstaff_channel, self.guint_user):
                if ctx.channel != channel:
                    await channel.send(f'{ctx.author.display_name} refreshed `{", ".join(str(p) for p in pages)}`.')
        except Exception as e:
            if self._debug:
                traceback.print_exception(e)

    @wheelcompendium.command(aliases=['s'], with_app_command=False)
    async def search(self, ctx, *, options: SearchFlags):
        """Lists every puzzle in the compendium that matches a set of conditions.

        If 'all' is given to logicExpr (short for logical expression), all the conditions must match. If 'any' is given, at least one condition must match. Custom logic expressions are also allowed, see the FAQ for more details.

        Each condition is a set of "words", separated by a semicolon. See the FAQ for exact formatting details.

        The "aggregation" is by default None - the usual full table output of matching rows will be printed. You can specify one of the following instead:
        -"CATEGORY", "CAT", "ROUND, "RD", "R": Category or Round frequency table. All seasons & categories/rounds with all zeros (columns/rows) will be automatically omitted. The number of columns is determined by an additional "by" parameter, separated by a semicolon like a condition, determining how many seasons to sum up in one column.
        -"PUZZLE", "P": Puzzle frequency list. Simple listing of every puzzle that occurs at least N (default 2) times in the result. N can be supplied just like "by" above.
        -"MULT", "M": Letter multiple table. The number of times a letter was a dud, single, double..., etc. in each puzzle.

        "random" can be specified to output a random sample of the rseulting matching dataset instead off the full one.

        The dataset used is specified by the "time" parameter."""

        async with ctx.typing():
            if options.conditions:
                total_expr, expr_str, cond_descriptions, join = build_puzzle_search_expr(options)

                match join:
                    case 'sched':
                        df = self.wc.join_cache[f'sched_{options.time}']
                    case _:
                        df = self.wc.dfs[options.time]

                sub_df = await asyncio.to_thread(df.filter(total_expr).collect)
                # _log.debug(f'\n{sub_df}')
                plural = 's' if sub_df.height != 1 else ''

                if options.time == 'syndicated':
                    bonus_df = sub_df.lazy().filter(pl.col('CLUE/BONUS') != '').collect()

                    if bonus_df.height:
                        if bonus_df[0, 'S'] >= 33:
                            sub_df = sub_df.rename({'CLUE/BONUS': 'CLUE'})
                        elif bonus_df[-1, 'S'] < 33:
                            sub_df = sub_df.rename({'CLUE/BONUS': 'BONUS'})

                    cov_total_pct = self.wc.calc_coverage(None).select(pl.col('PCT').tail(1)).item()
                    description_str = (
                        f'{sub_df.height} puzzle{plural} found in {options.time.upper()} ({cov_total_pct:.1f}% COV) for'
                    )
                elif options.time == 'daytime':
                    description_str = f'{sub_df.height} puzzle{plural} found in {options.time.upper()} (very incomplete) for'
                else:
                    description_str = f'{sub_df.height} puzzle{plural} found in {options.time.upper()} for'

                if len(cond_descriptions) > 1:
                    description_str += f'{expr_str}\n\n'
                    if expr_str.endswith('where'):
                        description_str += '\n'.join(
                            [f'{l} = {cd}' for cd, l in zip(cond_descriptions, string.ascii_uppercase)]
                        )
                    else:
                        description_str += '\n'.join([f'* {cd}' for cd in cond_descriptions])
                else:
                    description_str += ' ' + cond_descriptions[0]
            else:
                sub_df = self.wc.dfs[options.time].collect()
                description_str = f'{sub_df.height} puzzles make up the whole table for {options.time.upper()} currently.'

            total_str = f'{description_str}\n\n'

            if options.random and options.random < sub_df.height:
                total_str += f'{options.random} chosen randomly\n\n'
                sub_df = sub_df.sample(options.random)

            if sub_df.height:
                match options.aggregation.upper().split(';'):
                    case ['RD' | 'ROUND' | 'R' | 'CAT' | 'CATEGORY' as col, *by]:
                        col = COL_NAME_REMAPPING.get(col, col)

                        if options.time != 'syndicated':
                            raise ValueError('RD/CAT season chart only applicable in syndicated.')

                        season_range = tuple(sub_df.select(pl.col('S').unique(maintain_order=True)).to_series())

                        by = int(by[0]) if by else 1
                        if by < 0:
                            raise ValueError(f'{by} must be non-negative.')

                        s_chunks = list(chunked(season_range, by))

                        vc = (
                            sub_df.lazy()
                            .group_by(col)
                            .agg(pl.col('S').value_counts(sort=True))
                            .collect()
                            .lazy()
                            .select(
                                pl.col(col).cast(str),
                                *[
                                    pl.col('S')
                                    .list.eval(
                                        pl.element().filter(pl.element().struct['S'].is_in(list(s))).struct['count'],
                                        parallel=True,
                                    )
                                    .list.sum()
                                    .alias(season_portion_str_2(s))
                                    for s in s_chunks
                                ],
                            )
                            .sort(col)
                        )

                        description_str = f'{col}, '
                        cov_pct = self.wc.calc_coverage(season_range).select(pl.col('PCT').tail(1)).item()

                        if len(s_chunks) > 1:
                            vc = vc.with_columns(pl.sum_horizontal(pl.exclude(col)).alias('ALL'))

                        df = vc.collect()

                        if df.height > 1:
                            df.extend(df.select(pl.lit('ALL').alias(col), *[pl.sum(c) for c in df.columns[1:]]))

                        ssss = add_separator_lines(ppp(df), df.columns[-2] if len(s_chunks) > 1 else None, df.height > 1)

                        total_str += f'{col} FREQUENCY TABLE, {season_portion_str_2(season_range)} ({by}) ({cov_pct:.1f}% COV)\n\n{ssss}'
                    case ['PUZZLE' | 'P', *n]:
                        n = int(n[0]) if n else 2
                        if n < 2:
                            raise ValueError('N must be at least 2.')

                        vc = (
                            sub_df.lazy()
                            .select(pl.col('PUZZLE').value_counts(sort=True, parallel=True))
                            .unnest('PUZZLE')
                            .filter(pl.col('count') >= n)
                            .select(pl.col('count').alias('N'), pl.col('PUZZLE'))
                            .sort('N', 'PUZZLE', descending=(True, False))
                        )

                        nc = vc.select(pl.col('N').value_counts(sort=True, parallel=True)).reverse().collect()

                        if nc.height:
                            total_str += (
                                'PUZZLE FREQUENCY LIST: '
                                + (', '.join(f'{d[0]["count"]} {d[0]["N"]}x' for d in nc.rows()))
                                + '\n\n'
                                + ppp(vc.collect())
                            )
                        else:
                            total_str += f'PUZZLE FREQUENCY: No puzzles that occurred more than {n} times.'
                    case ['MULT' | 'M', *discard]:
                        agg_df = (
                            sub_df.lazy()
                            .select(pl.col('_lc').explode().struct.rename_fields(('LETTER', 'm')))
                            .unnest('_lc')
                            .group_by('LETTER')
                            .agg(pl.col('m').value_counts())
                            .explode('m')
                            .unnest('m')
                            .collect()
                            .pivot(index='LETTER', columns='m', values='count', sort_columns=True)
                            .fill_null(0)
                            .sort('LETTER')
                        )

                        agg_df = agg_df.select(
                            'LETTER',
                            (sub_df.height - pl.sum_horizontal(cs.numeric())).alias('Duds'),
                            *(
                                pl.col(f'{i}').alias(NUM_TO_MULT[i].title())
                                for i in range(1, 13)
                                if f'{i}' in agg_df.columns
                            ),
                        )

                        total_str += 'LETTER MULTIPLE TABLE\n\n' + ppp(agg_df)
                    case _:
                        total_str += gen_compendium_submes(sub_df, options.time)
            else:
                total_str = description_str

        await send_long_mes(ctx, total_str)

    @wheelcompendium.command(aliases=['sc'], with_app_command=False)
    async def search_choices(self, ctx, *, options: SearchFlags):
        """Lists every set of BR choices in the compendium that matches a set of conditions.

        If 'all' is given to logicExpr (short for logical expression), all the conditions must match. If 'any' is given, at least one condition must match. Custom logic expressions are also allowed, see the FAQ for more details.

        Each condition is a set of "words", separated by a semicolon. See the FAQ for exact formatting details.

        The "aggregation" is by default None - the usual full table output of matching rows will be printed. You can instead specify:
        -"CATEGORY", "CAT": Category Choice Table. Per season on the resulting dataset, will list a fraction of the number of times the category was chosen over the number of times it was a choice.

        "random" can be specified to output a random sample of the rseulting matching dataset instead off the full one.

        The dataset used is specified by the "time" parameter (syndicated and primetime only)."""

        if options.time not in ('syndicated', 'primetime'):
            raise ValueError('Invalid time for this search table.')

        async with ctx.typing():
            if options.conditions:
                total_expr, expr_str, cond_descriptions = build_choices_search_expr(options)
                sub_df = await asyncio.to_thread(self.wc.df_choices[options.time].filter(total_expr).collect)
                # _log.debug(f'\n{sub_df}')
                plural = 's' if sub_df.height != 1 else ''

                if options.time == 'syndicated':
                    description_str = f'{sub_df.height} BR{plural} found in {options.time.upper()} (S35+) for'
                else:
                    description_str = f'{sub_df.height} BR{plural} found in {options.time.upper()} for'

                if len(cond_descriptions) > 1:
                    description_str += f'{expr_str}\n\n'
                    if expr_str.endswith('where'):
                        description_str += '\n'.join(
                            [f'{l} = {cd}' for cd, l in zip(cond_descriptions, string.ascii_uppercase)]
                        )
                    else:
                        description_str += '\n'.join([f'* {cd}' for cd in cond_descriptions])
                else:
                    description_str += ' ' + cond_descriptions[0]
            else:
                sub_df = self.wc.df_choices[options.time].collect()
                description_str = f'{sub_df.height} BRs make up the whole table for {options.time.upper()} currently.'

            total_str = f'{description_str}\n\n'

            if options.random and options.random < sub_df.height:
                total_str += f'{options.random} chosen randomly\n\n'
                sub_df = sub_df.sample(options.random)

            if sub_df.height:
                match options.aggregation.upper():
                    case 'CAT' | 'CATEGORY':
                        s_expr = pl.col('S') if options.time == 'syndicated' else (pl.col('EP') // 100).alias('S')
                        unique_s = sub_df.select(s_expr.unique()).to_series()

                        q = sub_df.with_columns(cs.contains('CH').cast(str))
                        qs = [
                            q.group_by(pl.col(f'CHOICE{i}').alias('CATEGORY'), s_expr).agg(
                                pl.count().alias('CHOICE'),
                                pl.col('CHOSEN').filter(pl.col('CHOSEN') == pl.col(f'CHOICE{i}')).count().alias('CHOSEN'),
                            )
                            for i in range(1, 4)
                        ]

                        raw_agg_df = (
                            reduce(lambda df1, df2: df1.vstack(df2), qs)
                            .fill_null(0)
                            .groupby('CATEGORY', 'S')
                            .sum()
                            .with_columns(pct=(100 * pl.col('CHOSEN') / pl.col('CHOICE')).round(1))
                            .pivot(
                                index='CATEGORY', columns='S', values=['CHOSEN', 'CHOICE', 'pct'], aggregate_function='sum'
                            )
                            .with_columns(
                                pl.sum_horizontal(cs.starts_with('CHOSEN').fill_null(0)).alias('ALL_CHOSEN'),
                                pl.sum_horizontal(cs.starts_with('CHOICE').fill_null(0)).alias('ALL_CHOICE'),
                            )
                            .with_columns((100 * pl.col('ALL_CHOSEN') / pl.col('ALL_CHOICE')).round(1).alias('ALL_PCT'))
                        )

                        # print(raw_agg_df.select(pl.col('ALL_CHOSEN').sum()).item(), raw_agg_df.select(pl.col('ALL_CHOICE').sum()).item())

                        max_log10 = raw_agg_df.select(cs.numeric().log10().max().cast(pl.UInt8)).row(index=0, named=True)

                        agg_df = (
                            raw_agg_df.select(
                                'CATEGORY',
                                *(
                                    pl.format(
                                        '{}/{} ({}%)',
                                        pl.col(f'CHOSEN_S_{s}')
                                        .cast(pl.String)
                                        .str.pad_start(1 + max_log10[f'CHOSEN_S_{s}']),
                                        pl.col(f'CHOICE_S_{s}')
                                        .cast(pl.String)
                                        .str.pad_start(1 + max_log10[f'CHOICE_S_{s}']),
                                        pl.col(f'pct_S_{s}').cast(pl.String).str.pad_start(3 + max_log10[f'pct_S_{s}']),
                                    ).alias(f'S{s}')
                                    for s in unique_s
                                ),
                                pl.format(
                                    '{}/{} ({}%)',
                                    pl.col('ALL_CHOSEN').cast(pl.String).str.pad_start(1 + max_log10['ALL_CHOSEN']),
                                    pl.col('ALL_CHOICE').cast(pl.String).str.pad_start(1 + max_log10['ALL_CHOICE']),
                                    pl.col('ALL_PCT').cast(pl.String).str.pad_start(3 + max_log10['ALL_PCT']),
                                ).alias('ALL'),
                            )
                            .fill_null('-/-')
                            .sort('CATEGORY')
                        )

                        if len(unique_s) > 1:
                            ssss = add_separator_lines(ppp(agg_df), agg_df.columns[-2], False)
                        else:
                            # breaking up the above to remove the ALL parts
                            # error'ed for some reason, so while inefficient
                            # this is simplest for now.
                            ssss = add_separator_lines(ppp(agg_df.drop('ALL')), None, False)

                        total_str += f'CATEGORY CHOICE TABLE\n\n{ssss}'
                    case _:
                        total_str += gen_compendium_submes(sub_df, options.time)
            else:
                total_str = description_str

        await send_long_mes(ctx, total_str)

    @wheelcompendium.command(aliases=['ss'], with_app_command=False)
    async def search_sched(self, ctx, *, options: SearchFlags):
        """Lists every set of contestant names and themes in the compendium that matches a set of conditions.

        If 'all' is given to logicExpr (short for logical expression), all the conditions must match. If 'any' is given, at least one condition must match. Custom logic expressions are also allowed, see the FAQ for more details.

        Each condition is a set of "words", separated by a semicolon. See the FAQ for exact formatting details.

        No aggregation output type is supported at this time.

        "random" can be specified to output a random sample of the rseulting matching dataset instead off the full one.
        The dataset used is specified by the "time" parameter (syndicated and primetime only)."""

        if options.time not in ('syndicated', 'primetime'):
            raise ValueError('Invalid time for this search table.')

        async with ctx.typing():
            if options.conditions:
                total_expr, expr_str, cond_descriptions = build_sched_search_expr(options)
                sub_df = await asyncio.to_thread(self.wc.df_sched[options.time].filter(total_expr).collect)
                # _log.debug(f'\n{sub_df}')
                plural = 's' if sub_df.height != 1 else ''

                description_str = f'{sub_df.height} episode{plural} found in {options.time.upper()} SCHEDULE for'

                if len(cond_descriptions) > 1:
                    description_str += f'{expr_str}\n\n'
                    if expr_str.endswith('where'):
                        description_str += '\n'.join(
                            [f'{l} = {cd}' for cd, l in zip(cond_descriptions, string.ascii_uppercase)]
                        )
                    else:
                        description_str += '\n'.join([f'* {cd}' for cd in cond_descriptions])
                else:
                    description_str += ' ' + cond_descriptions[0]
            else:
                sub_df = self.wc.df_sched[options.time].collect()
                description_str = f'{sub_df.height} episodes make up the whole table for {options.time.upper()} currently.'

            total_str = f'{description_str}\n\n'

            if options.random and options.random < sub_df.height:
                total_str += f'{options.random} chosen randomly\n\n'
                sub_df = sub_df.sample(options.random)

            if sub_df.height:
                total_str += gen_compendium_submes(sub_df, 'sched')
            else:
                total_str = description_str

        await send_long_mes(ctx, total_str)

    @wheelcompendium.command(aliases=['pc'], description='Gives the total puzzle count in the given seasons.')
    async def puzzle_count(
        self, ctx, seasons: commands.Greedy[SEASON_RANGE], range: Optional[bool] = False, *, options: TimeFlags
    ):
        """Gives the total puzzle count in the compendium in the given seasons (all by default, if range is True it will treat each pair of inputs as an inclusive range) compendium without any further results.

        If time is not syndicated, all other arguments are ignored."""
        q = self.wc.dfs[options.time]
        if options.time == 'syndicated' and seasons:
            if range:
                seasons = list(
                    P.iterate(
                        reduce(
                            operator.or_,
                            [P.closed(*s) if len(s) > 1 else P.singleton(*s) for s in chunked(seasons, 2)],
                            P.empty(),
                        ),
                        step=1,
                    )
                )
            q = q.filter(pl.col('S').is_in(seasons))
        await ctx.send(f'`{q.collect().height}`')

    @wheelcompendium.command(
        aliases=['cov'],
        description='Calculates the number of unique shows covered in the given seasons.',
    )
    async def coverage(self, ctx, seasons: commands.Greedy[SEASON_RANGE], do_range: bool = False):
        """The compendium is incomplete at points. This command calculates the number of unique shows it has in the given seasons (all by default, if doRange is True it will treat each pair of inputs as an inclusive range) and gives a percentage of "coverage" (COV). Only syndicated seasons are supported for this command.

        There are 195 shows every syndicated season, with the exceptions of the pandemic season 37, when there were 167, and S16 and 21 had one clip show each, so effectively 194 true shows. The current season is assumed to be at max coverage at all times.

        The compendium has several incomplete shows not in the database, that will go in if full copies of the show are ever found: https://buyavowel.boards.net/page/compendiumapp (Incomplete Shows)
        """

        await send_long_mes(ctx, ppp(self.wc.calc_coverage(tuple(seasons), do_range).fill_null('')))

    @wheelcompendium.command(aliases=['br'], with_app_command=False)
    @commands.max_concurrency(1, commands.BucketType.channel)
    async def play(self, ctx, *, options: PlayFlags):
        """Play a random round from the compendium, bonus-round style. The sample pool is determined in the same way as running the search command (by default, all BRs).

        What letters are given for free and the number of consonants & vowels allowed to be called after are customizable with the keyword parameters above. By default the bot will give RSTLNE 3/1 for puzzles on or after Oct 1988, and (nothing) 5/1 otherwise. "Nothing" or "none" can be given to the "free" parameter to force no free letters.

        You will have 60 seconds to give a set number of consonants and vowels as its own message (or the bot will "time out" and stop playing), then 15 seconds after that to solve in another separate message. You can instantly solve without giving letters if you know it.

        Use the hide keyword argument to hide either the "date", "round", or "all", info until the command ends. This carries some risk as certain categories were misleading in the past (e.g. THING could be FOOD & DRINK), etc.

        The bot will scan all users' messages for letters/puzzle guesses unless single=True is specified, then the bot will only respond to the command giver's messages.
        """

        total_expr, _, _, _ = build_puzzle_search_expr(options)

        sub_df = await asyncio.to_thread(self.wc.dfs[options.time].filter(total_expr).collect)
        if not sub_df.height:
            raise ValueError('No puzzles found to sample from (check the parameters passed in).')

        row = sub_df.sample(1)  # .filter(pl.col('DATE') == date(1992, 10, 5))

        d, r, p, c, clue = row.select(pl.col('DATE', 'RD', 'PUZZLE', 'CATEGORY', 'CLUE/BONUS')).row(0)

        words = [' '.join(w) for w in p.split(' ')]
        if len(words) > 1 and c != 'CROSSWORD':
            if r == 'BR':
                spl = ceil(len(words) / 2)
                p_str = (' / '.join(words[:spl])) + '\n' + (' / '.join(words[spl:]))
            else:
                si = []
                l_c = 0
                s = 0
                for w in p.split(' '):
                    l_c += len(w)
                    s += 1
                    if l_c > 14:
                        si.append(s - 1)
                        l_c = len(w) + 1
                        s = 1
                    else:
                        l_c += 1
                if sum(si) < len(words):
                    si.append(None)
                p_str = '\n'.join(' / '.join(line) for line in split_into(words, si))
        else:
            p_str = ' / '.join(words)

        d_s = d.strftime('%b %d %Y')
        if c == 'CROSSWORD':
            c += f' ({clue})'

        retro = d < date(1988, 10, 3) and r == 'BR'

        given_letters = ('' if retro else 'RSTLNE') if options.freeLetters == 'default' else options.freeLetters
        blank_vowel_letters = set('AEIOU') - set(given_letters)
        blank_letters = set(string.ascii_uppercase) - set(given_letters)
        bl_str = ''.join(blank_letters)

        if options.consonants == 'default':
            options.consonants = 5 if retro else 3
        if options.vowels == 'default':
            options.vowels = 1
        count_letters = options.consonants + options.vowels

        if len(blank_vowel_letters) < options.vowels:
            raise ValueError('Not enough vowels left to call.')
        elif len(set(blank_letters) - set('AEIOU')) < options.consonants:
            raise ValueError('Not enough consonants left to call.')

        # create embed.
        embed = discord.Embed(title='Wheel Compendium Play')
        embed.set_footer(text=f'Sample size: {sub_df.height}')

        regex_letters = fr'^(?:([{bl_str}])(?!.*\1)){{{count_letters}}}$'

        current_p = re.sub(f"[{bl_str}]", '_', p_str)
        if '_' not in current_p:
            embed.description = (
                f'```{current_p}\n\n{c} ({d_s}, {r})\n{given_letters}\n\nHey, what are you trying to pull!?```'
            )
            await ctx.send(embed=embed)
            return
        if count_letters:
            letter_str = ' and '.join(
                '{} {}{}'.format(e, word, '' if e == 1 else 's')
                for e, word in zip((options.consonants, options.vowels), ('consonant', 'vowel'))
            )
            bottom_str = f'You have 60 seconds to give {letter_str}, or just type it if you know it!'
        else:
            bottom_str = f"That's all you're getting. You have 60 seconds to type it if you know it!"

        match options.hideMeta:
            case 'none':
                meta_str = f' ({d_s}, {r})'
            case 'round':
                meta_str = f' ({d_s})'
            case 'date':
                meta_str = f' ({r})'
            case 'all':
                meta_str = ''

        embed.description = f'```\n{current_p}\n\n{c}{meta_str}\n{given_letters}\n\n{bottom_str}```'
        m = await ctx.send(embed=embed)

        def check(letters: bool):
            def inner_check(m):
                if m.channel != ctx.channel or (options.singlePlayer and m.author != ctx.author):
                    return False
                mc = re.sub("‘|’|`", "'", m.content.upper())
                if mc == p:
                    return True
                elif letters:
                    return re.match(regex_letters, mc) and len(set(mc) & blank_vowel_letters) == options.vowels
                else:
                    return False

            return inner_check

        if count_letters:
            t1 = datetime.now()
            try:
                m1 = await ctx.bot.wait_for('message', check=check(True), timeout=60)
                t2 = datetime.now()
            except asyncio.TimeoutError:
                embed.description = f'```\n{p_str}\n\n{c} ({d_s}, {r})\n{given_letters}\n\nUser timed out.```'
                await m.edit(embed=embed)
                return

            letters = m1.content.upper()
            if letters == p:
                t = (t2 - t1).total_seconds()
                embed.description = f'```\n{p_str}\n\n{c} ({d_s}, {r})\n{given_letters}\n\nVery well done! ({t:.2f} sec)```'
                await m.edit(embed=embed)
                return
            else:
                blank_letters -= set(letters)
                bl_str = ''.join(blank_letters)
                current_p = re.sub(f"[{bl_str}]", '_', p_str)
                bottom_str = f'\n{given_letters} {letters}' if given_letters else f'\n{letters}'
                if '_' in current_p:
                    embed.description = f'```\n{current_p}\n\n{c}{meta_str}\n{bottom_str}\n15 seconds, GO!```'
                    await m.edit(embed=embed)
        else:
            bottom_str = ''

        if '_' in current_p:
            t2 = datetime.now()
            try:
                timer = 15 if count_letters else 60
                m2 = await ctx.bot.wait_for('message', check=check(False), timeout=timer)
                t3 = datetime.now()
                t = timer - (t3 - t2).total_seconds()
                embed.description = f'```\n{p_str}\n\n{c} ({d_s}, {r}){bottom_str}\n\nNicely done! ({t:.2f} sec left)```'
            except asyncio.TimeoutError:
                embed.description = f'```\n{p_str}\n\n{c} ({d_s}, {r}){bottom_str}\n\nNot this time.```'
        else:
            embed.description = f'```{p_str}\n\n{c} ({d_s}, {r}){bottom_str}\n\nWell look at you!!```'

        await m.edit(embed=embed)

    @wheelcompendium.command(aliases=['a', 'notes', 'n'])
    async def addendum(self, ctx):
        """Prints a static message with some extra explanation on a few oddities in the compendium database.

        For more, see https://buyavowel.boards.net/page/compendiumapp"""
        await ctx.send(COMPENDIUM_NOTES)

    async def cog_command_error(self, ctx, e):
        if isinstance(e, (commands.errors.MissingRequiredArgument, commands.errors.MissingRequiredFlag)):
            if ctx.command.name == 'search':
                await ctx.send('At least one condition required.')
            elif ctx.command.name == 'play':
                await ctx.send('At least one condition required. (Suggestion for play: `cond=rd;br`)')
            else:  # freq
                await ctx.send('`Must specify column.`')
        elif isinstance(e, commands.CommandInvokeError):
            if isinstance(e.__cause__, pl.exceptions.ComputeError):
                await ctx.send(f'Error computing query:\n```\n{e.__cause__}\n```')
            else:  # if isinstance(e.__cause__, ValueError):
                await ctx.send(f'`{e.__cause__}`')
        elif isinstance(e, commands.CheckFailure):
            await ctx.send('`Only compendium maintainers can run this command.`')
        elif isinstance(e, commands.MaxConcurrencyReached):
            await ctx.send('`Only one puzzle can be played per channel at a time.`')
        elif isinstance(e, commands.BadFlagArgument):
            if isinstance(e.original, commands.BadLiteralArgument):
                await ctx.send(f'`Parameter "{e.flag.name}" must be one of {e.original.literals} (case-sensitive).`')
            elif isinstance(e.original, commands.RangeError):
                await ctx.send(f'`Parameter "{e.flag.name}": {e.original}`')
            else:
                await ctx.send(f'`Parameter "{e.flag.name}" was improperly formatted: {e.argument}`')
        else:
            await ctx.send(f'`{e}`')  # wayo.py handler

        if self._debug:
            traceback.print_exception(e)


async def setup(bot):
    await bot.add_cog(CompendiumCog(bot))

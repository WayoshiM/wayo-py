import asyncio
import logging
import operator
import re
import string
from datetime import *
from functools import reduce
from itertools import product, repeat
from math import ceil
from operator import attrgetter
from typing import List, Optional, Union, Literal

import discord
import discord.ui as dui
import polars as pl
import portion as P
from discord import app_commands
from discord.ext import commands
from humanize import ordinal
from more_itertools import chunked, split_into, value_chain
from sortedcontainers import SortedSet

from compendium import COMPENDIUM_NOTES, CURRENT_SEASON, WheelCompendium, dl_season
from util import (
    NONNEGATIVE_INT,
    POSITIVE_INT,
    SCHEDULER_TZ,
    logic_expression,
    season_portion_str_2,
    send_long_mes,
    TimeConverter,
)
from util_expr import (
    pretty_print_polars as ppp,
    build_int_expression,
    build_date_expression,
    build_dt_q_expression,
    transform_str_to_dts,
)

_log = logging.getLogger('wayo_log')

SEASON_RANGE = commands.Range[int, 1, CURRENT_SEASON]
SEASON_TYPE = SEASON_RANGE | Literal['primetime', 'kids', 'daytime']


def ordinal_adjust(idx):
    if idx > 0:
        sub_cd = ordinal(idx)
        idx -= 1
    elif idx == 0:
        raise ValueError('"Zeroth" is invalid.')
    else:
        sub_cd = ordinal(-idx) + '-to-last' if idx < -1 else 'last'
    return idx, sub_cd


def gen_compendium_submes(df: pl.DataFrame, time: str) -> str:
    q = df.lazy().select(pl.exclude('^_.+$'))

    bool_cols = [col for col, dtype in df.schema.items() if dtype is pl.Boolean]

    if bool_cols:
        sel = [pl.col(bool_cols).is_not().all()]
        if time == 'syndicated':
            sel.append((pl.col(q.columns[-1]).str.lengths() == 0).all())

        empty = q.clone().select(sel).collect()

        exclude = [col for r, col in zip(empty.row(0), empty.columns) if r]
        if exclude:
            q = q.select(pl.exclude(exclude))

        c_exprs = [
            pl.when(pl.col(col)).then(pl.lit(col)).otherwise(pl.lit('')).alias(col)
            for col, dtype in q.schema.items()
            if dtype is pl.Boolean and col not in exclude
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
    output: str = commands.flag(aliases=['o'], default='full')
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


_col_name_remapping = {
    'SEASON': 'S',
    'EPISODE': 'EP',
    'ES': 'E/S',
    'ROUND': 'RD',
    'CAT': 'CATEGORY',
    'P': 'PUZZLE',
    'CB': 'CLUE/BONUS',
    'B': 'CLUE/BONUS',
    'CLUE': 'CLUE/BONUS',
    'BONUS': 'CLUE/BONUS',
    'D': 'DATE',
}

_letters_mapping = {'CONSONANT': 'BCDFGHJKLMNPQRSTVWXYZ', 'ALL': string.ascii_uppercase}
_word_regex = r"\b[A-Z-'\.]+\b"


class CompendiumCog(commands.Cog, name='Compendium'):
    """https://buyavowel.boards.net/page/compendiumindex"""

    def __init__(self, bot):
        self.bot = bot
        self._debug = _log.getEffectiveLevel() == logging.DEBUG
        try:
            self.wc = WheelCompendium(loop=self.bot.loop, debug=self._debug)
        except ValueError as e:
            self.wc = None
            _log.error(f'loading wc failed! {e}')

    async def cog_load(self):
        self.guint_user = await self.bot.fetch_user(688572894036754610)

    async def cog_check(self, ctx):
        return self.wc and all(v is not None for v in self.wc.dfs.values())

    # bases

    @commands.hybrid_group(aliases=['wc'], case_insensitive=True)
    async def wheelcompendium(self, ctx):
        """Commands related to the Buy a Vowel boards' compendium of all known Wheel of Fortune puzzles."""
        if not ctx.invoked_subcommand:
            await ctx.send('Invalid subcommand (see `help wheelcompendium`).')

    # end bases

    @wheelcompendium.command(aliases=['r'], with_app_command=False)
    @commands.check(compendium_admin_check)
    async def refresh(self, ctx, seasons: commands.Greedy[SEASON_TYPE]):
        """Redownload compendium data from the given syndicated seasons (all seasons if not provided) and update wayo.py's version of the compendium.

        Only Wayoshi, dftackett, 9821, Kev347, and Thetrismix can currently run this command."""
        if not seasons:
            seasons = range(1, CURRENT_SEASON + 1)
        await ctx.message.add_reaction('🚧')
        try:
            await asyncio.gather(*(dl_season(s, self.bot.asession) for s in seasons))
            await self.wc.load(seasons)
            await ctx.message.remove_reaction('🚧', ctx.bot.user)
            await ctx.message.add_reaction('✅')
        except Exception as e:
            await ctx.send(f'Error refreshing: {e}')
            await ctx.message.remove_reaction('🚧', ctx.bot.user)
            await ctx.message.add_reaction('❌')
            return

        if not self._debug:
            try:
                await self.guint_user.send(f'{ctx.author.name} refreshed {seasons} at {datetime.now()}')
            except:
                pass

    def search_inner(self, options):
        f_exprs = []
        cond_descriptions = []

        if len(options.conditions) > 26:
            raise ValueError("Too many conditions given, max is 26. (You shouldn't need close to this many!)")
        # if options.time not in ('syndicated', 'primetime'):
        # raise ValueError('Only syndicated & primetime supported at the moment.')

        for cond in options.conditions:
            words = [w.strip().upper() for w in cond.split(';')]

            match words:
                case ['BONUS' | 'B']:
                    if options.time in ('primetime', 'kids', 'gb'):
                        raise ValueError(f'BONUS is invalid in {options.time}.')

                    col = 'BONUS' if options.time in ('daytime', 'au') else 'CLUE/BONUS'

                    f = (pl.col(col).str.lengths() > 0) & (~pl.col('CATEGORY').cast(str).str.contains('CROSSWORD'))
                    cd = 'has a bonus'
                case ['BONUS' | 'B' as col, '1' | '0' | 'YES' | 'NO' | 'Y' | 'N' | 'T' | 'F' | 'TRUE' | 'FALSE' as b]:
                    if options.time in ('primetime', 'kids', 'gb'):
                        raise ValueError(f'BONUS is invalid in {options.time}.')

                    col = 'BONUS' if options.time in ('daytime', 'au') else 'CLUE/BONUS'
                    b = re.match('[1YT]', b)

                    f = (pl.col('BONUS').str.lengths() > 0) & (~pl.col('CATEGORY').cast(str).str.contains('CROSSWORD'))
                    if not b:
                        f = f.is_not()
                    cd = 'has a bonus' if b else 'does not have a bonus'
                case [
                    'PUZZLE'
                    | 'P'
                    | 'CLUE/BONUS'
                    | 'CLUE'
                    | 'BONUS'
                    | 'CB'
                    | 'B'
                    | 'RD'
                    | 'R'
                    | 'ROUND'
                    | 'CAT'
                    | 'CATEGORY' as col,
                    lit,
                    'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as p_q,
                ]:
                    col = _col_name_remapping.get(col, col)
                    if col == 'CLUE/BONUS':
                        if options.time in ('primetime', 'kids', 'gb'):
                            raise ValueError(f'{col} is invalid in {options.time}.')
                        elif options.time in ('daytime', 'au'):
                            col = 'BONUS'

                    if p_q.startswith('L'):
                        f = pl.col(col).str.contains(lit, literal=True)
                        cd = f'{col} contains "{lit}"'
                    else:
                        f = pl.col(col).str.contains(f'^{lit}$')
                        cd = f'{col} is exactly "{lit}"'
                case ['PUZZLE' | 'P' | 'CLUE/BONUS' | 'CLUE' | 'BONUS' | 'CB' | 'B' as col, regex, *e]:
                    col = _col_name_remapping.get(col, col)
                    if col == 'CLUE/BONUS':
                        if options.time in ('primetime', 'kids', 'gb'):
                            raise ValueError(f'{col} is invalid in {options.time}.')
                        elif options.time in ('daytime', 'au'):
                            col = 'BONUS'

                    if options.time == 'primetime' and col != 'PUZZLE':
                        raise ValueError(f'{col} is invalid in primetime.')
                    regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)

                    if e:
                        f, cd, p = build_int_expression(pl.col(col).str.count_match(regex), e)
                        cd = f'{col} matches "{regex}" {cd} time' + ('s' if p else '')
                    else:
                        f = pl.col(col).str.contains(regex)
                        cd = f'{col} matches "{regex}"'
                case ['RD' | 'ROUND' | 'R' | 'CAT' | 'CATEGORY' as col, regex]:
                    col = _col_name_remapping.get(col, col)
                    regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
                    if col == 'CATEGORY' and ':tm:' in col:
                        regex = regex.replace(':tm:', '™️')

                    f = pl.col(col).cast(str).str.contains(regex)
                    cd = f'{col} matches "{regex}"'
                case ['PUZZLE/EP' | 'PUZ/EP' | 'P/E' | 'PE', idx]:
                    idx, sub_cd = ordinal_adjust(int(idx))
                    f = pl.col('RD') == pl.col('RD').cast(str).list().over(pl.col('DATE')).arr.get(idx)
                    cd = f'PUZZLE is the {sub_cd} of EP (DATE)'
                case ['SEASON' | 'S' | 'EPISODE' | 'EP' | 'ES' | 'E/S' as col, *e]:
                    col = _col_name_remapping.get(col, col)
                    if options.time != 'syndicated' and not (options.time == 'primetime' and col == 'EP'):
                        raise ValueError(f'{col} is invalid in {options.time}.')

                    f, cd, _ = build_int_expression(pl.col(col), e)
                    cd = f'{col} is {cd}'
                case ['HH', 'A' | 'B' as hh]:
                    if options.time != 'primetime':
                        raise ValueError('HH is only valid in primetime.')
                    f = pl.col('HH') == hh
                    cd = f'HH is {hh}'
                case [
                    'DATE' | 'D' as col,
                    'YEAR' | 'Y' | 'MONTH' | 'M' | 'DAY' | 'D' | 'DOW' | 'WKDAY' | 'WEEKDAY' as dt_q,
                    *e,
                ]:
                    if options.time in ('kids', 'daytime', 'au', 'gb'):
                        raise ValueError(f'DATE is too incomplete to search specifics on for {options.time}.')
                    col = _col_name_remapping.get(col, col)
                    f, cd = build_dt_q_expression(col, dt_q, e)
                case ['DATE' | 'D' as col, *e]:
                    col = _col_name_remapping.get(col, col)
                    if options.time in ('kids', 'daytime', 'au', 'gb'):
                        regex = ' '.join(e)
                        f = pl.col(col).str.contains(regex)
                        cd = f'{col} matches "{regex}"'
                    else:
                        e = transform_str_to_dts(e, options.dateFormat)
                        f, cd = build_date_expression(pl.col(col), e, options.dateFormat)
                        cd = f'{col} is {cd}'
                case ['LENGTH' | 'LC' | 'L', *e]:
                    f, cd, _ = build_int_expression(pl.col('PUZZLE').str.extract_all('[A-Z]').arr.lengths(), e)
                    cd = f'length is {cd}'
                case ['LENGTH_UNIQUE' | 'LCU' | 'LU', *e]:
                    f, cd, _ = build_int_expression(pl.col('PUZZLE').str.extract_all('[A-Z]').arr.unique().arr.lengths(), e)
                    cd = f'total number of unique letters is {cd}'
                case ['COUNT' | 'C' | 'COUNT_UNIQUE' | 'CU' as col, letters, *e]:
                    if letters in _letters_mapping:
                        letters = _letters_mapping[letters]
                    elif not re.match('[A-Z]+', letters) or not len(set(letters)) == len(letters):
                        raise ValueError(f'Malformed letter string (must be all A-Z and all unique): {letters}')

                    base_expr = pl.col('PUZZLE').str.extract_all('[A-Z]')
                    if 'U' in col:
                        base_expr = base_expr.arr.unique()
                        extra = ' unique'
                    else:
                        extra = ''

                    f, cd, _ = build_int_expression(
                        base_expr.arr.eval(pl.element().is_in(list(letters)), parallel=True).arr.sum(),
                        e,
                    )
                    cd = f'total{extra} number of {letters} is {cd}'
                case ['WORD_COUNT' | 'WC', *e]:
                    f, cd, _ = build_int_expression(pl.col('PUZZLE').str.count_match(_word_regex), e)
                    cd = f'total word count is {cd}'
                case ['WORD' | 'W', regex]:
                    regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
                    f = (
                        pl.col('PUZZLE')
                        .str.extract_all(_word_regex)
                        .arr.eval(pl.element().str.contains(regex), parallel=True)
                        .arr.contains(True)
                    )
                    cd = f'any word matches "{regex}"'
                case ['WORD' | 'W', word, 'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as w_q]:
                    if w_q.startswith('L'):
                        f = (
                            pl.col('PUZZLE')
                            .str.extract_all(_word_regex)
                            .arr.eval(pl.element().str.contains(word, literal=True), parallel=True)
                            .arr.contains(True)
                        )
                        cd = f'any word contains "{word}"'
                    else:
                        f = pl.col('PUZZLE').str.extract_all(_word_regex).arr.contains(word)
                        cd = f'any word is exactly "{word}"'
                case ['WORD' | 'W', regex, idx]:
                    idx = int(idx)
                    if idx > 0:
                        sub_cd = ordinal(idx)
                        idx -= 1
                    elif idx == 0:
                        raise ValueError('Zeroth word is not defined.')
                    else:
                        sub_cd = ordinal(-idx) + '-to-last' if idx < -1 else 'last'
                    regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)

                    f = pl.col('PUZZLE').str.extract_all(_word_regex).arr.get(idx).str.contains(regex)
                    cd = f'{sub_cd} word matches "{regex}"'
                case ['WORD' | 'W', word, 'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as w_q, idx]:
                    idx, sub_cd = ordinal_adjust(int(idx))

                    base_expr = pl.col('PUZZLE').str.extract_all(_word_regex).arr.get(idx)

                    if w_q.startswith('L'):
                        f = base_expr.str.contains(word, literal=True)
                        cd = f'{sub_cd} word contains "{word}"'
                    else:
                        f = base_expr == word
                        cd = f'{sub_cd} word is exactly "{word}"'
                case ['MULT' | 'M', letters, mults, *e]:
                    if letters in _letters_mapping:
                        letters = _letters_mapping[letters]
                    elif not re.match('[A-Z]+', letters) or not len(set(letters)) == len(letters):
                        raise ValueError(f'Malformed letter string (must be all A-Z and all unique): {letters}')

                    try:
                        _, mult_cd, _ = build_int_expression(pl.col('DUMMY'), [mults], mult_hybrid=True)
                    except KeyError:
                        raise ValueError('All multiples must be between 1 (single) and 12 (dodecuple).')

                    if not e:
                        e = ['>=1']

                    f, cd, _ = build_int_expression(
                        pl.col('_lc')
                        .arr.eval(
                            (pl.element().struct.field('').is_in(list(letters)))
                            & (build_int_expression(pl.element().struct.field('counts'), [mults], expr_only=True)),
                            parallel=True,
                        )
                        .arr.sum(),
                        e,
                    )
                    cd = f'number of {mult_cd} of "{letters}" is {cd}'
                case ['UC' | 'PP' | 'PR' | 'RL' as col]:
                    if options.time != 'syndicated' and not (
                        (options.time == 'primetime' and col == 'PP') or (options.time == 'gb' and col == 'PR')
                    ):
                        raise ValueError(f'{col} is invalid in {options.time}.')
                    f = pl.col(col)
                    cd = f'is a {col} puzzle'
                case [
                    'UC' | 'PP' | 'PR' | 'RL' as col,
                    '1' | '0' | 'YES' | 'NO' | 'Y' | 'N' | 'T' | 'F' | 'TRUE' | 'FALSE' as b,
                ]:
                    if options.time != 'syndicated' and not (
                        (options.time == 'primetime' and col == 'PP') or (options.time == 'gb' and col == 'PR')
                    ):
                        raise ValueError(f'{col} is invalid in {options.time}.')

                    b = re.match('[1YT]', b)

                    f = pl.col(col) if b else pl.col(col).is_not()
                    cd = f'is a {col} puzzle' if b else f'is not a {col} puzzle'
                case _:
                    raise ValueError(f'Malformed condition: {cond}')

            f_exprs.append(f)
            cond_descriptions.append(cd)

        if options.logicExpr == 'all':
            total_expr = pl.all(f_exprs)
            expr_str = ' all of'
        elif options.logicExpr == 'any':
            total_expr = pl.any(f_exprs)
            expr_str = ' any of'
        else:
            l = locals()
            l |= {letter: fe for fe, letter in zip(f_exprs, string.ascii_uppercase)}
            total_expr = eval(re.sub('([A-Z])', r'(\1)', options.logicExpr))
            expr_str = f'\n{options.logicExpr}; where'

        return self.wc.dfs[options.time].filter(total_expr).collect(), expr_str, cond_descriptions

    @wheelcompendium.command(aliases=['s'], with_app_command=False)
    async def search(self, ctx, *, options: SearchFlags):
        """Lists every puzzle in the compendium that matches a set of conditions.

        If 'all' is given to logicExpr (short for logical expression), all the conditions must match. If 'any' is given, at least one condition must match. Custom logic expressions are also allowed, see the pastebin for more details.

        Each condition is a set of "words", separated by a semicolon. See the pastebin for exact formatting details.

        The "output" is by default the usual full table output of matching rows. You can specify one of the following instead:
        -"CATEGORY", "CAT", "ROUND, "RD", "R": Category or Round frequency table. All seasons & categories/rounds with all zeros (columns/rows) will be automatically omitted. The number of columns is determined by an additional "by" parameter, separated by a semicolon like a condition, determining how many seasons to sum up in one column.
        -"PUZZLE", "P": Puzzle frequency list. Simple listing of every puzzle that occurs at least N (default 2) times in the result. N can be supplied just like "by" above.

        The dataset used is specified by the "time" parameter. Currently only "syndicated" is supported."""

        if not options.conditions:
            raise ValueError('At least one condition required.')

        async with ctx.typing():
            sub_df, expr_str, cond_descriptions = await asyncio.to_thread(self.search_inner, options)
            # _log.debug(f'\n{sub_df}')
            plural = 's' if sub_df.height != 1 else ''

            if options.time == 'syndicated':
                bonus_df = sub_df.lazy().filter(pl.col('CLUE/BONUS').str.lengths() > 0).collect()

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
                    description_str += '\n'.join([f'{l} = {cd}' for cd, l in zip(cond_descriptions, string.ascii_uppercase)])
                else:
                    description_str += '\n'.join([f'* {cd}' for cd in cond_descriptions])
            else:
                description_str += ' ' + cond_descriptions[0]

            total_str = f'{description_str}\n\n'

            if sub_df.height:
                match options.output.upper().split(';'):
                    case ['RD' | 'ROUND' | 'R' | 'CAT' | 'CATEGORY' as col, *by]:
                        if options.time != 'syndicated':
                            raise ValueError('RD/CAT season chart only applicable in syndicated.')

                        season_range = tuple(sub_df.select(pl.col('S').unique(True)).to_series())

                        by = int(by[0]) if by else 1
                        if by < 0:
                            raise ValueError(f'{by} must be non-negative.')

                        s_chunks = list(chunked(season_range, by))
                        col = _col_name_remapping.get(col, col)

                        vc = (
                            sub_df.lazy()
                            .groupby(col)
                            .agg(pl.col('S').value_counts(sort=True))
                            .collect()
                            .lazy()
                            .select(
                                [pl.col(col).cast(str)]
                                + [
                                    pl.col('S')
                                    .arr.eval(
                                        pl.when(pl.element().struct.field('').is_in(list(s)))
                                        .then(pl.element().struct.field('counts'))
                                        .otherwise(0),
                                        parallel=True,
                                    )
                                    .arr.sum()
                                    .alias(season_portion_str_2(s))
                                    for s in s_chunks
                                ]
                            )
                            .sort(col)
                        )

                        description_str = f'{col}, '
                        cov_pct = self.wc.calc_coverage(season_range).select(pl.col('PCT').tail(1)).item()

                        if len(s_chunks) > 1:
                            vc = vc.with_columns(pl.sum(pl.exclude(col)).alias('ALL'))

                        df = vc.collect()

                        if df.height > 1:
                            total = df.select([pl.lit('ALL').alias(col)] + [pl.sum(c) for c in df.columns[1:]])
                            df.vstack(total, in_place=True)
                            ss = df.to_pandas().to_string(index=False).replace(col, ''.join(repeat(' ', len(col))))

                            # lift from cog_lineup slots
                            sss = ss.split('\n')
                            last_S_label = df.columns[-2]
                            sss[0] = sss[0].replace(last_S_label + ' ', last_S_label + ' | ')
                            j = sss[0].rindex('|')
                            for i in range(1, len(sss)):
                                sss[i] = sss[i][: j - 1] + ' | ' + sss[i][j:]
                            extra_line = ''.join(repeat('-', len(sss[0])))
                            extra_line = extra_line[:j] + '|' + extra_line[j + 1 :]
                            sss.insert(len(sss) - 1, extra_line)
                            ssss = '\n'.join(sss)
                        else:
                            ssss = df.to_pandas().to_string(index=False).replace(col, ''.join(repeat(' ', len(col))))

                        total_str += f'{col} FREQUENCY TABLE, {season_portion_str_2(season_range)} ({by}) ({cov_pct:.1f}% COV)\n\n{ssss}'
                    case ['PUZZLE' | 'P', *n]:
                        n = int(n[0]) if n else 2
                        if n < 2:
                            raise ValueError('N must be at least 2.')

                        vc = (
                            sub_df.lazy()
                            .select(pl.col('PUZZLE').value_counts(True, True))
                            .unnest('PUZZLE')
                            .filter(pl.col('counts') >= n)
                            .select([pl.col('counts').alias('N'), pl.col('PUZZLE')])
                            .select(pl.all().sort_by(['N', 'PUZZLE'], [True, False]))
                        )

                        nc = vc.select(pl.col('N').value_counts(True, True)).reverse().collect()

                        if nc.height:
                            total_str += (
                                'PUZZLE FREQUENCY LIST: '
                                + (', '.join(f'{d[0]["counts"]} {d[0]["N"]}x' for d in nc.rows()))
                                + '\n\n'
                                + str(
                                    vc.collect().to_pandas().to_string(index=False, formatters={'PUZZLE': lambda s: f' {s}'})
                                )
                            )
                        else:
                            total_str += f'`PUZZLE FREQUENCY: No puzzles that occurred more than {n} times.`'
                    case _:
                        total_str += gen_compendium_submes(sub_df, options.time)
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

        The compendium has several incomplete shows not in the database, that will go in if full copies of the show are ever found: https://buyavowel.boards.net/page/compendiumapp (Incomplete Shows)"""

        await send_long_mes(ctx, ppp(self.wc.calc_coverage(tuple(seasons), do_range).fill_null('')))

    @wheelcompendium.command(aliases=['br'], with_app_command=False)
    @commands.max_concurrency(1, commands.BucketType.channel)
    async def play(self, ctx, *, options: PlayFlags):
        """Play a random round from the compendium, bonus-round style. The sample pool is determined in the same way as running the search command (by default, all BRs).

        What letters are given for free and the number of consonants & vowels allowed to be called after are customizable with the keyword parameters above. By default the bot will give RSTLNE 3/1 for puzzles on or after Oct 1988, and (nothing) 5/1 otherwise. "Nothing" or "none" can be given to the "free" parameter to force no free letters.

        You will have 60 seconds to give a set number of consonants and vowels as its own message (or the bot will "time out" and stop playing), then 15 seconds after that to solve in another separate message. You can instantly solve without giving letters if you know it.

        Use the hide keyword argument to hide either the "date", "round", or "all", info until the command ends. This carries some risk as certain categories were misleading in the past (e.g. THING could be FOOD & DRINK), etc.

        The bot will scan all messages for letters/puzzle guesses unless single=True is specified, then the bot will only respond to the command giver's messages."""

        sub_df, _, _ = await asyncio.to_thread(self.search_inner, options)
        if not sub_df.height:
            raise ValueError('No puzzles found to sample from (check the parameters passed in).')

        row = sub_df.sample(1)  # .filter(pl.col('DATE') == date(1992, 10, 5))

        d, r, p, c = row.select(pl.col(['DATE', 'RD', 'PUZZLE', 'CATEGORY'])).row(0)

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
            clue = row.row(0)[-1]
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
                mc = m.content.upper()
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
        await ctx.send('>>> ' + COMPENDIUM_NOTES)

    async def cog_command_error(self, ctx, e):
        if isinstance(e, (commands.errors.MissingRequiredArgument, commands.errors.MissingRequiredFlag)):
            if ctx.command.name == 'search':
                await ctx.send('At least one condition required.', ephemeral=True)
            elif ctx.command.name == 'play':
                await ctx.send('At least one condition required. (Suggestion for play: `cond=rd;br`)', ephemeral=True)
            else:  # freq
                await ctx.send('`Must specify column.`', ephemeral=True)
        elif isinstance(e, commands.CommandInvokeError):
            if isinstance(e.__cause__, pl.exceptions.ComputeError):
                await ctx.send(f'Error computing query:\n```\n{e.__cause__}\n```', ephemeral=True)
            else:  # if isinstance(e.__cause__, ValueError):
                await ctx.send(f'`{e.__cause__}`', ephemeral=True)
        elif isinstance(e, commands.CheckFailure):
            if not self.wc or any(v is None for v in self.wc.df.values()):
                await ctx.send('`Compendium is loading (try again shortly) or failed to load.`', ephemeral=True)
            else:
                await ctx.send('`Only compendium maintainers can run this command.`', ephemeral=True)
        elif isinstance(e, commands.MaxConcurrencyReached):
            await ctx.send('`Only one puzzle can be played per channel at a time.`', ephemeral=True)
        elif isinstance(e, commands.BadFlagArgument):
            if isinstance(e.original, commands.BadLiteralArgument):
                await ctx.send(f'`Parameter "{e.flag.name}" must be one of {e.original.literals} (case-sensitive).`')
            elif isinstance(e.original, commands.RangeError):
                await ctx.send(f'`Parameter "{e.flag.name}": {e.original}`')
            else:
                await ctx.send(f'`Parameter "{e.flag.name}" was improperly formatted: {e.argument}`')
        else:
            await ctx.send(f'`{e}`', ephemeral=True)  # wayo.py handler


async def setup(bot):
    await bot.add_cog(CompendiumCog(bot))

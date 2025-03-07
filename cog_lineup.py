import asyncio
import itertools
import logging
import operator
import random
import string
import traceback
from collections import OrderedDict
from copy import copy
from datetime import *
from functools import reduce
from typing import List, Literal, Optional, Union

import discord
import discord.ui as dui
import numpy as np
import polars as pl
import polars.selectors as cs
import portion as P
from discord.ext import commands
from more_itertools import chunked, value_chain
from sortedcontainers import SortedSet

from conflictsheet import *
from dropboxwayo import dropboxwayo
from pg import *
from util import (
    CancelButton,
    PGConverter,
    PGGroupConverter,
    PGPlayingConverter,
    TimeConverter,
    add_separator_lines,
    excel_date_str,
    logic_expression,
    parse_endpoints,
    parse_time_options,
    season_portion_str,
    send_long_mes,
    PLAYING_FLAGS,
    NAME_ATTRGET,
    SCHEDULER_TZ,
    SORT_PROD,
    SEASON_RANGE,
    NONNEGATIVE_INT,
)
from util_expr import (
    build_flag_expr as has_any_flags,
    pretty_print_polars as ppp,
    build_lineup_expr,
    build_int_expression,
    build_date_expression,
    build_dt_q_expression,
    transform_str_to_dts,
)
from util2 import SI

_col_name_remapping = {
    'SEASON': 'S',
    'PRODUCTION': 'PROD',
    'NUMBER': 'PROD',
    'P': 'PROD',
    'AIR': 'AIRDATE',
    'ID': 'INT. DATE',
    'INTENDED': 'INT. DATE',
    'INT': 'INT. DATE',
    'INTENT': 'INT. DATE',
    'D': 'AIRDATE',
    'DATE': 'AIRDATE',
    'N': 'NOTES',
    'NOTE': 'NOTES',
}

_log = logging.getLogger('wayo_log')


ORDINAL_SUFFIXES = {1: 'st', 2: 'nd', 3: 'rd', 4: 'th', 5: 'th', 6: 'th'}
PLAYING_FLAGS_SINGLE = ('C', 'T', '&', '*', '@', 'R', '$', '^', '?', 'M', '0')
PLAYING_FLAGS_SINGLE_CERTAIN = tuple([pfs for pfs in PLAYING_FLAGS_SINGLE if pfs not in '^?'])
PF_TO_DESCRIPTIVE = {pf: pff for pf, pff in zip(PLAYING_FLAGS_SINGLE, reversed(PLAYING_FLAGS))}
PF_TO_DESCRIPTIVE['0'] = 'no flag'
SLOTS_REGEX = r'^(?:([123456])(?!.*\1)){1,6}$'
HALF_REGEX = r'[12]'
FREQS_REGEX = r'^(?:([0123456])(?!.*\1)){1,7}$'
FLAGS_REGEX = r'^(?:([' + ''.join(PLAYING_FLAGS_SINGLE) + r'])(?!.*\1)){1,' + str(len(PLAYING_FLAGS_SINGLE)) + r'}$'
FLAGS_REGEX_5 = (
    r'^(?:([' + ''.join(PLAYING_FLAGS_SINGLE_CERTAIN) + r'])(?!.*\1)){1,' + str(len(PLAYING_FLAGS_SINGLE_CERTAIN)) + r'}$'
)
FLAGS_REGEX_3 = '[' + ''.join(PLAYING_FLAGS_SINGLE) + ']'
FLAGS_REGEX_2 = '(' + FLAGS_REGEX_3 + '{3}){1,2}'
FLAGS_REGEX_4 = r'^([123456]):(.+)$'
CONFLICTN_REGEX = '(max|m)'

LINEUP_SEP = '\n\n' + (' '.join('~' * 22)) + '\n\n'
BY_SEASON_UNCERTAIN_LABELS = ['PG^', 'PG?']

ALL_PLINKO_FLAGS = frozenset({0} | {len(PLAYING_FLAGS) - PLAYING_FLAGS.index(f) for f in ('car', 'T', '$')})


def prodStr(s):
    if m := re.fullmatch(r'(\d{1,4})(S|LV)', s, re.I):
        return (m.group(1).zfill(4) + m.group(2)).upper()
    elif m := re.fullmatch(r'(\d{1,2})(D)', s, re.I):
        return (m.group(1).zfill(2) + m.group(2)).upper()
    elif m := re.fullmatch(r'(\d{1,3}?)([12345][KDLRX]|SP|[NP])', s, re.I):
        return (m.group(1).zfill(3) + m.group(2)).upper()
    elif m := re.fullmatch('58XXD', s, re.I):
        return s.upper()
    else:
        raise commands.BadArgument(f'{s} is not a valid production string.')


def dayProdStr(s):
    if m := re.fullmatch(r'(\d{1,3})([12345][KDLRX])', s, re.I):
        return (m.group(1).zfill(3) + m.group(2)).upper()
    else:
        raise commands.BadArgument(f'{s} is not a valid daytime production string.')


def slotsStr(s):
    if re.fullmatch(SLOTS_REGEX, s):
        return s.upper()
    else:
        raise commands.BadArgument(f'{s} is not a valid slots string.')


def flag2str(s):
    if re.fullmatch('any', s, re.I):
        return s.lower()
    if re.fullmatch(FLAGS_REGEX_5, s, re.I):
        return s.upper()
    else:
        raise commands.BadArgument(f'{s} is not a valid certain flags string.')


def flagStr(s):
    if re.fullmatch(FLAGS_REGEX_2, s, re.I):
        return s.upper()
    else:
        raise commands.BadArgument(f'{s} is not a valid flags string.')


def flag3str(s):
    if re.fullmatch(FLAGS_REGEX_3, s, re.I):
        return s.upper()
    else:
        raise commands.BadArgument(f'{s} is not a valid flags string.')


def flag4str(s):
    if m := re.fullmatch(FLAGS_REGEX_4, s, re.I):
        pg_slot, reg = m.groups()
        if re.fullmatch(FLAGS_REGEX, reg, re.I):
            return int(pg_slot), reg.upper()
        elif re.fullmatch('any', reg, re.I):
            return int(pg_slot), reg.lower()
        else:
            raise commands.BadArgument(f'{s} is not a valid flags string.')
    else:
        raise commands.BadArgument(f'{s} is not a valid flags string.')


def dateStr(arg):
    if re.search(r'\d', arg) or '%' not in arg:
        raise commands.BadArgument(f'{arg} is not a valid date string.')
    else:
        return arg


def freqStr(arg):
    if re.fullmatch(r'(\d+([ywd]|mo))+', arg, re.I):
        return arg.lower()
    else:
        raise commands.BadArgument(f'{arg} is not a valid frequency string.')


def sortStr(arg):
    if re.fullmatch('([pd]|PROD|DATE)', arg, re.I):
        return 'prod' if arg[0] == 'p' else 'date'
    else:
        return commands.BadArgument(f'{arg} is not a valid sort string.')


def conflictNint(arg):
    if re.fullmatch(CONFLICTN_REGEX, arg, re.I):
        return SI.inf
    else:
        i = int(arg)
        if i < 0:
            raise commands.BadArgument('N must be non-negative.')
        return i


async def trim_query(q: pl.LazyFrame, sortBy: str = 'prod', since: bool = False):
    if byDate := sortBy == 'date' and 'AIRDATE' in q.columns:
        q = q.sort('AIRDATE')
    if since:
        if byDate:
            q = q.select(
                pl.col('PROD', 'S', 'AIRDATE'),
                pl.col('AIRDATE').diff().dt.days().alias('SINCE'),
                pl.exclude('PROD', 'S', 'AIRDATE'),
            )
            extra = 'day'
        else:
            q = q.select(pl.col('PROD'), pl.col('PG_n').diff().alias('SINCE'), pl.exclude('PROD'))
            extra = 'lineup'

        q = q.with_columns(
            (
                pl.col('SINCE').cast(str)
                + pl.when(pl.col('SINCE') == 1).then(pl.lit(f' {extra}')).otherwise(pl.lit(f' {extra}s'))
            ).fill_null('')
        )
    return await asyncio.to_thread(q.drop(cs.contains('_')).collect)


def gen_lineup_submes(sub_df: pl.DataFrame, initial_str: str, time: str):
    q = sub_df.lazy()

    if notes_col := 'SPECIAL' if time == 'primetime' else '' if time == 'syndicated' else 'NOTES':
        notes_check = sub_df.select(pl.col(notes_col).is_null()).to_series()
        if notes_check.all():
            q = q.drop(notes_col)
        elif notes_check.any():
            q = q.with_columns(pl.col(notes_col).fill_null(''))

    if 'AIRDATE' in sub_df.columns:
        if sub_df.select((pl.col('INT. DATE') == pl.col('AIRDATE')).all()).to_series().all():
            q = q.drop('INT. DATE')
        q = q.with_columns(cs.ends_with('DATE').dt.strftime('%b %d %Y').fill_null('NEVER AIRED'))

    if time != 'syndicated':
        half_hour_check = sub_df.select(pl.col('PG6').is_null()).to_series()
        if half_hour_check.all():
            q = q.drop('PG4', 'PG5', 'PG6')
        elif half_hour_check.any():
            q = q.with_columns(pl.col('PG4', 'PG5', 'PG6').fill_null(''))

    sub_df = q.collect()

    sub_df_str = ppp(sub_df) if sub_df.height else '' if initial_str else 'None'
    return f'{initial_str}\n\n{sub_df_str}' if initial_str else sub_df_str


class CSUpdateView(dui.View):
    def __init__(self, cc, prodNumber, retro, guild, scheduler, pgUpdate=True):
        super().__init__(timeout=3600.0)

        self.callback_func = cc.cs_update if pgUpdate else cc.cs_metaupdate
        self.timeout_func = cc.cs_cancel
        data = 'lineup' if pgUpdate else 'metadata'
        self.prodNumber = prodNumber
        self.update.label = (
            f'OVERWRITE {prodNumber} with the above {data}' if retro else f'ADD {prodNumber} with the above {data}'
        )
        self.dig_emoji = cc.bot.dig_emoji
        self.update.emoji = self.dig_emoji

        self.retro = retro
        self.scheduler = scheduler
        self.check_role = cc.conflict_role
        self.pgUpdate = pgUpdate
        self.job = None

        self.cancel = CancelButton(emoji=cc.bot.x_emoji, extra_callback=self.on_timeout)
        self.add_item(self.cancel)

    @dui.button(style=discord.ButtonStyle.primary)
    async def update(self, interaction, button):
        assert not self.job
        self.job = self.scheduler.add_job(
            self.callback_func, 'date', args=(self,), run_date=datetime.now(tz=SCHEDULER_TZ) + timedelta(seconds=1)
        )
        button.label = 'Processing...'
        button.emoji = '🚧'
        button.disabled = True
        self.remove_item(self.cancel)
        await interaction.response.edit_message(view=self)
        # up to callback_func to call finish

    async def on_timeout(self):
        await self.timeout_func(self)

    def finish(self):
        self.update.label = f'Overwrote {self.prodNumber}.' if self.retro else f'Added {self.prodNumber}.'
        self.update.emoji = '🖋️' if self.retro else self.dig_emoji
        self.update.style = discord.ButtonStyle.success
        self.stop()

    async def interaction_check(self, interaction):
        return True  # (
        # self.check_role in interaction.user.roles
        # if self.pgUpdate
        # else interaction.user.id in (149230437850415104, 688572894036754610)
        # )


class EditLineupFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    prodNumber: prodStr = commands.flag(aliases=['prod'])
    airdate: str = commands.flag(aliases=['air'], default=None)
    intended_date: str = commands.flag(aliases=['id', 'intent'], default=None)
    dateFormat: dateStr = commands.flag(aliases=['format'], default='%m/%d/%y')
    notes: str = None


class TimeFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    time: TimeConverter = commands.flag(aliases=['version'], default='daytime')
    start: Union[SEASON_RANGE, str] = commands.flag(
        default=lambda ctx: 1 if ctx.command.name in ('search', 'lineupRandom') else CURRENT_SEASON - 4
    )
    end: Union[SEASON_RANGE, str] = commands.flag(default=CURRENT_SEASON)
    dateFormat: dateStr = commands.flag(aliases=['format'], default='%m/%d/%y')


class ConflictSheetFlags(TimeFlags):
    prodNumber: prodStr = commands.flag(aliases=['prod', 'p'], default=None)
    pgFlags: flagStr = commands.flag(aliases=['flags', 'f'], default='000000')
    hideSheet: bool = commands.flag(aliases=['hide', 'h'], default=None)
    halfHour: bool = commands.flag(aliases=['hh'], default=False)


class ConflictBaseFlags(TimeFlags):
    excludeEducated: bool = commands.flag(aliases=['exclude'], default=False)
    pgFlags: List[flag4str] = commands.flag(aliases=['flags', 'f'], default=None)


class ConflictNFlags(ConflictBaseFlags):
    N1: NONNEGATIVE_INT = 1
    N2: conflictNint = 'max'
    pgGroupCompare: PGGroupConverter = commands.flag(aliases=['pgGroup', 'compare'], default=None)


class ConflictFlags(ConflictBaseFlags):
    bySeason: NONNEGATIVE_INT = commands.flag(aliases=['by'], default=0)
    showLineup: bool = commands.flag(aliases=['show'], default=False)
    sortBy: sortStr = commands.flag(aliases=['sort'], default='prod')
    since: bool = False


class SlotFlags(TimeFlags):
    bySeason: NONNEGATIVE_INT = commands.flag(aliases=['by'], default=0)
    pgFlags: flag2str = commands.flag(aliases=['flag', 'f'], default=None)
    pgsOnly: bool = commands.flag(aliases=['pgOnly', 'only'], default=False)


class SearchFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    time: TimeConverter = commands.flag(aliases=['version'], default='daytime')
    logicExpr: logic_expression = commands.flag(aliases=['logic'], default='all')
    conditions: List[str] = commands.flag(name='condition', aliases=['cond', 'c'])
    excludeUncertain: bool = commands.flag(aliases=['exclude'], default=False)
    sortBy: sortStr = commands.flag(aliases=['sort'], default='prod')
    since: bool = False
    dateFormat: dateStr = commands.flag(aliases=['format'], default='%m/%d/%y')


class MostPlayedFlags(TimeFlags):
    N: NONNEGATIVE_INT = 3
    excludeUncertain: bool = commands.flag(aliases=['exclude'], default=False)


class LastPlayedFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    sortBy: sortStr = commands.flag(aliases=['sort'], default='date')
    asOf: dayProdStr = None
    pgFlag: flag3str = commands.flag(aliases=['flag', 'f'], default=None)
    activeOnly: bool = commands.flag(aliases=['active'], default=True)


class LineupRFlags(TimeFlags):
    sort: bool = True


class GenerateFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    N: commands.Range[int, 1, 5] = commands.flag(name='n', default=1)
    smart: bool = commands.flag(aliases=['s'], default=True)
    retired: bool = commands.flag(aliases=['r'], default=False)
    unique: bool = commands.flag(aliases=['u'], default=False)
    half_hour: bool = commands.flag(aliases=['halfHour', 'hh'], default=False)


class LineupCog(commands.Cog, name='TPIRLineups'):
    """Commands related to an internal database of all known Price is Right lineups."""

    def __init__(self, bot):
        self.cs = None
        self.latest_conflict = {}
        self.latest_meta = {}
        self.latest_lock = asyncio.Lock()
        self.bot = bot

    # bases

    @commands.hybrid_group(aliases=['l'], invoke_without_command=True, case_insensitive=True)
    async def lineup(self, ctx):
        """Commands specifically related to viewing and editing TPiR lineups."""
        await ctx.send('Invalid subcommand (see `help lineup`).')

    @commands.hybrid_group(aliases=['playings', 'playing', 'play', 'p'], invoke_without_command=True, case_insensitive=True)
    async def played(self, ctx):
        """Commands specifically related to statistics of Pricing Game playings in TPiR lineups."""
        await ctx.send('Invalid subcommand (see `help played`).')

    # end bases

    @commands.Cog.listener()
    async def on_ready(self):
        self.guild = self.bot.get_guild(314598609591074816)
        self.conflict_role = self.guild.get_role(493259556655202304)
        # _log.info(f'cog_lineup on_ready: fetched conflict_role as {self.conflict_role}')

    async def cog_before_invoke(self, ctx):
        if not self.cs or not self.cs.is_ready():
            m = await ctx.send('`Lineups need to be reloaded, give me a moment...`')
            dropboxfn = '/heroku/wayo-py/'
            _log.info('start creating cs at ' + str(datetime.now()))
            self.cs = await asyncio.to_thread(
                ConflictSheet,
                lambda fn: io.BytesIO(dropboxwayo.download(dropboxfn + fn)),
                lambda iob, fn: dropboxwayo.upload(iob.getvalue(), dropboxfn + fn),
            )
            _log.info('end creating cs at ' + str(datetime.now()))
            await m.delete()

    async def cs_update(self, view):
        async with self.latest_lock:
            mes, prodNumber, pgps, retro = self.latest_conflict[view]
            _log.info('start updating & reloading cs at ' + str(datetime.now()))
            today = datetime.now(tz=SCHEDULER_TZ).date()
            _log.info(f'feeding in {pgps} to cs.update')
            await asyncio.to_thread(
                self.cs.update, prodNumber, pgps, not retro, None if retro else today, None if retro else today, None
            )
            _log.info('end cs at ' + str(datetime.now()))
            del self.latest_conflict[view]
            view.finish()
            await mes.edit(view=view)
            try:
                await asyncio.to_thread(self.cs.save_excel)
                _log.info('end saving excel at ' + str(datetime.now()))
            except Exception as e:
                await mes.add_reaction('❌')
                _log.error('failed to save excel at ' + str(datetime.now()))
                traceback.print_tb(e.__traceback__)

    @played.command(aliases=['conflictsheet', 'sheet', 'cs'], with_app_command=False)
    async def concurrencesheet(
        self,
        ctx,
        pg1: PGPlayingConverter,
        pg2: PGPlayingConverter,
        pg3: PGPlayingConverter,
        pg4: Optional[PGPlayingConverter],
        pg5: Optional[PGPlayingConverter],
        pg6: Optional[PGPlayingConverter],
        *,
        options: ConflictSheetFlags,
    ):
        """Generates a concurrence sheet (an overview of the full lineup's pair-concurrencies and slot info) for the provided pricing games. Note for slot info in daytime, numbers are adjusted to exclude any uncertain slotting, if applicable.

        The dataset used is determined by the time, start and end parameters. For more on these, see the FAQ.

        PGs are all mapped to at least one single-word key. See the FAQ for a complete listing.

        If prodNumber is given, and a certified user press the given button prompt, this sheet (presumably from an actual Price episode) will be added or overwritten to the overall lineup data, based on how prodNumber ends (D/K for daytime, SP for primetime), with the given flags (such as non-car for a car, one-time rule change to a game, etc.). This will only have an effect in #daytime and #retrotime.

        If hideSheet is true when prodNumber is also given, the lineup is parroted back instead of a conflict sheet. This is meant for shorthand lineup overwrites when the actual sheet is not of interest. By default this is true in #retrotime.
        """

        try:
            ep, epText, _ = await parse_time_options(ctx, options)
        except ValueError:
            return  # error message sending taken care of in parse_time_options (could have factored in coding better)
        except AssertionError as e:
            await ctx.send(f'`Error parsing time options: {e.message}`')
            return

        raw_pgps = ctx.args[2 : 5 if options.halfHour else 8]
        pgs = [rp[0] if rp else None for rp in raw_pgps]
        assert len(set(pgs)) == len(pgs) and all(pgs), 'Invalid PG and/or duplicate PG provided.'

        if options.prodNumber and options.time == 'daytime':
            try:
                row = (
                    self.cs.get('daytime')
                    .select(cs.by_name('PROD', 'S') | cs.matches(r'^PG\d$'))
                    .row(by_predicate=pl.col('PROD') == options.prodNumber)
                )
                retro = True
            except pl.exceptions.RowsError:
                retro = False

            if retro:
                playing_names = []
                prodSorted = SORT_PROD(options.prodNumber)
                season = int(row[1])

                for pg, arg in raw_pgps:
                    if pg == PG.LuckySeven and prodSorted < SORT_PROD('6131D'):
                        playing_names.append('Lucky Seven')
                    elif pg == PG.MostExpensive and prodSorted < SORT_PROD('5035K'):
                        playing_names.append('Most Expensive')
                    elif pg == PG.BargainGame and season < 40:
                        playing_names.append("Barker's Bargain Bar")
                    elif pg == PG.CheckGame and prodSorted < SORT_PROD('6354D'):
                        playing_names.append('Blank Check')
                    elif pg == PG.FivePriceTags and season < 42:
                        playing_names.append('Five Price Tags')
                    elif pg == PG.PickAPair and season < 19:
                        playing_names.append('Pick a Pair')
                    elif pg == PG.TenChances and season < 39:
                        playing_names.append('Ten Chances')
                    elif pg == PG.NowOrThen and ('nat' in arg.lower() or prodSorted < SORT_PROD('6292D')):
                        playing_names.append('Now....and Then')
                    elif pg == PG.DiceGame and 'deluxe' in arg.lower():
                        playing_names.append('Deluxe Dice Game')
                    elif pg == PG.ThreeStrikes and '+' in arg:
                        playing_names.append('3 Strikes +')
                    elif pg == PG.CardGame and SORT_PROD('4843D') <= prodSorted < SORT_PROD('5383D'):
                        playing_names.append('New Card Game')
                    elif pg == PG.HoleInOne and SORT_PROD('6365D') <= prodSorted < SORT_PROD('6671D'):
                        playing_names.append('Hole in One or Two')
                    elif pg == PG.MoneyGame and 'big' in arg.lower():
                        playing_names.append('Big Money Game')
                    else:
                        playing_names.append(pg.sheetName)

                pgps = [
                    PGPlaying(pn, 2 ** PLAYING_FLAGS_SINGLE.index(f) if f != '0' else 0, pg=pg)
                    for pg, pn, f in zip(pgs, playing_names, options.pgFlags)
                ]
            else:
                back_year = (date.today().year - 50) % 100
                pgps = [
                    PGPlaying(
                        f"Back to '{back_year}" if pg == PG.BackToXX else pg.sheetName,
                        2 ** PLAYING_FLAGS_SINGLE.index(f) if f != '0' else 0,
                        pg=pg,
                    )
                    for (pg, arg), f in zip(raw_pgps, options.pgFlags)
                ]

            if retro and ctx.channel.id == 783063090852921344:  # actual: 783063090852921344 test: 814907236023271454
                if options.hideSheet == None:
                    options.hideSheet = True
            elif not retro and ctx.channel.id == 314598609591074816:  # actual: 314598609591074816 test: 281324492146343946
                pass
            else:
                gs = self.cs.gen_sheet(pgps, ep, epText, options.time)
                mc = f'```\n{gs}```'
                await ctx.send(content=mc)
                return

            v = CSUpdateView(self, options.prodNumber, retro, ctx.guild, self.bot.SCHEDULER)

            # if self.latest_conflict:
            # 	mm,_,_,_,_ = self.latest_conflict[-1]
            # 	await mm.edit(view=None)

            pgps_strs = [str(pgp) for pgp in pgps]
            mc = (
                f'Proposed {options.prodNumber} - ' + ', '.join(pgps_strs)
                if options.hideSheet
                else self.cs.gen_sheet(pgps, ep, epText, options.time)
            )
            if retro:
                old_pgps_strs = row[-3 if time == 'syndicated' else -6 :]
                mc += (
                    ('`\n`' if options.hideSheet else '\n\n')
                    + f'Current {options.prodNumber} - '
                    + ', '.join(ops for ops in old_pgps_strs if ops)
                )
                if pgps_strs == old_pgps_strs:
                    mc += ('`\n`' if options.hideSheet else '\n\n') + 'This proposal has no changes.'
                    v = None

            m = await ctx.send(content=f'>>> `{mc}`' if options.hideSheet else f'```\n{mc}```', view=v)
            if v:
                self.latest_conflict[v] = (m, options.prodNumber, pgps, retro)
        else:
            pgps = [
                PGPlaying(pg.sheetName, 2 ** PLAYING_FLAGS_SINGLE.index(f) if f != '0' else 0, pg=pg)
                for pg, f in zip(pgs, options.pgFlags)
            ]
            gs = self.cs.gen_sheet(pgps, ep, epText, options.time)
            mc = f'```\n{gs}```'
            await ctx.send(content=mc)

    async def cs_metaupdate(self, view):
        async with self.latest_lock:
            mes, prodNumber, ad, ind, notes = self.latest_meta[view]
            _log.info('META: start updating & reloading cs at ' + str(datetime.now()))
            await asyncio.to_thread(self.cs.update, prodNumber, None, False, ad, ind, notes)
            _log.info('META: end cs at ' + str(datetime.now()))
            del self.latest_meta[view]
            view.finish()
            await mes.edit(view=view)
            try:
                await asyncio.to_thread(self.cs.save_excel)
                _log.info('META: end saving excel at ' + str(datetime.now()))
            except Exception as e:
                await mes.add_reaction('❌')
                _log.error('META: failed to save excel at ' + str(datetime.now()))
                traceback.print_tb(e.__traceback__)

    @lineup.command(name='edit', aliases=['e'], with_app_command=False)
    async def editLineup(self, ctx, *, options: EditLineupFlags):
        """Edit a lineup's date(s) and/or notes. Currently only Wayoshi and dftackett can confirm this command, but anyone can set it up.

        Notes should NOT be in quotes. Use "notes=empty" to specifying removing the notes for the episode."""

        assert options.intended_date or options.airdate or options.notes

        try:
            ind = datetime.strptime(options.intended_date, options.dateFormat) if options.intended_date else None
            ad = datetime.strptime(options.airdate, options.dateFormat) if options.airdate else None
        except ValueError as e:
            await ctx.send(f'`Malformed date: {e}`')
            return

        for time in ('daytime', 'primetime'):
            dfd = self.cs.get(time)
            try:
                n_col = 'NOTES' if time == 'daytime' else 'SPECIAL'
                _, cur_notes, cur_ad, cur_id = dfd.select('PROD', n_col, 'AIRDATE', 'INT. DATE').row(
                    by_predicate=pl.col('PROD') == options.prodNumber
                )

                if type(cur_notes) != str:
                    cur_notes = ''
                current = (
                    f'CURRENT {options.prodNumber}:\n\tAIRDATE: '
                    + excel_date_str(cur_ad)
                    + '\n\tINT. DATE: '
                    + excel_date_str(cur_id)
                    + f'\n\t{n_col}: {cur_notes}'
                )
                break
            except pl.exceptions.RowsError:
                pass
        else:
            await ctx.send('`Production code must exist and be in daytime or primetime.`')
            return

        ind_str = excel_date_str(ind) if ind else 'Do not change'
        ad_str = excel_date_str(ad) if ad else 'Do not change'
        empty_notes = options.notes and options.notes.lower() == 'empty'
        notes_str = 'Do not change' if not options.notes else '' if empty_notes else options.notes
        input_str = f'PROPOSED CHANGES:\n\tAIRDATE: {ad_str}\n\tINT. DATE: {ind_str}\n\t{n_col}: {notes_str}'

        v = CSUpdateView(self, options.prodNumber, True, ctx.guild, self.bot.SCHEDULER, pgUpdate=False)

        # if self.latest_meta:
        # 	mm = self.latest_meta[-1]
        # 	await mm.edit(view=None)

        m = await ctx.send(f'```{input_str}\n\n{current}```', view=v)
        self.latest_meta[v] = (m, options.prodNumber, ad, ind, None if empty_notes else options.notes)

    async def cs_cancel(self, view):
        async with self.latest_lock:
            try:
                del (self.latest_conflict if view.pgUpdate else self.latest_meta)[view]
            except KeyError:
                pass

    @played.command(aliases=['conflict', 'c'], with_app_command=False)
    async def concurrence(
        self,
        ctx,
        pg1: PGConverter,
        pg2: PGConverter,
        pg3: Optional[PGConverter],
        pg4: Optional[PGConverter],
        pg5: Optional[PGConverter],
        pg6: Optional[PGConverter],
        *,
        options: ConflictFlags,
    ):
        """Fetches concurrence info for the provided pricing games (at least 2, up to 6). (The number of times played together in the same lineup.)

        The dataset used is determined by the time, start and end parameters. For more on these, see the FAQ.

        PGs are all mapped to at least one single-word key. See the FAQ for a complete listing.

        See !flagshelp for more on pgFlags.

        The output is the number of times, within the given dataset, all the PGs have shown together in one lineup.

        If showLineups is True (see the FAQ for more info on how to specify True), also print out the complete lineup info for every matching entry. Can then further be sorted by production number or date ("sort" option), with the additional option to show the number of shows/days since the prior sorted entry ("since" option).

        If bySeason is non-zero, partition the output by the given number of ep (provided the given time has ep).

        For daytime, playings with the educated guess flag (?) can be optionally excluded."""

        pgs = list(itertools.takewhile(operator.truth, ctx.args[2:8]))
        pgs_set = set(pgs)
        assert len(pgs_set) == len(pgs), 'Duplicate PG found.'

        try:
            ep, epText, isDate = await parse_time_options(ctx, options, *pgs)
        except ValueError:
            return  # error message sending taken care of in parse_time_options (could have factored in coding better)
        except AssertionError as e:
            await ctx.send(f'`Error parsing time options: {e.message}`')
            return

        bySeasonBool = (
            not isDate
            and options.bySeason > 0
            and not ep.empty
            and options.bySeason < len(ep_list := list(SI.iterate(ep, step=1)))
        )

        async with ctx.typing():
            flags = [None] * len(pgs)
            fs_str = [None] * len(pgs)
            if options.pgFlags:
                for pg_idx, fs in options.pgFlags:
                    assert pg_idx <= len(pgs), 'Flags given for non-existent PG.'
                    flags[pg_idx - 1] = frozenset(
                        {2**f for f in range(10)}
                        if fs == 'any'
                        else {0 if f.isnumeric() else 2 ** PLAYING_FLAGS_SINGLE.index(f) for f in fs}
                    )
                    fs_str[pg_idx - 1] = (
                        'any flag' if fs == 'any' else '/'.join([PF_TO_DESCRIPTIVE[f] if f else 'no flag' for f in fs])
                    )
                    if 1 in flags[pg_idx - 1] and pgs[pg_idx - 1] in PG.partition_table['CAR_BOATABLE']:
                        fs_str[pg_idx - 1] = fs_str[pg_idx - 1].replace('car', 'boat')

            # determine pg_strs before exclude
            pgs_str = ', '.join(str(pg) + (f' ({fss})' if fss else '') for pg, fss in zip(pgs, fs_str))

            # now, exclude
            if options.excludeEducated:
                flags = [fl - {2**q for q in Q_FLAG} if fl else ALL_FLAGS_BUT_GUESS for fl in flags]

            sub_df = await trim_query(
                self.cs.concurrence_query(ep, options.time, tuple(pgs), tuple(flags)), options.sortBy, options.since
            )

            ttl = sub_df.height

            if bySeasonBool and ttl:
                season_chunks = [ep.replace(lower=sc[0], upper=sc[-1]) for sc in chunked(ep_list, options.bySeason)]
                season_chunk_lists = [list(SI.iterate(sc, step=1)) for sc in season_chunks]
                sub_df_groups = [sub_df.filter(pl.col('S').is_in(scl)) for scl in season_chunk_lists]
                freq_chunks = [sdg.height for sdg in sub_df_groups]

                initial_str = '{}, {}{}{}: {} | {}'.format(
                    pgs_str,
                    epText,
                    f' ({options.bySeason})' if options.bySeason else '',
                    ', no ? flag' if options.excludeEducated else '',
                    ', '.join(str(fc) for fc in freq_chunks),
                    ttl,
                )
                if options.showLineup:
                    total_str = []
                    for fc, sc, scl, sdg in zip(freq_chunks, season_chunks, season_chunk_lists, sub_df_groups):
                        sub_is = '{}, {}: {}'.format(pgs_str, season_portion_str(sc), fc)
                        if fc:
                            if options.bySeason == 1:
                                sdg = sdg.drop('S')
                            total_str.append(gen_lineup_submes(sdg, sub_is, options.time))
                        else:
                            total_str.append(sub_is)
                    total_str = initial_str + LINEUP_SEP + LINEUP_SEP.join(total_str)
            else:
                initial_str = '{}, {}{}: {}'.format(pgs_str, epText, ', no ? flag' if options.excludeEducated else '', ttl)
                if options.showLineup and ttl:
                    total_str = gen_lineup_submes(
                        sub_df.drop('S') if ep and options.start == options.end else sub_df,
                        initial_str,
                        options.time,
                    )

        if options.showLineup and ttl:
            await send_long_mes(ctx, total_str)
        else:
            await ctx.send(f'`{initial_str}`')

    @played.command(aliases=['slot', 's'], with_app_command=False)
    async def slots(self, ctx, pgQueries: commands.Greedy[Union[PGConverter, PGGroupConverter]], *, options: SlotFlags):
        """Fetches full slot counts (with the given flag if provided) for each query.

        A pgQuery in this context is a valid single PG, or a PGGroup. A PGGroup is treated as the sum of all its underlying PGs.

        See !flagshelp for more on pgFlags. This flag must be a certain one (no ^ or ?) as the slots for such flags are by nature undefined.

        The dataset used is determined by the time, start and end parameters. For more on these, see the FAQ.

        If bySeason is non-zero, partition the output by the given number of seasons (provided the given time has seasons).

        If pgsOnly is true, PGGroups are instead treated as shorthand for multiple single PGs.

        PGs and PGGroups are all mapped to at least one single-word key. See the FAQ for a complete listing.
        """

        try:
            ep, epText, isDate = await parse_time_options(ctx, options)
        except ValueError:
            return  # error message sending taken care of in parse_time_options (could have factored in coding better)
        except AssertionError as e:
            await ctx.send(f'`Error parsing time options: {e.message}`')
            return

        bySeasonBool = (
            not isDate
            and options.bySeason > 0
            and not ep.empty
            and options.bySeason < len(ep_list := list(SI.iterate(ep, step=1)))
        )

        if pgQueries:
            if options.pgsOnly:
                pgQueries = SortedSet(value_chain(*pgQueries), key=NAME_ATTRGET)
        else:
            await ctx.send(
                '`No valid PG or PGGroups given. (If giving multiple arguments, double-check the first one).`',
                ephemeral=True,
            )
            return

        async with ctx.typing():
            fs = []
            sep = LINEUP_SEP if bySeasonBool else '\n'

            if options.pgFlags:
                flags_str = (
                    ' ('
                    + (
                        'any flag'
                        if options.pgFlags == 'any'
                        else '/'.join([PF_TO_DESCRIPTIVE[f] if f else 'no flag' for f in options.pgFlags])
                    )
                    + ')'
                )
                options.pgFlags = (
                    {2**f for f in range(10)}
                    if options.pgFlags == 'any'
                    else {0 if f.isnumeric() else 2 ** PLAYING_FLAGS_SINGLE.index(f) for f in options.pgFlags}
                )
            else:
                flags_str = ''

            sub_slots_df = self.cs.endpoint_sub(ep, options.time, table='slots')
            if options.pgFlags:
                sub_slots_df = sub_slots_df.filter(pl.col('flag').is_in(list(options.pgFlags)))

            for pgQ in pgQueries:
                isPGGroup = not type(pgQ) == PG
                if isPGGroup:
                    qPG = frozenset(pgQ)
                    qPGName = PG.partition_table.inverse[qPG]
                    if options.pgFlags and 1 in options.pgFlags:
                        if qPG <= PG.partition_table['CAR_BOATABLE']:
                            f_str = flags_str.replace('car', 'boat')
                        elif qPG & PG.partition_table['CAR_BOATABLE']:
                            f_str = flags_str.replace('car', 'car/boat')
                        else:
                            f_str = flags_str
                    else:
                        f_str = flags_str
                else:
                    qPG = frozenset([pgQ])
                    qPGName = str(pgQ)
                    if options.pgFlags and 1 in options.pgFlags and pgQ in PG.partition_table['CAR_BOATABLE']:
                        f_str = flags_str.replace('car', 'boat')
                    else:
                        f_str = flags_str

                if options.time in ('daytime', 'syndicated'):
                    if not isDate:
                        if isPGGroup:
                            if not any(pg.activeIn(ep) for pg in qPG):
                                fs.append(
                                    'No {} game {} active in {}.'.format(
                                        qPGName, 'is' if CURRENT_SEASON in ep else 'was', epText
                                    )
                                )
                                continue
                        else:
                            if not pgQ.activeIn(ep):
                                if pgQ.lastSeason < ep.lower:
                                    fs.append('{} was retired after S{}.'.format(pgQ, pgQ.lastSeason))
                                elif pgQ.firstSeason > ep.upper:
                                    fs.append('{} was not created until S{}.'.format(pgQ, pgQ.firstSeason))
                                else:
                                    fs.append(
                                        '{} {} inactive in {}.'.format(pgQ, 'is' if CURRENT_SEASON in ep else 'was', epText)
                                    )
                                continue

                    pg_ep, epText = parse_endpoints(
                        options.start,
                        options.end,
                        *qPG,
                        dateF=options.dateFormat,
                        syndicated=options.time == 'syndicated',
                        or_=True,
                    )
                    if options.time == 'syndicated':
                        epText = 'SYNDICATED ' + epText

                if bySeasonBool:
                    ep_list = list(SI.iterate(pg_ep, step=1))
                    season_chunks = [pg_ep.replace(lower=sc[0], upper=sc[-1]) for sc in chunked(ep_list, options.bySeason)]
                    sc_strs = [season_portion_str(sc) for sc in season_chunks]

                    h = (
                        sub_slots_df.filter(
                            pl.col('PG').is_in([str(pg) for pg in pgQ]) if isPGGroup else pl.col('PG') == str(pgQ)
                        )
                        .drop('PG')
                        .group_by([(pl.col('S').rank('dense') - 1) // options.bySeason, 'flag'])
                        .agg(pl.exclude('S').sum())
                    )
                    if not options.pgFlags:
                        sc_t = (SlotCertainty.SLOT, SlotCertainty.GAME)
                        h_unc = (
                            h.select(
                                [pl.col('S')]
                                + [
                                    pl.when(pl.col('flag') == sc)
                                    .then(pl.concat_list(cs.matches(r'^PG\d$')).list.sum())
                                    .otherwise(pl.lit(0))
                                    .alias(f'PG{f}')
                                    for sc, f in zip(sc_t, '^?')
                                ]
                            )
                            .group_by('S')
                            .sum()
                        )
                        h = (
                            h.filter(~has_any_flags('flag', frozenset(sc_t)))
                            # 2025-02-05 - this needs to be outer in case a game only has unknown slotting in a season
                            # like Race S2. suffix='' kills the dupe columns
                            .join(h_unc, on='S', how='outer', suffix='')
                            .group_by('S')
                            .agg(pl.exclude('flag').sum())
                            .sort('S')
                            .with_columns(pl.Series('S', sc_strs), pl.concat_list(pl.exclude('S')).list.sum().alias('ALL'))
                        )
                    h = (
                        pl.concat([h, h.select([pl.lit(epText).alias('S'), pl.exclude('S').sum()])])
                        .rename({'S': ''})
                        .collect()
                    )
                    h = h.select(pl.col(*[s.name for s in h if s.name not in ('PG^', 'PG?') or s.sum()]))

                    # add some line spacing between total row/column
                    # Oct 2023 - refactored as util method
                    ssss = add_separator_lines(ppp(h), h.columns[-2], True)

                    fs.append(f'{qPGName}{f_str}, {epText} ({options.bySeason}):\n\n{ssss}')
                else:
                    h = (
                        sub_slots_df.filter(
                            pl.col('PG').is_in([str(pg) for pg in pgQ]) if isPGGroup else pl.col('PG') == str(pgQ)
                        )
                        .drop('PG')
                        .group_by('flag')
                        .agg(cs.matches(r'^PG\d$').sum())
                        # .with_columns(pl.concat_list(pl.exclude('flag')).list.sum().alias('ALL'))
                        .collect()
                    )

                    ssd_pg_certain = h.filter(pl.col('flag') < SlotCertainty.SLOT).sum().fill_null(0).row(0)[1:]
                    try:
                        ssd_pg_slot = sum(h.row(by_predicate=pl.col('flag') == SlotCertainty.SLOT)[1:])
                    except pl.exceptions.RowsError:
                        ssd_pg_slot = 0
                    try:
                        ssd_pg_game = sum(h.row(by_predicate=pl.col('flag') == SlotCertainty.GAME)[1:])
                    except pl.exceptions.RowsError:
                        ssd_pg_game = 0

                    ssd_sum = sum(ssd_pg_certain) + ssd_pg_slot + ssd_pg_game

                    if ssd_sum:
                        if options.time == 'daytime':
                            uncertain_str = (
                                ' | {}{}{}'.format(
                                    f'{ssd_pg_slot}^' if ssd_pg_slot else '',
                                    ', ' if ssd_pg_slot and ssd_pg_game else '',
                                    f'{ssd_pg_game}?' if ssd_pg_game else '',
                                )
                                if ssd_pg_slot or ssd_pg_game
                                else ''
                            )

                            fs.append(
                                '{}{}, {}: {}{} | {}'.format(
                                    qPGName,
                                    f_str,
                                    epText,
                                    ', '.join(str(freq) for freq in ssd_pg_certain),
                                    uncertain_str,
                                    ssd_sum,
                                )
                            )
                        else:
                            fs.append(
                                '{}{}, {}: {} | {}'.format(
                                    qPGName, f_str, epText, ', '.join(str(freq) for freq in ssd_pg_certain), ssd_sum
                                )
                            )
                    else:
                        fs.append(
                            '{}{} {} in {}.'.format(
                                qPGName,
                                f_str,
                                (
                                    'has not been played yet'
                                    if (
                                        not isDate
                                        and (
                                            (options.time == 'daytime' and CURRENT_SEASON in pg_ep)
                                            or (
                                                options.time in ('daytime', 'primetime')
                                                and not any(pg.activeIn(pg_ep) for pg in qPG)
                                            )
                                        )
                                    )
                                    else 'was not played'
                                ),
                                epText if options.time in ('daytime', 'syndicated') else options.time.upper(),
                            )
                        )

        if len(pgQueries) == 1 and not bySeasonBool:
            await ctx.send('`' + sep.join(fs) + '`')
        else:
            await send_long_mes(ctx, sep.join(fs))

    @played.command(name='most', aliases=['m'], with_app_command=False)
    async def mostPlayed(
        self, ctx, slots: slotsStr, pgGroups: commands.Greedy[PGGroupConverter], *, options: MostPlayedFlags
    ):
        """Fetches the N-most playings, out of all PGs (or only those PGs in the PGGroup(s), if given), in the slots given.

        Slots can be any combination of '123456'. The output will do the N-most calculation for each slot individually as well as the sum of the given slots.

        A pgQuery in this context is a valid single PG, or a PGGroup. PGGroups are treated as shorthand for multiple single PGs.

        The dataset used is determined by the time, start and end parameters. For more on these, see the FAQ."""

        try:
            ep, epText, _ = await parse_time_options(ctx, options)
        except ValueError:
            return  # error message sending taken care of in parse_time_options (could have factored in coding better)
        except AssertionError as e:
            await ctx.send(f'`Error parsing time options: {e.message}`')
            return

        # not sure why copy is needed? reset_index?
        sub_slots_df = self.cs.endpoint_sub(ep, options.time, table='slots')

        pgQueries = list(value_chain(*pgGroups)) or list(PG)

        if options.N > len(pgQueries):
            options.N = len(pgQueries)

        filt_exprs = []
        if pgGroups:
            filt_exprs.append(pl.col('PG').is_in([str(pg) for pg in pgQueries]))
        if options.excludeUncertain:
            filt_exprs.append(~has_any_flags('flag', frozenset({2**qu for qu in QU_FLAGS})))
        if filt_exprs:
            sub_slots_df = sub_slots_df.filter(filt_exprs)

        ddf = sub_slots_df.group_by('PG').agg(pl.exclude('flag', 'S').sum()).select(cs.matches(r'^PG\d?$')).collect()

        if ddf.height:
            q = ddf.lazy()
            result = []

            for slot in slots:
                ser = ddf.select('PG', f'PG{slot}').sort(f'PG{slot}', descending=True).head(options.N)
                result.append(
                    slot
                    + ORDINAL_SUFFIXES[int(slot)]
                    + ':'
                    + (' ' if options.N == 1 else '\n')
                    + ('\n'.join(f'\t{pg} ({freq})' for pg, freq in ser.rows()))
                )
            if len(slots) > 1:
                ser = (
                    ddf.select('PG', sum=pl.sum_horizontal(cs.matches(f'PG[{slots}]')))
                    .sort('sum', descending=True)
                    .head(options.N)
                )
                result.append('\nALL:\n' + ('\n'.join(f'\t{pg} ({freq})' for pg, freq in ser.rows())))

            pg_str = (
                'only {} games'.format(', '.join([PG.partition_table.inverse[frozenset(pgG)] for pgG in pgGroups]))
                if pgGroups
                else 'all PGs'
            )
            await send_long_mes(ctx, '{}, top {}, {}:\n\n{}'.format(epText, options.N, pg_str, '\n'.join(result)))
        else:
            await ctx.send(f'`None of the PG(s) given have been / were played in {epText}.`')

    @played.command(aliases=['conflictN', 'cN'], with_app_command=False)
    async def concurrenceN(
        self,
        ctx,
        pg1: PGConverter,
        pg2: Optional[PGConverter],
        pg3: Optional[PGConverter],
        pg4: Optional[PGConverter],
        pg5: Optional[PGConverter],
        *,
        options: ConflictNFlags,
    ):
        """Lists every game (if any) given in pgGroupCompare (if not specified, every other game) that has between played between N1 and N2 entries (inclusive), or exactly N1 entries if only N1 is provided, with the provided pricing game(s) (with the provided flag(s) if given), within the given season range (e.g. inactive games are excluded from the listing).

        N2 must be more than N1 if provided. "Max" or "m" can be provided for N2 to automatically be set to the total number of playings of the provided pricing game. N1 must be non-negative.

        See !flagshelp for more on pgFlags.

        The dataset used is determined by the time, start and end parameters. For more on these, see the FAQ.

        For daytime, playings with the educated guess flag (?) can be optionally excluded.

        PGs are all mapped to at least one single-word key. See the FAQ for a complete listing."""

        pgs = list(itertools.takewhile(operator.truth, ctx.args[2:7]))
        assert len(set(pgs)) == len(pgs)

        N1 = options.N1
        N2 = options.N2
        if type(N2) == str:
            N2 = SI.inf

        if N1 < 0 or (N2 and N1 > N2):
            await ctx.send('`Invalid N parameters. N1 must be >= 0, N2 must be >= N1 if provided.`')
            return

        try:
            ep, epText, _ = await parse_time_options(ctx, options, *pgs)
        except ValueError:
            return  # error message sending taken care of in parse_time_options (could have factored in coding better)
        except AssertionError as e:
            await ctx.send(f'`Error parsing time options: {e.message}`')
            return

        async with ctx.typing():
            flags = [None] * len(pgs)
            fs_str = [None] * len(pgs)
            if options.pgFlags:
                for pg_idx, fs in options.pgFlags:
                    assert pg_idx <= len(pgs), 'Flags given for non-existent PG.'
                    flags[pg_idx - 1] = frozenset(
                        [2**f for f in range(10)]
                        if fs == 'any'
                        else [0 if f.isnumeric() else 2 ** PLAYING_FLAGS_SINGLE.index(f) for f in fs]
                    )
                    fs_str[pg_idx - 1] = (
                        'any flag' if fs == 'any' else '/'.join([PF_TO_DESCRIPTIVE[f] if f else 'no flag' for f in fs])
                    )
                    if 1 in flags[pg_idx - 1] and pgs[pg_idx - 1] in PG.partition_table['CAR_BOATABLE']:
                        fs_str[pg_idx - 1] = fs_str[pg_idx - 1].replace('car', 'boat')

            # determine pg_strs before exclude
            pgs_str = ', '.join(str(pg) + (f' ({fss})' if fss else '') for pg, fss in zip(pgs, fs_str))

            # now, exclude
            if options.excludeEducated:
                flags = [fl - {2**q for q in Q_FLAG} if fl else ALL_FLAGS_BUT_GUESS for fl in flags]

            sub_df = self.cs.concurrence_query(ep, options.time, tuple(pgs), tuple(flags)).collect()
            total_playings = sub_df.height

            # all-Dinko check
            try:
                check = sub_df.row(by_predicate=pl.col('PROD') == '6435K')
                all_dinko = True
            except pl.exceptions.RowsError:
                all_dinko = False

            sub_df = (
                sub_df.select(
                    pl.concat_list(r'^PG\d_p$')
                    .alias('PG')
                    .list.explode()
                    .drop_nulls()
                    .value_counts(sort=True, parallel=True)
                    .struct.rename_fields(('PG', 'count'))
                )
                .unnest('PG')
                .filter(~pl.col('PG').is_in([str(pg) for pg in pgs]))
            )

            if options.pgGroupCompare:
                sub_df = sub_df.filter(pl.col('PG').is_in([str(pg2) for pg2 in options.pgGroupCompare if pg2.activeIn(ep)]))

            if N1 > total_playings:
                N1 = total_playings
            if N2 and N2 > total_playings:
                N2 = total_playings
            if N1 == N2:
                N2 = None

            # all-Dinko show handling. will be true only when exactly only Dinko in PGs and S42 included.
            if all_dinko and (not (fs := flags[0]) or (fs & ALL_PLINKO_FLAGS)):
                val = 5  # 6 - len(pgs)

                sub_df = sub_df.vstack(
                    pl.DataFrame([pl.Series('PG', ['Plinko']), pl.Series('count', [val], dtype=pl.UInt32)])
                    # unfortunately forced to resort here
                ).sort('count', True)

                if N1 == val and not N2:
                    pass
                elif not N2 and N1 != val:
                    if N1 > val:
                        N2 = N1
                        N1 = val
                    else:
                        N2 = val
                elif N1 > val:
                    N1 = val
                elif N2 < val:
                    N2 = val

            if not total_playings:
                pgGroupStr = (
                    ' with {} games'.format(PG.partition_table.inverse[frozenset(options.pgGroupCompare)])
                    if options.pgGroupCompare
                    else ''
                )
                if len(pgs) == 1:
                    await ctx.send(
                        '`{} {} played in {} ({}){}.`'.format(
                            pgs_str,
                            'has not been' if CURRENT_SEASON in ep and not pgs[0].retired else 'was not',
                            options.time if options.time != 'daytime' else 'this time period',
                            epText,
                            pgGroupStr,
                        )
                    )
                else:
                    await ctx.send(
                        '`{} have no concurrences in {} ({}){}.`'.format(
                            pgs_str,
                            options.time if options.time != 'daytime' else 'this time period',
                            epText,
                            pgGroupStr,
                        )
                    )
                return

            if not N1:
                result0 = filter(
                    lambda pg2: pg2 != PG._UNKNOWN if options.time != 'daytime' else pg2.activeIn(ep),
                    (options.pgGroupCompare or set(list(PG))) - {PG.lookup(s) for s in sub_df.to_series()},
                )

            pgGroupStr = (
                ' (only {} games)'.format(PG.partition_table.inverse[frozenset(options.pgGroupCompare)])
                if options.pgGroupCompare
                else ''
            )
            cr_str = 'playing' if len(pgs) == 1 else 'concurrence'

            if N2:
                sdf = sub_df.filter(pl.col('count').is_between(N1, N2, closed='both'))
                # as of between polars 1.5 and 1.9, group names are now tuples of the group_by key(s)
                # also causing the default iteration order to change, so now it's made explicitly by reverse count
                r_text = [
                    f'{N}: ' + ', '.join(sorted(sdfg.to_series()))
                    for (N,), sdfg in sorted(sdf.group_by('count'), key=lambda g: g[0], reverse=True)
                ]

                if not N1 and (zero_str := ', '.join([str(pg2) for pg2 in sorted(result0, key=NAME_ATTRGET)])):
                    r_text.append(f'0: {zero_str}')

                initial_str = '{}, {}, {}{}, out of {} {}{}:'.format(
                    pgs_str,
                    epText,
                    f'{N1} <= N <= {N2}' + pgGroupStr,
                    ', no ? flag' if options.excludeEducated else '',
                    total_playings,
                    cr_str,
                    's' if total_playings != 1 else '',
                )
                total_str = initial_str + '\n\n' + ('\n'.join(r_text) if r_text else 'None')

                await send_long_mes(ctx, total_str)
            else:
                if N1:
                    r_text = (', '.join(sub_df.filter(count=N1).to_series())) or 'None'
                else:
                    r_text = (', '.join([str(pg2) for pg2 in sorted(result0, key=NAME_ATTRGET)])) or 'None'
                await ctx.send(
                    '`{}, {}, {}{}, out of {} {}{}: {}`'.format(
                        pgs_str,
                        epText,
                        f'N = {N1}' + pgGroupStr,
                        ', no ? flag' if options.excludeEducated else '',
                        total_playings,
                        cr_str,
                        's' if total_playings != 1 else '',
                        r_text,
                    )
                )

    @lineup.command(name='prod', aliases=['p', 'production'])
    async def lineupProd(self, ctx, production_numbers: commands.Greedy[prodStr]):
        """Lists every lineup given in a space-separated input of any desired length.

        For valid production code patterns, see the FAQ.
        """
        if not production_numbers:
            raise commands.BadArgument('`Must provide at least one production number.`')

        sent_any = False

        for time in ('daytime', 'primetime', 'syndicated', 'unaired'):
            sub_df = await trim_query(self.cs.get(time).lazy().filter(pl.col('PROD').is_in(production_numbers)))
            if sub_df.height:
                await send_long_mes(ctx, gen_lineup_submes(sub_df, '', time))
                sent_any = True

        if not sent_any:
            await ctx.send('`No existing production codes given.`')

    @lineup.command(name='date', aliases=['d'], with_app_command=False)
    async def lineupDate(
        self, ctx, time: Optional[TimeConverter] = 'daytime', dateFormat: Optional[dateStr] = '%m/%d/%y', *, dates
    ):
        """Lists every lineup that aired on any of the given date(s). Only applies to daytime and primetime.

        Date formatting by default, for example, is "03/26/20" (leading zeros optional)."""
        dates = re.split(r'\s+', dates)
        try:
            dts = [(datetime.strptime(d, dateFormat) - datetime(1970, 1, 1)).days for d in dates]
        except ValueError as e:
            await ctx.send(f'`Malformed date: {e}`')
            return

        df = self.cs.get(time)
        sub_df = await trim_query(df.lazy().filter(pl.col('AIRDATE').dt.epoch('d').is_in(dts)))
        if sub_df.height:
            await send_long_mes(ctx, gen_lineup_submes(sub_df, '', time))
        else:
            await ctx.send(f'`No lineups in {time} for any of these dates.`')

    @lineup.command(name='prod_range', aliases=['prodRange', 'pr', 'productionRange'])
    async def lineupProdRange(self, ctx, start: prodStr, end: prodStr, time: Optional[TimeConverter] = 'daytime'):
        """Lists every lineup in a range of production numbers from start to end, inclusive, in the given time.

        For valid production code patterns, see the FAQ.
        """
        sub_df = self.cs.get(time)
        n_idx = sub_df.columns.index('PG_n')

        try:
            start_idx = sub_df.row(by_predicate=pl.col('PROD') == start)[n_idx]
        except pl.exceptions.RowsError as e:
            await ctx.send(f'`Invalid production code for {time}: {start}`')
            return
        try:
            end_idx = sub_df.row(by_predicate=pl.col('PROD') == end)[n_idx]
        except pl.exceptions.RowsError as e:
            await ctx.send(f'`Invalid production code for {time}: {end}`')
            return

        if start_idx < end_idx:
            await send_long_mes(
                ctx,
                gen_lineup_submes(await trim_query(sub_df.lazy().slice(start_idx - 1, end_idx - start_idx + 1)), '', time),
            )
        else:
            await ctx.send(
                '`No production codes within that range. Are you sure start < end? Are you sure the time is right?`',
                ephemeral=True,
            )

    @lineup.command(name='dateRange', aliases=['dr'], with_app_command=False)
    async def lineupDateRange(
        self,
        ctx,
        start: str,
        end: str,
        step: Optional[freqStr] = '1d',
        time: Optional[TimeConverter] = 'daytime',
        dateFormat: Optional[dateStr] = '%m/%d/%y',
    ):
        """Lists every lineup that aired within the range of start to end, inclusive, stepped by step. Only applies to daytime and primetime.

        Step can be any combination of number of days, weeks, months or years (in any order, e.g. "2mo1y3w4d"). Dates with no shows will automatically be excluded from the listing.

        Date formatting by default, for example, is "03/26/20" (leading zeros optional)."""
        try:
            startDate = datetime.strptime(start, dateFormat).date()
            endDate = date.today() if re.fullmatch('today', end, re.I) else datetime.strptime(end, dateFormat).date()
        except ValueError as e:
            await ctx.send(f'`Malformed input: {e}`')
            return

        dts = pl.date_range(startDate, endDate, step, eager=True)

        df = self.cs.get(time)
        sub_df = await trim_query(df.lazy().filter(pl.col('AIRDATE').is_in(dts)))
        if sub_df.height:
            await send_long_mes(ctx, gen_lineup_submes(sub_df, '', time))
        else:
            await ctx.send(f'`No lineups in {time} for any of these dates.`')

    @lineup.command(name='random', aliases=['r'], with_app_command=False)
    async def lineupRandom(self, ctx, N: Optional[NONNEGATIVE_INT] = 1, *, options: LineupRFlags):
        """Randomly picks and prints out N lineups from the dataset. If sort is False, random print order as well.

        The dataset used is determined by the time, start and end parameters. For more on these, see the FAQ."""

        try:
            ep, epText, isDate = await parse_time_options(ctx, options)
        except ValueError:
            return  # error message sending taken care of in parse_time_options (could have factored in coding better)
        except AssertionError as e:
            await ctx.send(f'`Error parsing time options: {e.message}`')
            return

        df = self.cs.endpoint_sub(ep, options.time).collect()

        if options.sort:
            comp = operator.lt
            extra_str = ' (and randomizing order of)'
        else:
            comp = operator.le
            extra_str = ''

        if comp(N, df.height):
            sub_df = await trim_query(df.sample(n=N, with_replacement=False, shuffle=not options.sort).lazy())
            await send_long_mes(ctx, gen_lineup_submes(sub_df, '', options.time))
        else:
            await ctx.send(f'`No point to picking{extra_str} {N} shows out of a sample size of {df.height}.`')

    @lineup.command(aliases=['s'], with_app_command=False)
    async def search(self, ctx, *, options: SearchFlags):
        """Lists every lineup that matches a set of conditions. If 'all' is given to logicExpr (short for logical expression), all the conditions must match. If 'any' is given, at least one condition must match. Custom logic expressions are also allowed, see the FAQ for more details.

        Each condition is one to four "words", separated by commas or semicolons. See the FAQ for exact formatting details.

        The dataset used is determined by the time, start and end parameters. For more on these, see the FAQ.

        PGs and PGGroups are all mapped to at least one single-word key. See the FAQ for a complete listing.

        If excludeUncertain is True, it's a shorthand for specifying every flag except ^ and ? in every PG condition as the default.

        The resulting lineups can be sorted by production number or date ("sort" option), with the additional option to show the number of shows/days since the prior sorted entry ("since" option).

        A regular expression can be used for the "notes" parameter to search that column of the data. It will be case insensitive. See FAQ for more on this.
        """

        overallCond = []
        condition_strs = []
        warning_strs = SortedSet()
        slot_queried = False
        warned_slot = False
        warned_game = False
        DEFAULT_FLAGS = ALL_FLAGS_BUT_UNCERTAIN if options.excludeUncertain else ANY_FLAG
        used_pg_condition = False

        for cond in options.conditions:
            words = [w.strip() for w in cond.split(';')]

            match [w.upper() for w in words]:
                case ['NOTES' | 'N' | 'SPECIAL' | 'PROD' | 'NUMBER' | 'P' as col, regex]:
                    col = _col_name_remapping.get(col, col)
                    if col == 'NOTES' and options.time != 'daytime':
                        raise ValueError('NOTES only exists in daytime. (SPECIAL is primetime.)')
                    elif col == 'SPECIAL' and options.time != 'primetime':
                        raise ValueError('SPECIAL only exists in primetime. (NOTES is daytime.)')
                    regex = words[1]

                    f = pl.col(col)
                    if col != 'PROD':
                        f = f.cast(str)
                    f = f.str.contains(f'(?i){regex}')
                    cd = f'{col} matches "{regex}" (case-insensitive)'
                case ['SEASON' | 'S' as col, *e]:
                    if options.time == 'primetime':
                        raise ValueError('Season condition is invalid for primetime.')
                    f, cd, _ = build_int_expression(pl.col('S'), e)
                    cd = f'SEASON is {cd}'
                case [
                    'DATE' | 'D' | 'AIR' | 'ID' | 'INT' | 'INTENT' | 'INT. DATE' | 'INTENDED' | 'AIRDATE' as col,
                    'YEAR' | 'Y' | 'MONTH' | 'M' | 'DAY' | 'D' | 'DOW' | 'WKDAY' | 'WEEKDAY' as dt_q,
                    *e,
                ]:
                    col = _col_name_remapping.get(col, col)
                    if options.time == 'syndicated' or (options.time == 'unaired' and col == 'AIRDATE'):
                        raise ValueError(f'{col} condition is invalid for {options.time}.')
                    f, cd = build_dt_q_expression(col, dt_q, e)
                case ['DATE' | 'D' | 'AIR' | 'ID' | 'INTENDED' | 'AIRDATE' as col, *e]:
                    col = _col_name_remapping.get(col, col)
                    if options.time == 'syndicated' or (options.time == 'unaired' and col == 'AIRDATE'):
                        raise ValueError(f'{col} condition is invalid for {options.time}.')
                    e = transform_str_to_dts(e, options.dateFormat)
                    f, cd = build_date_expression(pl.col(col), e, options.dateFormat)
                    cd = f'{col} is {cd}'
                case [pgCondWords]:
                    used_pg_condition = True
                    pgq = None
                    slots = ANY_SLOT
                    flags = DEFAULT_FLAGS
                    freqs = ANY_FREQ

                    for pgCond in cond.split(','):
                        if (
                            pg := PG.lookup_table.get(pgCond) or PG.partition_table.get(PG.partition_lookup.get(pgCond))
                        ) or re.match('^[-*]+$', pgCond):
                            if pgq:
                                raise ValueError(f'More than one PG/PGGroup in condition "{cond}".')
                            pgq = (pg if type(pg) == frozenset else (pg,)) if pg else PG_WILDCARD
                        elif pgCond[0].lower() == 's' and re.match(SLOTS_REGEX, pgc := pgCond[1:]):
                            if slots != ANY_SLOT:
                                raise ValueError(f'More than one slot specification in condition "{cond}".')
                            slots = frozenset(int(i) for i in pgc)
                        elif pgCond[0].lower() == 'h' and re.match(HALF_REGEX, pgc := pgCond[1:]):
                            if slots != ANY_SLOT:
                                raise ValueError(f'More than one slot specification in condition "{cond}".')
                            slots = FIRST_HALF if pgCond[1] == '1' else SECOND_HALF
                        elif pgCond[0].lower() == 'c' and re.match(FREQS_REGEX, pgc := pgCond[1:]):
                            if freqs != ANY_FREQ:
                                raise ValueError(f'More than one count specification in condition "{cond}".')
                            freqs = frozenset(int(i) for i in pgc)
                        elif pgCond[0].lower() == 'f' and re.match(FLAGS_REGEX, pgcu := pgCond[1:].upper()):
                            if flags != DEFAULT_FLAGS:
                                raise ValueError(f'More than one flag specification in condition "{cond}".')
                            flags = tuple(0 if i.isnumeric() else 2 ** PLAYING_FLAGS_SINGLE.index(i) for i in pgcu)
                        else:
                            if pgq:
                                raise ValueError(f'More than one PG/PGGroup in condition "{cond}".')
                            pgq = pgCond

                    if not pgq:
                        raise ValueError(f'No valid PG or wildcard specified in condition "{cond}"')
                    if slots != ANY_SLOT:
                        if type(pgq) is not str and PG._UNKNOWN in pgq:
                            warning_strs.add('DISCLAIMER: Missing games have uncertain slots by definition.')
                        elif SlotCertainty.SLOT in flags:
                            warning_strs.add(
                                'DISCLAIMER: Slotting of uncertainly slotted playings specified in a condition.'
                            )
                            warned_slot = True
                        else:
                            slot_queried = True
                    if options.time == 'syndicated':
                        slots &= FIRST_HALF
                        if not slots:
                            raise ValueError('Must provide at least one "first" half slot in an all half-hour show query.')
                    if SlotCertainty.GAME in flags:
                        warning_strs.add(
                            'DISCLAIMER: Uncertain playing flag specified in a condition. Playings marked with the ? flag belong to a lineup that is, at worst, close to the given production number.'
                        )
                        warned_game = True

                    f = build_lineup_expr(pgq, slots, flags, freqs)

                    slots_l = sorted(slots)
                    slots_str = 'played ' + (
                        'in any slot'
                        if slots == ANY_SLOT or (slots == FIRST_HALF and options.time == 'syndicated')
                        else (
                            'in the first half'
                            if slots == FIRST_HALF
                            else (
                                'in the second half'
                                if slots == SECOND_HALF
                                else (
                                    str(slots_l[0]) + ORDINAL_SUFFIXES[slots_l[0]]
                                    if len(slots) == 1
                                    else (
                                        ', '.join(str(s) + ORDINAL_SUFFIXES[s] for s in slots_l[:-1])
                                        + (',' if len(slots) != 2 else '')
                                        + ' or '
                                        + str(slots_l[-1])
                                        + ORDINAL_SUFFIXES[slots_l[-1]]
                                    )
                                )
                            )
                        )
                    )

                    flags_str = (
                        ''
                        if flags == ANY_FLAG
                        else (
                            'with no guess flags'
                            if flags == ALL_FLAGS_BUT_UNCERTAIN
                            else (
                                f'with the "{PLAYING_FLAGS_SINGLE[int(np.log2(flags[0]))] if flags[0] else 0}" flag'
                                if len(flags) == 1
                                else 'with at least one of the "'
                                + ''.join(PLAYING_FLAGS_SINGLE[int(np.log2(j))] if j else '0' for j in flags)
                                + '" flags'
                            )
                        )
                    )

                    freqs_str = '' if freqs == ANY_FREQ else ' (x' + ','.join(str(i) for i in sorted(freqs)) + '),'

                    pg_str = (
                        (
                            str(pgq[0])
                            if len(pgq) == 1
                            else (
                                ('any ' + pti + ('' if pti.endswith('PRIZER') else ' game'))
                                if (pti := PG.partition_table.inverse.get(pgq)) != 'any game'
                                else pti
                            )
                        )
                        if type(pgq) is not str
                        else f'PG matches "{pgq}" (case-insensitive)'
                    )

                    cd = (
                        pg_str
                        + freqs_str
                        + (', ' if type(pgq) is str else ' ')
                        + slots_str
                        + (', ' + flags_str if flags_str else '')
                    )
                case _:
                    raise ValueError(f'Malformed condition: {cond}')

            overallCond.append(f)
            condition_strs.append(cd)

        if len(overallCond) > 26:
            raise ValueError(
                "More than 26 conditions in a custom logical expression. You don't need anywhere near this many, stop trying to break me!`"
            )
        elif (sym_free := len(set(re.findall('[A-Z]', options.logicExpr)))) and sym_free != len(overallCond):
            raise ValueError(
                f'`Logical expression mismatch. Expecting {len(overallCond)} variables, got {sym_free} instead in "{options.logicExpr}"`'
            )

        async with ctx.typing():
            if options.logicExpr == 'all':
                total_expr = pl.all_horizontal(overallCond)
            elif options.logicExpr == 'any':
                total_expr = pl.any_horizontal(overallCond)
            else:
                l = locals()
                l |= {letter: fe for fe, letter in zip(overallCond, string.ascii_uppercase)}
                total_expr = eval(re.sub('([A-Z])', r'(\1)', options.logicExpr))

            sub_df = await trim_query(
                self.cs.endpoint_sub(None, options.time).filter(total_expr), options.sortBy, options.since
            )

            all_full_hour = not (
                options.time == 'syndicated'
                or (sub_df.select(pl.col('PG6').is_null().any()).item() if sub_df.height else True)
            )

            final_cond_str = (
                ''
                if len(overallCond) == 1 and not sym_free
                else (f'\n{options.logicExpr} ; where\n\n' if sym_free else f'{options.logicExpr} of\n')
            ) + '\n'.join(
                [
                    (l + ' = ' if sym_free else ('* ' if len(overallCond) > 1 else '')) + cs
                    for l, cs in zip(string.ascii_uppercase, condition_strs)
                ]
            )

            initial_str = '{} lineup{} found in {} for {}'.format(
                sub_df.height,
                '' if sub_df.height == 1 else 's',
                options.time.upper(),
                final_cond_str,
            )

            main_mes = gen_lineup_submes(sub_df, initial_str, options.time)

            if slot_queried and not warned_slot and '(^)' in main_mes:
                warning_strs.add('DISCLAIMER: Slotting of uncertainly slotted playings factored into results.')
            if not warned_game and '(?)' in main_mes:
                warning_strs.add(
                    'DISCLAIMER: Playings marked with the ? flag belong to a lineup that is, at worst, close to the given production number.'
                )

        await send_long_mes(ctx, '\n'.join(warning_strs) + ('\n\n' if warning_strs else '') + main_mes)

    @played.command(name='last', aliases=['l'], with_app_command=False)
    async def lastPlayed(
        self,
        ctx,
        pgs: commands.Greedy[Union[PGConverter, PGGroupConverter, dayProdStr]],
        nth: Optional[NONNEGATIVE_INT] = 1,
        *,
        options: LastPlayedFlags,
    ):
        """Lists the n-th to last playing in daytime (production number and airdate) of the given PGs, with the playing flag if given. PGGroups and (daytime-only) lineup codes can be included as shorthand for multiple games.

        If asOf is given (a daytime-only production number). start searching backwards from that lineup.

        If pgFlag is provided, must be exactly one character (see !flags for more info).

        Results are listed in sorted airdate order. If sortBy is prod, sort by production number instead.

        If you try to go back too far on a game, it will cap at the game's premiere."""

        try:
            unfound_pgs = set()
            for q in pgs:
                if type(q) is str:
                    r = self.cs.get('daytime').row(by_predicate=pl.col('PROD') == q, named=True)
                    unfound_pgs |= {PG.lookup(r[f'PG{i}_p']) for i in range(1, 7 if r['PG4_p'] else 4)}
                elif type(q) is PG:
                    unfound_pgs.add(q)
                else:
                    unfound_pgs |= q

            if options.asOf:
                cutoff = self.cs.get('daytime').select('PROD', 'PG_n').row(by_predicate=pl.col('PROD') == options.asOf)[1]
        except pl.exceptions.RowsError:
            await ctx.send(
                '(One of) the daytime codes given to `pgs` or `asOf`, despite being properly formatted, does not exist. Some codes do get skipped.',
                ephemeral=True,
            )
            return

        if options.activeOnly:
            unfound_pgs &= PG.partition_table['ACTIVE']
        if not unfound_pgs:
            await ctx.send('`No PGs given. (If doing retired games only, set activeOnly to False.)`')
            return

        seasons = SI.closed(1, CURRENT_SEASON) & reduce(operator.or_, [pg.activeSeasons for pg in unfound_pgs])

        if options.pgFlag:
            extra_str = (
                PLAYING_FLAGS[len(PLAYING_FLAGS) - 1 - PLAYING_FLAGS_SINGLE.index(options.pgFlag)]
                if options.pgFlag != '0'
                else 'no flag'
            )
            options.pgFlag = 2 ** PLAYING_FLAGS_SINGLE.index(options.pgFlag) if options.pgFlag != '0' else 0
            fs = frozenset((options.pgFlag,))
        else:
            fs = ANY_FLAG

        unfound_pgs = {str(pg) for pg in unfound_pgs}

        async with ctx.typing():
            results = []
            q = self.cs.endpoint_sub(seasons, 'daytime')

            for pg in unfound_pgs:
                e = [build_lineup_expr(frozenset({pg}), ANY_SLOT, fs, ANY_FREQ)]
                if options.asOf:
                    e.append(pl.col('PG_n') <= cutoff)

                qq = q.filter(e).select('PROD', 'AIRDATE')
                if options.sortBy == 'date':
                    qq = qq.sort('AIRDATE')
                qq = qq.tail(nth).collect()
                if qq.height:
                    prod, airdate = qq.row(0)
                    results.append((pg, prod, airdate))

            if results:
                results.sort(key=lambda t: SORT_PROD(t[1]) if options.sortBy == 'prod' else t[2], reverse=True)
                extra = f' ({extra_str})' if options.pgFlag is not None else ''
                if options.sortBy == 'prod':
                    result_strs = [f'{pg}{extra}: {ind}, ' + ts.strftime('%b %d %Y') for pg, ind, ts in results]
                else:
                    result_strs = [f'{pg}{extra}: ' + ts.strftime('%b %d %Y') + f', {ind}' for pg, ind, ts in results]

                if options.pgFlag == 1:
                    for e, r in enumerate(results):
                        pg, _, _ = r
                        if PG.lookup(pg) in PG.partition_table['CAR_BOATABLE']:
                            result_strs[e] = result_strs[e].replace('car', 'boat')

                if len(result_strs) > 1:
                    await send_long_mes(ctx, '\n'.join(result_strs), newline_limit=14)
                else:
                    await ctx.send(f'`{result_strs[0]}`')
            else:
                await ctx.send(
                    f'`No results found. (Check the flag, nth, activeOnly options if you were expecting results.)`'
                )

    @played.command(aliases=['proj', 'p'], with_app_command=False)
    async def projected(self, ctx, pgs: commands.Greedy[Union[PGConverter, PGGroupConverter, dayProdStr]]):
        """Does a simple pro-rated calculation projecting the number of playings for the PG this season, then if possible, compares that projection to this completed calculation for last season. PGGroups, or a (daytime-only) lineup code, can be included as shorthand for multiple games.

        Each game must be active in the current season to show as output in this command.

        This command makes the most sense to run in the second half of an ongoing season, or over summer break."""

        try:
            qs = []
            for q in pgs:
                if type(q) is str:
                    qs.extend(
                        [
                            PG.lookup(p)
                            for p in self.cs.get('daytime')
                            .select(pl.col('PROD') | cs.matches(r'^PG\d_p$'))
                            .row(by_predicate=pl.col('PROD') == q)[1:]
                            if p
                        ]
                    )
                elif type(q) is PG:
                    qs.append(q)
                else:
                    qs.extend(q)
        except pl.exceptions.RowsError:
            await ctx.send(
                '(One of) the daytime codes given to `pgs` or `asOf`, despite being properly formatted, does not exist. Some codes do get skipped.',
                ephemeral=True,
            )
            return

        # this is the recommended way to remove duplicates while keeping order (no set)
        pgs = list(OrderedDict.fromkeys(filter(lambda p: p.activeIn(CURRENT_SEASON), qs)))

        if not pgs:
            await ctx.send(
                '`No PGs given. (Check the PG is active in S{}-{}, and check your spelling from leftmost.)`'.format(
                    CURRENT_SEASON - 1, CURRENT_SEASON
                ),
                ephemeral=True,
            )
        else:
            pgs = [str(pg) for pg in pgs]
            async with ctx.typing():
                sub_slots_df = self.cs.endpoint_sub(SI.closed(CURRENT_SEASON - 1, CURRENT_SEASON), 'daytime', table='slots')
                prior_count, current_count = (
                    self.cs.get('daytime')
                    .filter(pl.col('S') >= CURRENT_SEASON - 1)
                    .select(pl.col('S').value_counts(sort=True, parallel=True))
                    .unnest('S')
                    .to_series(1)
                )

                res = []
                current = (
                    sub_slots_df.filter(pl.col('PG').is_in(pgs), S=CURRENT_SEASON)
                    .drop('flag', 'S')
                    .group_by('PG')
                    .sum()
                    .select(
                        [
                            pl.col('PG'),
                            (pl.fold(pl.lit(0), operator.add, pl.exclude('PG')) * 190.0 / current_count)
                            .round(0)
                            .cast(pl.Int8)
                            .alias(f'S{CURRENT_SEASON}'),
                        ]
                    )
                )
                prior = (
                    sub_slots_df.filter(pl.col('PG').is_in(pgs), S=CURRENT_SEASON - 1)
                    .drop('flag', 'S')
                    .group_by('PG')
                    .sum()
                    .select(
                        [
                            pl.col('PG'),
                            (pl.fold(pl.lit(0), operator.add, pl.exclude('PG')) * 190.0 / prior_count)
                            .round(0)
                            .cast(pl.Int8)
                            .alias(f'S{CURRENT_SEASON-1}'),
                        ]
                    )
                )

                result_df = (
                    current.join(prior, on='PG', how='outer')
                    .with_columns(pl.coalesce('PG', 'PG_right').alias('PG'))
                    .drop('PG_right')
                    .fill_null(strategy='zero')
                    .with_columns((pl.col(f'S{CURRENT_SEASON}') - pl.col(f'S{CURRENT_SEASON-1}')).alias('DIF'))
                    .sort('DIF')
                )
                result_df = result_df.with_columns(
                    pl.when(pl.col('DIF') >= 0).then(('+' + pl.col('DIF').cast(str)).alias('DIF')).otherwise(pl.col('DIF'))
                )

                await send_long_mes(ctx, ppp(result_df.collect().rename({'PG': ''})), newline_limit=14)

    @lineup.command(aliases=['c'])
    async def count(
        self, ctx, seasons: commands.Greedy[SEASON_RANGE], do_range: bool = False, time: Optional[TimeConverter] = 'daytime'
    ):
        """Prints the number of episodes (currently) in the lineup sheet for the given seasons.

        If do_range is True, treat each pair of input seasons as a "range" (for example, "1 3 7 9" would cover seasons 1-3 and 7-9).
        """
        if seasons:
            assert time != 'primetime', 'Primetime does not have seasons.'
            if len(seasons) > 1 and do_range:
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

            await send_long_mes(
                ctx,
                ppp(
                    self.cs.get(time)
                    .group_by('S')
                    .count()
                    .rename({'count': 'EPS'})
                    .filter(pl.col('S').is_in(seasons))
                    .sort('S')
                ),
            )
        else:
            await ctx.send(f'`{len(self.cs.get(time))}`')

    @lineup.command(aliases=['g'])
    async def generate(self, ctx, *, options: GenerateFlags):
        """Generates N random lineups, up to 5 (to simulate a standard week if unique is True).

        For full-hour shows, "smart" logic can be attempted (default True) to create a plausible modern-day lineup. The logic here is very experimental. Otherwise, the lineup is purely random.

        By default, only includes active games. Set retired=True to include those."""
        pgs = set(PG.partition_table['ACTIVE'])
        if options.retired:
            pgs |= PG.partition_table['RETIRED']

        lineup_strs = []
        for n in range(options.N):
            pg_sample = copy(pgs)

            if options.smart and not options.half_hour:
                nPGs, non_car = self.cs.gen_lineup(pg_sample)
                lineup_strs.append(', '.join([f'{pg}{" (car)" if n else ""}' for pg, n in zip(nPGs, non_car)]))
            else:
                nPGs = random.sample(tuple(pg_sample), k=3 if options.half_hour else 6)
                lineup_strs.append(', '.join([str(pg) for pg in nPGs]))

            if options.unique:
                pgs -= set(nPGs)

        await ctx.send(('>>> ' if options.N > 1 else '') + '\n'.join([f'`{ls}`' for ls in lineup_strs]))

    @lineup.command(aliases=['a', 'notes', 'n'])
    async def addendum(self, ctx):
        """Prints a static message with some extra explanation on a few oddities in the lineup database."""
        await ctx.send(self.cs.notes)

    @lineup.command(aliases=['flags', 'f'])
    async def flag(self, ctx, flags: Optional[str]):
        """Prints a static message with explanations on PG playing flags. If no flags are given, print all."""
        if flags:
            flag_strs = []
            for f in flags:
                try:
                    flag_strs.append(f'`{f}` {FLAG_INFO[f]}')
                except KeyError:
                    pass
            if flag_strs:
                await ctx.send('>>> ' + '\n'.join(flag_strs))
            else:
                await ctx.send('`None of those flags exist. Valid flags: ' + (''.join(FLAG_INFO.keys())) + '`')
        else:
            await ctx.send('>>> ' + '\n'.join(f'`{k}` {v}' for k, v in FLAG_INFO.items()))

    @lineup.command()
    async def excel(self, ctx):
        """Returns a neatly printed Excel file containing wayo.py's whole lineup database."""
        with self.cs.write_excel() as b:
            b.seek(0)
            await ctx.send(file=discord.File(b, filename='lineups.xlsx'))

    @lineup.command(hidden=True)
    @commands.is_owner()
    async def reload(self, ctx):
        await ctx.message.add_reaction('🚧')
        _log.info('start loading cs at ' + str(datetime.now()))
        async with self.latest_lock:
            await asyncio.to_thread(self.cs.initialize)
        _log.info('end loading cs at ' + str(datetime.now()))
        await ctx.message.remove_reaction('🚧', ctx.bot.user)
        await ctx.message.add_reaction('✅')

    async def cog_command_error(self, ctx, e):
        if ctx.command.name == 'search':
            if isinstance(e, (commands.errors.MissingRequiredArgument, commands.errors.MissingRequiredFlag)):
                await ctx.send('`At least one condition required. Use "condition=" or "cond=" (new syntax).`')
            elif isinstance(e, commands.ConversionError):
                if isinstance(e.original, AssertionError):
                    await ctx.send('`Logic expression does not evaluate to True or False.`')
                elif isinstance(e.original, TypeError):
                    await ctx.send('`Malformed logic expression.`')
            elif isinstance(e, commands.CommandInvokeError):
                await ctx.send(f'`{e.original}`')
            else:
                await ctx.send(f'`{e}`')
        elif ctx.command.name == 'edit':
            if hasattr(e, 'original') and isinstance(e.original, AssertionError):
                await ctx.send('`At least one of intended date, airdate, notes must be specified.`')
            elif isinstance(e, (commands.errors.MissingRequiredArgument, commands.errors.MissingRequiredFlag)):
                await ctx.send('`prodNumber required.`')
            else:
                await ctx.send(f'`{e}`')
        else:
            if isinstance(e, commands.ConversionError):
                if isinstance(e.original, KeyError):
                    await ctx.send(f'`The following is not a PG (or PGGroup, if the command takes one): {e.original}`')
                elif isinstance(e.original, ValueError):
                    await ctx.send(f'`The following value is not properly formatted: {e.original}`')
                else:
                    await ctx.send(f'`{e.__cause__}`')
            elif isinstance(e, commands.BadArgument):
                await ctx.send(f'`{e}`')
            elif isinstance(e, commands.CommandError):  # and isinstance(e.original, AssertionError):
                await ctx.send(f'`{e}`')
            else:
                pass  # wayo.py


async def setup(bot):
    await bot.add_cog(LineupCog(bot))

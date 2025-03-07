import asyncio
import itertools
import logging
import random
import re
from collections import OrderedDict
from datetime import *

import texttable
from discord import app_commands
from discord.ext import commands

from dropboxwayo import dropboxwayo
from pg import CURRENT_SEASON
from secretswayo import GR_PW
from util import (
    ARP_RANGE,
    MAX_MES_SIZE,
    POSITIVE_INT,
    SCHEDULER_TZ,
    PGConverter,
    csspoints,
    find_nth,
    pg_autocomplete,
    send_long_mes,
)

_log = logging.getLogger('wayo_log')


async def get_gr_css(asession, logged_in, cookie_length):
    if not logged_in:
        # gr.py header
        hidden_inputs = (await asession.get('http://www.golden-road.net/index.php?action=login')).html.find(
            'form#guest_form > input[type=hidden]'
        )

        if not hidden_inputs:
            raise RuntimeError('`G-R.net login unsuccessful, try again later.`')

        session_var = hidden_inputs[-1].attrs['name']
        session_id = hidden_inputs[-1].attrs['value']

        local_time = datetime.now()
        homepage = await asession.post(
            'http://www.golden-road.net/index.php?action=login2',
            data={
                'user': 'Wayoshi',
                'passwrd': GR_PW,
                'cookielength': cookie_length,
                'hash_passwrd': '',
                session_var: session_id,
            },
        )
    else:
        homepage = await asession.get('http://www.golden-road.net/index.php')

    try:
        forum_entry_time = datetime.strptime(
            homepage.html.find('div#upper_section li:last-child', first=True).text, '%B %d, %Y, %I:%M:%S %p'
        )
        difference_time = forum_entry_time - local_time
    except (ValueError, AttributeError):
        difference_time = timedelta()

    # gr.py get_pms(3,0)
    p = list()
    for i in range(3):
        pm_html = (
            await asession.get('http://www.golden-road.net/index.php?action=pm;f=inbox;sort=date;desc;start=' + str(i * 15))
        ).html
        pms = pm_html.find('div.clear')
        for pm in pms:
            username = pm.find('h4 > a', first=True).text

            ts_str = re.search(
                '((Today|Yesterday) at|\w+ \d{2}, \d{4},) \d{2}:\d{2}:\d{2} [AP]M',
                pm.find('span.smalltext', first=True).text,
            ).group()
            timestamp = datetime.strptime(
                ts_str,
                (
                    'Today at'
                    if ts_str.startswith('Today')
                    else 'Yesterday at' if ts_str.startswith('Yesterday') else '%B %d, %Y,'
                )
                + ' %I:%M:%S %p',
            )

            if ts_str.startswith('Today'):
                timestamp = datetime.combine(date.today(), timestamp.time())
            elif ts_str.startswith('Yesterday'):
                timestamp = datetime.combine(date.today() - timedelta(1), timestamp.time())

            message = pm.find('div.post > div.inner', first=True).text
            p.append((username, timestamp, message))

    return p, difference_time


async def get_bids(asession, logged_in, cookie_length, csshour, cssmin, csssec):
    csv_dict = {}
    hour = csshour
    # 57/58 -> 52, 27/28 -> 22, etc.
    min_time = time(hour, 10 * (cssmin // 10) + 2)

    pms, difference_time = await get_gr_css(asession, logged_in, cookie_length)

    # hour issue in heroku irrelevant with % 3600
    min, sec = divmod(round(difference_time.total_seconds()) % 3600, 60)
    minplus, sec = divmod(csssec + sec, 60)
    min = cssmin + min + minplus
    try:
        max_time = time(hour=hour, minute=min, second=sec)
    except ValueError:
        _log.warning(
            f'Invalid min/sec calculated: minute calc was {min}, second calc was {sec}. Using given min/sec instead.'
        )
        max_time = time(hour=hour, minute=cssmin, second=csssec)

    def get_message_nums(message):
        return [int(re.sub('[^\d]', '', s)) for s in re.findall('\d[\d,]*', message)]

    for u, ts, m in reversed(pms):
        if ts.date() == date.today() and min_time <= ts.time() <= max_time:
            bd = [bid for bid in get_message_nums(m) if bid >= 10000]
            if len(bd) >= 2:
                csv_dict[u] = bd[:2]

    return (csv_dict, max_time)


def bid_text(num):
    return '${:,d}'.format(num) if num >= 0 else '-${:,d}'.format(-num)


class CSSLiveFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    hour: commands.Range[int, 0, 23] = 11
    minute: commands.Range[int, 0, 59] = commands.flag(aliases=['min'], default=56)
    second: commands.Range[int, 0, 59] = commands.flag(aliases=['sec'], default=30)
    pages: POSITIVE_INT = 3


# https://pastebin.com/Yn2utiXK


def resultsStr(s):
    if re.fullmatch('[LW]{6}', s, re.I):
        return s.upper()
    else:
        raise commands.BadArgument('Results must be six of W or L exactly.')


valid_confidence = [
    ''.join([str(s) for s in se])
    for se in (set(itertools.permutations(range(1, 7))) | set(itertools.permutations([1, 1, 3, 4, 5, 6])))
]


def confidenceStr(s):
    if s in valid_confidence:
        return s
    else:
        raise commands.BadArgument('Confidence must be unique permutation of 1-6 (two 1s allowed with no 2s).')


def fpg_score(lineup, master):
    mp = 0
    cp = 0
    pgM = [pg for pg, _ in master]
    for e, l in enumerate(lineup):
        pg, r, c = l
        try:
            idx = pgM.index(pg)
        except ValueError:
            continue

        mpp = 1
        cpp = c
        if idx == e:
            mpp *= 2
            cpp *= 2
        if r == master[idx][1]:
            mpp *= 2
            cpp *= 2
        mp += mpp
        cp += cpp
    return '{}/{}'.format(mp, cp)


class PlayAlongCog(commands.Cog, name='PlayAlong'):
    """Commands related to the Play-Along section of G-R.net."""

    def __init__(self, bot):
        self.bot = bot
        self.stop_csslive = False
        self.lock = asyncio.Lock()
        self.logged_in = False
        self.cookie_length = 5
        self.master = []

        self.csslive.add_check(self.csslive_check)
        self.stop_live.add_check(self.csslive_check)

    @commands.Cog.listener()
    async def on_ready(self):
        self._csslive_channel = self.bot.get_channel(491321260819873807)

    @commands.hybrid_group(aliases=['c'], case_insensitive=True)
    async def css(self, ctx):
        """Commands related to Chatroom Showcase Showoff, a G-R.net forums game based on bidding on showcases."""
        if not ctx.invoked_subcommand:
            await ctx.send('Invalid subcommand (see `help css`).')

    @css.command(name='calc', aliases=['c'])
    @app_commands.describe(
        classic='Default is False. If True, the DSW cut-off will be $100 (exclusive) instead of $250 (inclusive).'
    )
    async def csscalc(
        self, ctx, bid1: POSITIVE_INT, bid2: POSITIVE_INT, arp1: POSITIVE_INT, arp2: POSITIVE_INT, classic: bool = False
    ):
        """Calculate the raw CSS score for the given bids and ARPs.

        The DSW/QSW/EXACTA bonuses are automatically applied if achieved; any other bonuses are left out.

        If classic is True, the DSW cut-off will be $100 (exclusive) instead of $250 (inclusive)."""

        await ctx.send(
            '{}: `{:.2f}`'.format(ctx.author.mention, csspoints(bid1, bid2, arp1, arp2, dsw_diff=99 if classic else 250))
        )

    @css.command(name='live', aliases=['l'])
    @app_commands.guilds(314598609591074816)
    @app_commands.default_permissions(kick_members=True)
    @app_commands.guild_only()
    @app_commands.describe(
        hour='24-hour format. For primetime, most shows would thus be hour 20.',
        pages='Number of G-R.net PM pages to search for bids. The default, 3, is almost always sufficient.',
    )
    @commands.max_concurrency(1)
    @commands.has_any_role('Moderator', 'Admin', 'Moderator-Alumnus')
    async def csslive(
        self, ctx, bid1: POSITIVE_INT, bid2: POSITIVE_INT, arp1: ARP_RANGE, arp2: ARP_RANGE, *, csstime: CSSLiveFlags
    ):
        """Given Stagey's bids and ARPs, fetch today's bids and reveal CSS scores, "live". Moderator only.

        Minute/second cutoff is attempted to be adjusted by up to a few second(s) by comparing the bot's local time to the forum time.

        Hour is 24-hour format. For primetime, most shows would thus be 20 (8pm-9pm).

        Pages is the number of private message pages on G-R.net (15 per page) to search for bids. 3 is almost always sufficient.
        """

        # If "post" is in the extra string, post these raw results to the CSS forum. Only Wayoshi can do this.

        async with ctx.typing():
            css = OrderedDict({'STAGE PLAYER': (bid1, bid2)})
            try:
                cssbids, cutoff_time = await get_bids(
                    self.bot.asession, self.logged_in, self.cookie_length, csstime.hour, csstime.minute, csstime.second
                )
            except RuntimeError as e:
                await ctx.send(e)
                return
            # del cssbids['tpirfan20251']

            self.logged_in = True
            self.bot.SCHEDULER.add_job(
                self.reset_login, 'date', run_date=datetime.now(tz=SCHEDULER_TZ) + timedelta(minutes=self.cookie_length)
            )

            if not cssbids:
                await ctx.send("No bids today... you sure it's a CSS day? If so, you sure it's the afternoon yet?")
                return
            else:
                css.update(cssbids)

            # await ctx.channel.purge(limit=None, before=ctx.message, check=lambda m : not m.pinned)

            m0 = await ctx.send("Today's cutoff time is `" + cutoff_time.strftime('%H:%M:%S') + '`.')

            def construct_line(player, b1, b2):
                diff1 = arp1 - b1
                diff2 = arp2 - b2
                return (
                    player,
                    bid_text(b1),
                    bid_text(arp1 - b1),
                    bid_text(b2),
                    bid_text(arp2 - b2),
                    bid_text(diff1 + diff2) if diff1 >= 0 and diff2 >= 0 else 'OVER',
                    csspoints(b1, b2, arp1, arp2),
                )

            ttable = texttable.Texttable(max_width=0)
            ttable.header(['PLAYER', 'SC1 BID', 'SC1DIFF', 'SC2 BID', 'SC2DIFF', 'TOTDIFF', 'POINTS'])
            ttable.set_cols_align(["l", "r", "r", "r", "r", "r", "r"])
            ttable.set_cols_dtype(['t'] * 6 + ['f'])
            ttable.set_precision(2)

            cssbid_items = list(css.items())

            ttable.add_rows(
                [
                    construct_line(player, *bids)
                    for player, bids in ([cssbid_items[0]] + sorted(cssbid_items[1:], key=lambda r: random.random()))
                ],
                header=False,
            )

            raw_tdraw = ttable.draw().split('\n')
            split_index = (MAX_MES_SIZE - 6) // (len(raw_tdraw[0]) + 1)  # two ``` for code block bookends
            if not split_index % 2:
                split_index -= 1
            # 0 to 25, 24 to 49, etc. (include borders twice in-between)
            tdraw = [
                '\n' + ('\n'.join(raw_tdraw[i - j : i + split_index - j]))
                for j, i in enumerate(range(0, len(raw_tdraw) + 1, split_index))
            ]

            start = [find_nth(tdraw[0], '\n|', 3) + 1] + [tdraw[i].index('|') for i in range(1, len(tdraw))]
            max_player_len = max(len(p) for p in css.keys())
            mes = [await ctx.send('```' + td[:s] + '```') for td, s in zip(tdraw, start)]

            arp1_string = str(arp1)
            arp1_string = arp1_string[:-3] + ',' + arp1_string[-3:]
            arp2_string = str(arp2)
            arp2_string = arp2_string[:-3] + ',' + arp2_string[-3:]

            for m, td, s in zip(mes, tdraw, start):
                stop = False
                try:
                    while not stop:
                        async with self.lock:
                            stop = self.stop_csslive
                        if stop:
                            continue

                        s += max_player_len + 6  # three spaces, two |'s, one $
                        await m.edit(content=f'```{td[:s]}```')
                        for k in range(0, 2):
                            arps = arp2_string if k else arp1_string
                            await asyncio.sleep(2)

                            first_extra_wait = False
                            for i in range(0, 3):
                                s += 2
                                await m.edit(content=f'```{td[:s]}```')
                                await asyncio.sleep(1)

                                if i == 0 and td[s - 2 : s] == arps[:2]:
                                    await asyncio.sleep(1.5)
                                    first_extra_wait = True
                                elif first_extra_wait and td[s - 1] == arps[3]:
                                    await asyncio.sleep(3)
                                    first_extra_wait = False

                            if not k:
                                s += 14
                                await m.edit(content=f'```{td[:s]}```')

                        try:
                            while td[s] != '\n':
                                s += 1
                            s += 1
                            while td[s] != '\n':
                                s += 1
                            s += 1
                        except IndexError:
                            break

                        await m.edit(content=f'```{td[:s]}```')
                        await asyncio.sleep(3)
                except (OSError, commands.CommandError) as e:
                    await ctx.send(f'`I hit an error, fully revealing this part of the results now: {e}`')
                    await asyncio.sleep(1)
                finally:
                    if stop:
                        await m.delete()
                    else:
                        await m.edit(content=f'```{td}```')

        # if 'post' in extra and ctx.author == WB.wayo_user:
        # 	subject = 'CSS Raw Results for {0:s}'.format(date.now().strftime('%m/%d/%Y'))
        # 	message = f'[code]{ttable}[/code]'
        # 	from gr import create_topic
        # 	gr.create_topic(13, subject, message)
        # 	del create_topic

        if self.stop_csslive:
            await m0.delete()

        async with self.lock:
            self.stop_csslive = False

    @css.command(name='stop', with_app_command=False)
    @commands.has_any_role('Moderator', 'Admin', 'Moderator-Alumnus')
    async def stop_live(self, ctx):
        """Stop a running CSS live, in case of a mistake. Text-only command.

        This command will only have an effect when "css live" is running. Moderator permissions required."""
        async with self.lock:
            self.stop_csslive = True
        await ctx.send('Stopping CSS Live... get the parameter order right this time!')

    @css.command()
    async def info(self, ctx, theme1: str.upper, placard1: str.upper, theme2: str.upper, placard2: str.upper):
        """Add the info of today's SCs to Wayoshi's Dropbox text file. Thanks for helping him!"""
        await ctx.message.add_reaction('🚧')

        txt = ''
        for arg in ctx.args[2:5]:
            txt += arg + ('\t' * max(1, 5 - len(arg) // 4))
        txt += ctx.args[-1]

        res = await asyncio.to_thread(
            dropboxwayo.update_str,
            date.today().strftime('%m/%d/%y') + '\t' + txt + '\n',
            f'/gr/css/sc_info_{CURRENT_SEASON-32}.txt',
            False,
        )

        await ctx.message.remove_reaction('🚧', ctx.bot.user)
        await ctx.message.add_reaction('❌' if 'failed' in res else '✅')

    async def csslive_check(self, ctx):
        return ctx.channel == self._csslive_channel

    async def reset_login(self):
        async with self.lock:
            self.logged_in = False

    @commands.hybrid_group(aliases=['f'], case_insensitive=True)
    async def fpg(self, ctx):
        """Commands related to Friday Prediction Game, a G-R.net forums game based on lineup prediction."""
        if not ctx.invoked_subcommand:
            await ctx.send('Invalid subcommand (see `help fpg`).')

    @fpg.command(name='set', aliases=['s'])
    @app_commands.autocomplete(
        pg1=pg_autocomplete,
        pg2=pg_autocomplete,
        pg3=pg_autocomplete,
        pg4=pg_autocomplete,
        pg5=pg_autocomplete,
        pg6=pg_autocomplete,
    )
    @app_commands.describe(results='Exactly 6 of "W" or "L".')
    async def fpgset(
        self,
        ctx,
        pg1: PGConverter,
        pg2: PGConverter,
        pg3: PGConverter,
        pg4: PGConverter,
        pg5: PGConverter,
        pg6: PGConverter,
        results: resultsStr,
    ):
        """Set the lineup & results to score against."""
        self.master = [(pg, r) for pg, r in zip(ctx.args[2:8], results)]
        await ctx.send('Lineup set:\n>>> ' + ('\n'.join([f'{pg} ({r})' for pg, r in self.master])))

    @fpg.command(name='calc', aliases=['c'])
    @app_commands.autocomplete(
        pg1=pg_autocomplete,
        pg2=pg_autocomplete,
        pg3=pg_autocomplete,
        pg4=pg_autocomplete,
        pg5=pg_autocomplete,
        pg6=pg_autocomplete,
    )
    @app_commands.describe(
        results='Exactly 6 of "W" or "L".',
        confidence="Exactly 6 unique values of 1-6, except two 1s allowed in the case of spoilers.",
    )
    async def fpgcalc(
        self,
        ctx,
        pg1: PGConverter,
        pg2: PGConverter,
        pg3: PGConverter,
        pg4: PGConverter,
        pg5: PGConverter,
        pg6: PGConverter,
        results: resultsStr,
        confidence: confidenceStr,
    ):
        """Calculate your FPG score."""
        if not self.master:
            await ctx.send('Lineup not set yet (use `!fpg set`).')
        else:
            await ctx.send(
                '{}: `{}`'.format(
                    ctx.author.mention,
                    fpg_score([(pg, r, int(c)) for pg, r, c in zip(ctx.args[2:8], results, confidence)], self.master),
                )
            )

    async def cog_command_error(self, ctx, e):
        if isinstance(e, (commands.BadArgument, commands.RangeError)):
            await ctx.send(f'`{e}`')
        elif isinstance(e, commands.ConversionError) and isinstance(e.original, KeyError):
            await ctx.send(f'`The following is not a PG: {e.original}`')

    # @commands.command(hidden=True)
    # @commands.is_owner()
    # async def test_gr_page(self, ctx):
    # 	text = (await self.bot.asession.get('http://www.golden-road.net/index.php?action=login')).html.html
    # 	await send_long_mes(ctx, text, fn='gr_front')


async def setup(bot):
    await bot.add_cog(PlayAlongCog(bot))

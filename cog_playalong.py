import asyncio
import calendar
import itertools
import logging
import random
import re
from collections import OrderedDict, defaultdict
from datetime import *

import orjson
from operator import ge, lt
import texttable
from discord import app_commands, DMChannel
from discord.ext import commands, tasks

from dropboxwayo import dropboxwayo
from pg import CURRENT_SEASON
from secretswayo import GR_PW
from util import (
    ARP_RANGE,
    MAX_MES_SIZE,
    POSITIVE_INT,
    NONNEGATIVE_INT,
    SCHEDULER_TZ,
    PGConverter,
    csspoints,
    find_nth,
    pg_autocomplete,
    send_long_mes,
)

_log = logging.getLogger('wayo_log')


async def gr_login(asession, cookie_length):
    # gr.py header
    hidden_inputs = (await asession.get('http://www.golden-road.net/index.php?action=login')).html.find(
        'form#guest_form > input[type=hidden]'
    )

    if not hidden_inputs:
        raise RuntimeError('G-R.net login unsuccessful, try again later.')

    session_var = hidden_inputs[-1].attrs['name']
    session_id = hidden_inputs[-1].attrs['value']

    # local_time = datetime.now()
    try:
        await asession.post(
            'http://www.golden-road.net/index.php?action=login2',
            data={
                'user': 'Wayoshi',
                'passwrd': GR_PW,
                'cookielength': cookie_length,
                'hash_passwrd': '',
                session_var: session_id,
            },
        )
    except:
        raise RuntimeError('G-R.net login unsuccessful, try again later.')

    return session_var, session_id


def et_offset(css_date):
    if 4 <= css_date.month <= 10:
        return 4
    elif css_date.month <= 2 or css_date.month == 12:
        return 5

    # https://stackoverflow.com/questions/28680896/how-can-i-get-the-3rd-friday-of-a-month-in-python
    c = calendar.Calendar(firstweekday=calendar.SUNDAY)
    monthcal = c.monthdatescalendar(css_date.year, css_date.month)

    # second Sunday of March, first Sunday of November
    crossover = [
        day for week in monthcal for day in week if day.weekday() == calendar.SUNDAY and day.month == css_date.month
    ][int(css_date.month == 3)]

    return 4 + (ge if css_date.month == 3 else lt)(css_date, crossover)


async def get_gr_css(asession):
    # gr.py get_pms infinitely
    p = list()
    for i in itertools.count(0):
        pm_html = (
            await asession.get('http://www.golden-road.net/index.php?action=pm;f=inbox;sort=date;desc;start=' + str(i * 15))
        ).html
        pms = pm_html.find('div.clear')
        for pm in pms:
            username = pm.find('h4 > a', first=True).text

            ts_str = re.search(
                r'((Today|Yesterday) at|\w+ \d{2}, \d{4},) \d{2}:\d{2}:\d{2} [AP]M',
                pm.find('span.smalltext', first=True).text,
            ).group()
            timestamp = datetime.strptime(
                ts_str,
                (
                    'Today at'
                    if ts_str.startswith('Today')
                    else ('Yesterday at' if ts_str.startswith('Yesterday') else '%B %d, %Y,')
                )
                + ' %I:%M:%S %p',
            )

            if ts_str.startswith('Today'):
                timestamp = datetime.combine(date.today(), timestamp.time())
            elif ts_str.startswith('Yesterday'):
                timestamp = datetime.combine(date.today() - timedelta(1), timestamp.time())

            message = pm.find('div.post > div.inner', first=True).text
            yield (username, timestamp, message)


def get_message_nums(message):
    return [int(re.sub(r'[^\d]', '', s)) for s in re.findall(r'\d[\d,]*', message)]


async def get_bids(bot, bid_log_channel, cssdate, csshour, cssmin, csssec):
    # 57/58 -> 52, 27/28 -> 22, etc.
    min_time = time(hour=csshour, minute=10 * (cssmin // 10) + 2)
    max_time = time(hour=csshour, minute=cssmin, second=csssec)

    min_plinko_time = time(hour=csshour, minute=4)
    max_plinko_time = time(hour=csshour, minute=42)

    css_discord_date = cssdate
    css_discord_hour = csshour + et_offset(cssdate)
    if css_discord_hour >= 24:
        css_discord_hour -= 24
        css_discord_date += timedelta(days=1)

    csv_dict = defaultdict(lambda: {'sc1': None, 'sc2': None, 'plinko': None})

    sucess_site = True

    try:
        # this is very circular/hacky, don't do this!
        await bot.get_cog('PlayAlong').login()

        async for u, ts, m in get_gr_css(bot.asession):
            if ts >= datetime.combine(cssdate, min_plinko_time):
                if min_time <= ts.time() <= max_time:
                    bd = [bid for bid in get_message_nums(m) if bid >= 10000]
                    if len(bd) >= 2:
                        csv_dict[u]['sc1'] = bd[0]
                        csv_dict[u]['sc2'] = bd[1]
                elif min_plinko_time <= ts.time() < max_plinko_time:
                    p = [bid for bid in get_message_nums(m)]
                    if len(bd) == 1 and u in csv_dict:
                        csv_dict[u]['plinko'] = p[0]
            else:
                break
    except Exception as e:
        _log.debug(e)
        success_site = False

    # oldest first with after provided
    async for message in bid_log_channel.history(
        after=datetime.combine(css_discord_date, time(hour=css_discord_hour, minute=4), tzinfo=UTC),
        before=datetime.combine(
            css_discord_date,
            time(hour=css_discord_hour, minute=58, second=10),
            tzinfo=UTC,
        ),
    ):
        # Wayoshi bid [31251, 28251] at 23:53:41
        if message.author == bot.user and (
            m := re.fullmatch(
                r'(.+?) bid \[(\d+)(?:, (\d{5,}))?\] at (\d{2}):(\d{2}):(\d{2})',
                message.content,
            )
        ):
            u, bid1, bid2, h, m, s = m.groups()

            if not bid2:
                csv_dict[u]['plinko'] = int(bid1)
            elif min_time <= time(int(h), int(m), int(s)) <= max_time:
                csv_dict[u]['sc1'] = int(bid1)
                csv_dict[u]['sc2'] = int(bid2)

    csv_dict = {k: v for k, v in csv_dict.items() if v['sc1']}

    return (csv_dict, max_time, success_site)


def bid_text(num):
    return '${:,d}'.format(num) if num >= 0 else '-${:,d}'.format(-num)


def construct_csslive_line(player, arp1, arp2, b1, b2, *extra):
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


class CSSLiveFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    hour: commands.Range[int, 0, 23] = 11
    minute: commands.Range[int, 0, 59] = commands.flag(aliases=['min'], default=56)
    second: commands.Range[int, 0, 59] = commands.flag(aliases=['sec'], default=30)
    date: str = None


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
        self.cookie_length = 15
        self.master = []

        self.cssbids = None
        self.bid_info = None
        self.cutoff_time = None
        self.success_site = None

        self.csslive.add_check(self.csslive_check)
        self.stop_live.add_check(self.csslive_check)

    @commands.Cog.listener()
    async def on_ready(self):
        self._csslive_channel = self.bot.get_channel(491321260819873807)
        self._bid_log_channel = self.bot.get_channel(1425691746318291006)
        await self._get_bidders()

    @commands.hybrid_group(aliases=['c'], case_insensitive=True)
    async def css(self, ctx):
        """Commands related to Chatroom Showcase Showoff, a G-R.net forums game based on bidding on showcases."""
        if not ctx.invoked_subcommand:
            await ctx.send('Invalid subcommand (see `help css`).')

    async def _get_bidders(self):
        self.bidders = {
            v: k
            for k, v in orjson.loads(
                await asyncio.to_thread(dropboxwayo.download, '/heroku/wayo-py/discord_bidders.json')
            ).items()
        }

    @css.command(hidden=True)
    @commands.is_owner()
    @commands.dm_only()
    async def get_bidders(self, ctx):
        await self._get_bidders()
        await ctx.send(', '.join(sorted(self.bidders.values(), key=lambda k: k.lower())))

    @css.command(name='calc', aliases=['c'])
    @app_commands.describe(
        classic='Default is False. If True, the DSW cut-off will be $100 (exclusive) instead of $250 (inclusive).'
    )
    async def csscalc(
        self,
        ctx,
        bid1: POSITIVE_INT,
        bid2: POSITIVE_INT,
        arp1: POSITIVE_INT,
        arp2: POSITIVE_INT,
        classic: bool = False,
    ):
        """Calculate the raw CSS score for the given bids and ARPs.

        The DSW/QSW/EXACTA bonuses are automatically applied if achieved; any other bonuses are left out.

        If classic is True, the DSW cut-off will be $100 (exclusive) instead of $250 (inclusive).
        """

        await ctx.send(
            '{}: `{:.2f}`'.format(
                ctx.author.mention,
                csspoints(bid1, bid2, arp1, arp2, dsw_diff=99 if classic else 250),
            )
        )

    @commands.Cog.listener()
    async def on_message(self, message):
        if isinstance(message.channel, DMChannel) and message.author.id in self.bidders.keys():
            dt = message.created_at.astimezone(SCHEDULER_TZ)
            if dt.weekday() < 5 and dt.hour in (11, 20, 21, 22):
                bids = get_message_nums(message.content)
                if len(bids) > 1:
                    bids = [b for b in bids if b >= 10000]
                if (len(bids) == 2 and 52 <= dt.minute <= 57) or (len(bids) == 1 and dt.hour == 11 and 4 <= dt.minute <= 41):
                    await message.channel.send(f'{bids} bid at {dt:%H:%M:%S}')
                    await self._bid_log_channel.send(f'{self.bidders[message.author.id]} bid {bids} at {dt:%H:%M:%S}')

    @css.command(hidden=True)
    @commands.is_owner()
    @commands.dm_only()
    async def get_bids(
        self,
        ctx,
        bid1: POSITIVE_INT,
        bid2: POSITIVE_INT,
        arp1: ARP_RANGE,
        arp2: ARP_RANGE,
        *,
        csstime: CSSLiveFlags,
    ):
        try:
            await self._get_bids(bid1, bid2, arp1, arp2, csstime=csstime)
        except Exception as e:
            await ctx.send(e)
        finally:
            await ctx.message.add_reaction('✅' if self.success_site else '⛔')

    async def _get_bids(
        self,
        bid1: POSITIVE_INT,
        bid2: POSITIVE_INT,
        arp1: ARP_RANGE,
        arp2: ARP_RANGE,
        csstime: CSSLiveFlags,
    ):
        now_utc = datetime.now(UTC)

        cssbids, self.cutoff_time, self.success_site = await get_bids(
            self.bot,
            self._bid_log_channel,
            (
                datetime.strptime(csstime.date, '%m/%d/%y').date()
                if csstime.date
                else (now_utc - timedelta(hours=et_offset(now_utc.date()))).date()
            ),
            csstime.hour,
            csstime.minute,
            csstime.second,
        )

        # del cssbids['tpirfan20251']

        if not cssbids:
            raise ValueError("No bids today... you sure it's a CSS day? If so, you sure it's the afternoon yet?")

        css = OrderedDict({'STAGE PLAYER': {'sc1': bid1, 'sc2': bid2}})
        css.update(cssbids)

        self.cssbids = css
        self.bid_info = (bid1, bid2, arp1, arp2, csstime)
        return self.cssbids, self.cutoff_time, self.success_site

    @css.command(name='live', aliases=['l'])
    @app_commands.guilds(314598609591074816)
    @app_commands.default_permissions(kick_members=True)
    @app_commands.guild_only()
    @app_commands.describe(
        hour='24-hour format. For primetime, most shows would thus be hour 20.',
    )
    @commands.max_concurrency(1)
    @commands.has_any_role('Moderator', 'Admin', 'Moderator-Alumnus')
    async def csslive(
        self,
        ctx,
        bid1: POSITIVE_INT,
        bid2: POSITIVE_INT,
        arp1: ARP_RANGE,
        arp2: ARP_RANGE,
        *,
        csstime: CSSLiveFlags,
    ):
        """Given Stagey's bids and ARPs, fetch today's bids and reveal CSS scores, "live". Moderator only.

        Minute/second cutoff is attempted to be adjusted by up to a few second(s) by comparing the bot's local time to the forum time.

        Hour is 24-hour format. For primetime, most shows would thus be 20 (8pm-9pm).

        Date is what it says on the tin, this should very rarely need to be used. mm/dd/yy format.
        """

        ttable = texttable.Texttable(max_width=0)
        ttable.header(['PLAYER', 'SC1 BID', 'SC1DIFF', 'SC2 BID', 'SC2DIFF', 'TOTDIFF', 'POINTS'])
        ttable.set_cols_align(['l', 'r', 'r', 'r', 'r', 'r', 'r'])
        ttable.set_cols_dtype(['t'] * 6 + ['f'])
        ttable.set_precision(2)

        async with ctx.typing():
            if not (self.cssbids and self.bid_info == tuple(ctx.args[2:]) and self.success_site):
                try:
                    r = await self._get_bids(bid1, bid2, arp1, arp2, csstime=csstime)
                    if not r:
                        return
                except Exception as e:
                    pass
            else:
                r = self.cssbids, self.cutoff_time, self.success_site

            cssbids, cutoff_time, success_site = r

            cssbid_items = list(cssbids.items())
            rows = [
                construct_csslive_line(player, arp1, arp2, *bids.values())
                for player, bids in ([cssbid_items[0]] + sorted(cssbid_items[1:], key=lambda r: random.random()))
            ]
            ttable.add_rows(rows, header=False)

            # await ctx.channel.purge(limit=None, before=ctx.message, check=lambda m : not m.pinned)
            extra_m0 = '\n\nDisclaimer: Could not get site bids.'
            m0 = await ctx.send(f"Today's cutoff time is `{cutoff_time:%H:%M:%S}`.{extra_m0}")

            raw_tdraw = ttable.draw().split('\n')
            split_index = (MAX_MES_SIZE - 6) // (len(raw_tdraw[0]) + 1)  # two ``` for code block bookends
            if not split_index % 2:
                split_index -= 1
            # 0 to 25, 24 to 49, etc. (include borders twice in-between)
            tdraw = [
                '\n' + ('\n'.join(raw_tdraw[i - j : i + split_index - j]))
                for j, i in enumerate(range(0, len(raw_tdraw) + 1, split_index))
            ]

            # edge case: exactly x players can lead to a border-only message at the end, remove it
            # this is probably when roughly len(raw_tdraw) // split_index is a multiple of 25, but this works just as well
            if tdraw[-1].count('\n') <= 1:
                del tdraw[-1]

            # _log.debug(tdraw)

            start = [find_nth(tdraw[0], '\n|', 3) + 1] + [tdraw[i].index('|') for i in range(1, len(tdraw))]
            max_player_len = max(len(p) for p in cssbids.keys())
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

        if self.stop_csslive:
            await m0.delete()

        async with self.lock:
            self.stop_csslive = False

    @css.command(name='post', with_app_command=False)
    @commands.max_concurrency(1)
    @commands.is_owner()
    @commands.dm_only()
    async def css_post(self, ctx):
        if self.cssbids:
            ttable_post = texttable.Texttable(max_width=0)
            ttable_post.header(
                [
                    'PLAYER',
                    'SC1 BID',
                    'SC1DIFF',
                    'SC2 BID',
                    'SC2DIFF',
                    'TOTDIFF',
                    'POINTS',
                ]
            )
            ttable_post.set_cols_align(['l', 'r', 'r', 'r', 'r', 'r', 'r'])
            ttable_post.set_cols_dtype(['t'] * 6 + ['f'])
            ttable_post.set_precision(2)

            rows = [
                construct_csslive_line(player, *self.bid_info[2:4], *bids.values()) for player, bids in self.cssbids.items()
            ]

            # points descending then case-insensitive name
            ttable_post.add_rows(sorted(rows, key=lambda r: (-r[-1], r[0].lower())), header=False)

            r = None
            try:
                await self.login()
                r = await self.bot.asession.post(
                    'http://www.golden-road.net/index.php?action=post2;start=0;board=13',
                    data={
                        'subject': f'CSS Raw Results for {date.today():%m/%d/%Y}',
                        'message': f'[code]{ttable_post.draw()}[/code]',
                        self.session_var: self.session_id,
                    },
                )
                await ctx.message.add_reaction('✅')
            except Exception as e:
                await ctx.send(e)
            finally:
                if r:
                    await ctx.send(r.content)
        else:
            await ctx.send('`No bids found, run "css live" (or if Wayoshi, "css get_bids").`')

    @css.command(name='download', with_app_command=False, hidden=True)
    @commands.max_concurrency(1)
    @commands.is_owner()
    async def css_download(
        self,
        ctx,
        plinko_amount: int = -1,
        plinko_chips: commands.Range[int, 1, 5] = 5,
        primetime: bool = False,
    ):
        if self.cssbids:
            plinko_played = plinko_amount >= 0

            file_str = 'username,sc1,sc2'
            if plinko_played:
                file_str += ',plinko'

            file_str += (
                f'\nSTAGE PLAYER,{self.bid_info[0]},{self.bid_info[1]}\n**ARP**,{self.bid_info[2]},{self.bid_info[3]}'
            )
            if plinko_played:
                file_str += f',{plinko_amount}/{plinko_chips}'
            file_str += '\n'

            cb = dict(self.cssbids)
            del cb['STAGE PLAYER']

            file_str += '\n'.join(
                u + ',' + (','.join(str(bb) for bb in list(b.values())[: 2 + int(plinko_played)] if len(b) >= 2))
                for u, b in cb.items()
            )
            file_str += '\n'

            cssdate = (
                datetime.strptime(self.bid_info[-1].date, '%m/%d/%y').date() if self.bid_info[-1].date else date.today()
            )

            fn = f'/gr/css/s{CURRENT_SEASON - 32}/{cssdate:%y.%m.%d}.{"p" if primetime else ""}b.csv'

            await asyncio.to_thread(dropboxwayo.upload, file_str.encode('utf-8'), fn)
            await ctx.message.add_reaction('✅')
        else:
            await ctx.send('`No bids found, run "css live" (or if Wayoshi, "css get_bids").`')

    @tasks.loop(time=time(5))
    async def _reset_bids(self):
        self.cssbids = None
        self.bid_info = None
        self.cutoff_time = None
        self.success_site = None

    @css.command(hidden=True)
    @commands.is_owner()
    @commands.dm_only()
    async def reset_bids(self, ctx):
        await self._reset_bids()
        await ctx.message.add_reaction('✅')

    @css.command(name='stop', with_app_command=False)
    @commands.has_any_role('Moderator', 'Admin', 'Moderator-Alumnus')
    async def stop_live(self, ctx):
        """Stop a running CSS live, in case of a mistake. Text-only command.

        This command will only have an effect when "css live" is running. Moderator permissions required.
        """
        async with self.lock:
            self.stop_csslive = True
        await ctx.send('Stopping CSS Live... get the parameter order right this time!')

    @css.command()
    async def info(
        self,
        ctx,
        theme1: str.upper,
        placard1: str.upper,
        theme2: str.upper,
        placard2: str.upper,
    ):
        """Add the info of today's SCs to Wayoshi's Dropbox text file. Thanks for helping him!"""
        await ctx.message.add_reaction('🚧')

        txt = ''
        for arg in ctx.args[2:5]:
            txt += arg + ('\t' * max(1, 5 - len(arg) // 4))
        txt += ctx.args[-1]

        res = await asyncio.to_thread(
            dropboxwayo.update_str,
            date.today().strftime('%m/%d/%y') + '\t' + txt + '\n',
            f'/gr/css/sc_info_{CURRENT_SEASON - 32}.txt',
            False,
        )

        await ctx.message.remove_reaction('🚧', ctx.bot.user)
        await ctx.message.add_reaction('❌' if 'failed' in res else '✅')

    async def csslive_check(self, ctx):
        return ctx.channel == self._csslive_channel

    async def login(self):
        if not self.logged_in:
            self.session_var, self.session_id = await gr_login(self.bot.asession, self.cookie_length)
            self.logged_in = True
            self.bot.SCHEDULER.add_job(
                self.reset_login,
                'date',
                run_date=datetime.now(tz=SCHEDULER_TZ) + timedelta(minutes=self.cookie_length),
            )

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
        confidence='Exactly 6 unique values of 1-6, except two 1s allowed in the case of spoilers.',
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
                    fpg_score(
                        [(pg, r, int(c)) for pg, r, c in zip(ctx.args[2:8], results, confidence)],
                        self.master,
                    ),
                )
            )

    async def cog_command_error(self, ctx, e):
        if isinstance(e, (commands.BadArgument, commands.RangeError, RuntimeError)):
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

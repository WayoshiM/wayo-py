import asyncio
import re
from datetime import *
from typing import Literal

import orjson
import pytz
from operator import itemgetter
from dateutil.parser import isoparse
from discord import app_commands
from discord.ext import commands

from util import tz_autocomplete, send_long_mes, NONNEGATIVE_INT, POSITIVE_INT

_CHANNEL_MAPPING = {'barker': 1025, 'tbe': 1025, '1025': 1025, 'carey': 1024, '1024': 1024, 'drew': 1024, 'bob': 1025}


def cm_str(s):
    return _CHANNEL_MAPPING.get(s.lower())


class ScheduleFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    timezone: str = commands.flag(aliases=['tz'], default='US/Eastern')


class PlutoScheduleFlags(ScheduleFlags):
    channel: cm_str = commands.flag(aliases=['c', 'ch'], default='barker')


class PlexScheduleFlags(ScheduleFlags):
    limit: NONNEGATIVE_INT = commands.flag(aliases=['l'], default=15)
    offset: POSITIVE_INT = commands.flag(aliases=['o'], default=0)


class PlutoCog(commands.Cog, name='TVChannels'):
    """Commands related to TV channel listings."""

    def __init__(self, bot):
        self.bot = bot
        self._pluto_api_url_template = 'http://api.pluto.tv/v2/channels?start={}&stop={}'
        self._roku_url = r'https://therokuchannel.roku.com/api/v2/homescreen/content/https%3A%2F%2Fcontent{}.sr.roku.com%2Fcontent%2Fv1%2Froku-trc%2F4ad47ace83b25065955adca3f8e9bdcf%3FfeatureInclude%3DlinearSchedule'
        self._samsung_url = r'https://raw.githubusercontent.com/matthuisman/i.mjh.nz/master/SamsungTVPlus/us.xml'
        self._plex_url = (
            'https://epg.provider.plex.tv/grid/bulk?channelGridKey=63e63d26abacad42ed3d1acd&startDate={}&endDate={}'
        )

    async def _get_pluto_sched(self, channel: int, tz: pytz.timezone):
        now = datetime.now(tz=tz)
        url = self._pluto_api_url_template.format(
            now.isoformat(timespec='milliseconds'), (now + timedelta(hours=12)).isoformat(timespec='milliseconds')
        ).replace('+', '%2B')

        r = await self.bot.session.get(url)
        j = await r.json(loads=orjson.loads)

        for jj in j:
            if jj['number'] == channel:
                return jj['timelines']

        return None

    async def _get_roku_sched(self):
        for s in ('', '-int-us-west-2'):
            try:
                # any -0x:00 works, timezone conversion done below regardless
                r = await self.bot.session.get(
                    self._roku_url.format(s), headers={'x-roku-reserved-time-zone-offset': '-04:00'}
                )
                j = await r.json(loads=orjson.loads)
                break
            except:
                pass

        try:
            return j['features']['linearSchedule']
        except:
            return None

    async def _get_samsung_sched(self):
        r = await self.bot.asession.get(self._samsung_url)
        try:
            return [
                (h.attrs['start'], h.find('sub-title', first=True).text)
                for h in r.html.find('programme[channel="USBC4400017KZ"]')
            ]
        except:
            return None

    async def _get_plex_sched(self):
        r = await self.bot.asession.get(
            self._plex_url.format(date.today().strftime('%Y-%m-%d'), (date.today() + timedelta(7)).strftime('%Y-%m-%d')),
            headers={'Accept': 'application/json', 'X-Plex-Provider-Version': '6.5.0'},
        )
        try:
            # print(r.content.decode('utf-16'))
            j = r.json()
            return sorted(
                [
                    (jj['index'], datetime.fromtimestamp(jj['Media'][0]['beginsAt']))
                    for jj in j['MediaContainer']['Metadata']
                    if 'index' in jj and 'Media' in jj
                ],
                key=itemgetter(1),
            )
        except:
            return None

    @commands.hybrid_command(aliases=['plexsched', 'plex_schedule', 'plex_sched', 'ps'])
    @app_commands.describe(
        timezone='Time zone to render schedule in. Default US/Eastern. Any standard tz database value can be used.',
    )
    @app_commands.autocomplete(timezone=tz_autocomplete)
    async def plexschedule(self, ctx, *, options: PlexScheduleFlags):
        """Fetches listings for Plex's Barker channel, in the given time zone.

        "limit" and "offset" control where the output starts and how long it is - this schedule goes up to a week for hundreds of episodes!
        """

        async with ctx.typing():
            tz = pytz.timezone(options.timezone)
            listings = await self._get_plex_sched()

        if listings:
            ss = []
            idx = 0
            for e, l in enumerate(listings):
                _, dt = l
                if dt >= datetime.now():
                    idx = max(0, e - 2)
                    break
            for l in listings[idx + options.offset :]:
                prod, dt = l
                ss.append(dt.astimezone(tz).strftime('%b %d, %I:%M:%S%p') + f" - {prod:04d}D")
            sss = [f'`{s}`' for e, s in enumerate(ss) if options.offset < e < options.limit + options.offset]
            if sss:
                ss = '\n'.join(sss)
                if len(sss) <= 20:
                    await ctx.send(f'>>> {ss}')
                else:
                    await send_long_mes(ctx, ss.replace("`", ""))
            else:
                await ctx.send('`No results found within those limit and/or offset arguments.`')
        else:
            await ctx.send("`Couldn't find listings.`")

    # @commands.hybrid_command(aliases=['plutosched', 'pluto_schedule', 'pluto_sched', 'ps'])
    # @app_commands.describe(
    #     timezone='Time zone to render schedule in. Default US/Eastern. Any standard tz database value can be used.',
    # )
    # @app_commands.autocomplete(timezone=tz_autocomplete)
    # async def plutoschedule(self, ctx, *, options: PlutoScheduleFlags):
    #     """Fetches listings for Pluto's Barker or Carey channel, listed in the given time zone."""

    #     cn = options.channel

    #     if not cn:
    #         await ctx.send(f'`Unsupported channel for this command. Must be one of {", ".join(_CHANNEL_MAPPING.keys())}.`')
    #         return

    #     async with ctx.typing():
    #         tz = pytz.timezone(options.timezone)
    #         listings = await self._get_pluto_sched(cn, tz)

    #     if listings:
    #         if cn == 1025:
    #             ss = [
    #                 isoparse(jjj['start']).astimezone(tz).strftime('%I%p')
    #                 + ': '
    #                 + (
    #                     m.group(1) + 'D'
    #                     if (m := re.search(r'\(S\d{2}E(\d+)\)', jjj['episode']['description']))
    #                     else jjj['episode']['description'][:10]
    #                 )
    #                 + f" (Pluto #{jjj['episode']['number']:03d})"
    #                 for jjj in listings
    #             ]
    #         else:
    #             ss = [
    #                 isoparse(jjj['start']).astimezone(tz).strftime('%I:%M%p')
    #                 + ': '
    #                 + isoparse(jjj['episode']['clip']['originalReleaseDate']).strftime('%b %d, %Y')
    #                 for jjj in listings
    #             ]
    #         ss = '\n'.join('`' + (scs[1:] if scs[0] == '0' else scs) + '`' for scs in ss)
    #         await ctx.send(f'>>> {ss}')
    #     else:
    #         await ctx.send("`Couldn't find channel in listings.`")

    @commands.hybrid_command(aliases=['rokusched', 'roku_schedule', 'roku_sched', 'rs'])
    @app_commands.describe(
        timezone='Time zone to render schedule in. Default US/Eastern. Any standard tz database value can be used.',
    )
    @app_commands.autocomplete(timezone=tz_autocomplete)
    async def rokuschedule(self, ctx, *, options: ScheduleFlags):
        """Fetches listings for Roku's Barker channel for the next 3-4 hours, listed in the given time zone.

        Valid time zones can be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"""
        async with ctx.typing():
            tz = pytz.timezone(options.timezone)
            listings = await self._get_roku_sched()

        if listings:
            ss = []
            for l in listings:
                num = int(l['content']['episodeNumber'])
                prod = '083DT' if num == 83 else f'{num:04d}D'
                ss.append(isoparse(l['date']).astimezone(tz).strftime('%I:%M%p') + f" - {prod}")
            ss = '\n'.join(f'`{s}`' for s in ss)
            await ctx.send(f'>>> {ss}')
        else:
            await ctx.send("`Couldn't find listings.`")

    @commands.hybrid_command(aliases=['samsungsched', 'samsung_schedule', 'samsung_sched', 'ss'])
    @app_commands.describe(
        timezone='Time zone to render schedule in. Default US/Eastern. Any standard tz database value can be used.',
    )
    @app_commands.autocomplete(timezone=tz_autocomplete)
    async def samsungschedule(self, ctx, *, options: ScheduleFlags):
        """Fetches listings for Samsung TV Plus's Barker channel, listed in the given time zone.

        Valid time zones can be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"""
        async with ctx.typing():
            tz = pytz.timezone(options.timezone)
            listings = await self._get_samsung_sched()

        if listings:
            ss = []
            for dt, title in listings:
                m = re.search(r'(\d+)$', title)
                num = int(m.group(1)) if m else '????'
                prod = '083DT' if num == 83 else f'{num:04d}D' if type(num) == int else num
                ss.append(datetime.strptime(dt, '%Y%m%d%H%M%S %z').astimezone(tz).strftime('%I:%M:%S%p') + f' - {prod}')
            ss = '\n'.join(f'`{s}`' for s in ss)
            await ctx.send(f'>>> {ss}')
        else:
            await ctx.send("`Couldn't find listings.`")

    async def cog_command_error(self, ctx, e):
        if isinstance(e, commands.BadArgument):
            if isinstance(e.__cause__, pytz.exceptions.UnknownTimeZoneError):
                await ctx.send('`Invalid time zone.`')
            elif isinstance(e.__cause__, commands.BadLiteralArgument):
                el = ', '.join(e.__cause__.literals)
                await ctx.send(f'Unsupported channel for this command. Supported channels are: `{el}`')
            else:
                await ctx.send(f'`{e}`')
        elif isinstance(e, commands.CommandError) and isinstance(e.__cause__, pytz.exceptions.UnknownTimeZoneError):
            await ctx.send('`Invalid time zone.`')
        else:
            await ctx.send(f'`{e}`')


async def setup(bot):
    p = PlutoCog(bot)
    await bot.add_cog(p)


if __name__ == '__main__':
    import requests

    now = datetime.now(tz=pytz.timezone('US/Eastern'))
    url = 'http://api.pluto.tv/v2/channels?start={}&stop={}'.format(
        now.isoformat(timespec='milliseconds'), (now + timedelta(hours=12)).isoformat(timespec='milliseconds')
    ).replace('+', '%2B')
    import pyperclip

    t = requests.get(url).text
    pyperclip.copy(url)
    j = orjson.loads(t)
    for jj in j:
        if jj['number'] == 1010:
            print(jj['timelines'])
            pyperclip.copy(str(jj['timelines']))
            break
    else:
        print('Channnel not found')

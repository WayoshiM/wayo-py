import asyncio
import re
from datetime import *
from typing import Literal

import orjson
import pytz
from dateutil.parser import isoparse
from discord import app_commands
from discord.ext import commands

from util import tz_autocomplete

_CHANNEL_MAPPING = {'barker': 1025, 'price': 1025, 'tbe': 1025, 'tpir': 1025, '1025': 1025}


class PSFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    timezone: str = commands.flag(aliases=['tz'], default='US/Eastern')


class PlutoCog(commands.Cog, name='PlutoRoku'):
    """Commands related to Pluto/Roku TV channels."""

    def __init__(self, bot):
        self.bot = bot
        self._pluto_api_url_template = 'http://api.pluto.tv/v2/channels?start={}&stop={}'
        self._roku_url = r'https://therokuchannel.roku.com/api/v2/homescreen/content/https%3A%2F%2Fcontent{}.sr.roku.com%2Fcontent%2Fv1%2Froku-trc%2F4ad47ace83b25065955adca3f8e9bdcf%3FfeatureInclude%3DlinearSchedule'

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

    @commands.hybrid_command(aliases=['plutosched', 'pluto_schedule', 'pluto_sched', 'ps'])
    @app_commands.describe(
        timezone='Time zone to render schedule in. Default US/Eastern. Any standard tz database value can be used.',
    )
    @app_commands.autocomplete(timezone=tz_autocomplete)
    async def plutoschedule(self, ctx, *, options: PSFlags):
        """Fetches listings for Pluto's Barker channel for the next 12-13 hours, listed in the given time zone.

        Valid time zones can be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"""

        # cn = _CHANNEL_MAPPING.get(options.channel)

        # if not cn:
        # await ctx.send('Unsupported channel for this command. See `!help plutoschedule` for supported channels.')
        # return

        async with ctx.typing():
            tz = pytz.timezone(options.timezone)
            listings = await self._get_pluto_sched(1025, tz)

        if listings:
            ss = [
                isoparse(jjj['start']).astimezone(tz).strftime('%I%p')
                + ': '
                + (
                    m.group(1) + 'D'
                    if (m := re.search(r'\(S\d{2}E(\d+)\)', jjj['episode']['description']))
                    else jjj['episode']['description'][:10]
                )
                + f" (Pluto #{jjj['episode']['number']:03d})"
                for jjj in listings
            ]
            ss = '\n'.join('`' + (scs[1:] if scs[0] == '0' else scs) + '`' for scs in ss)
            await ctx.send(f'>>> {ss}')
        else:
            await ctx.send("`Couldn't find channel in listings.`")

    @commands.hybrid_command(aliases=['rokusched', 'roku_schedule', 'roku_sched', 'rs'])
    @app_commands.describe(
        timezone='Time zone to render schedule in. Default US/Eastern. Any standard tz database value can be used.',
    )
    @app_commands.autocomplete(timezone=tz_autocomplete)
    async def rokuschedule(self, ctx, *, options: PSFlags):
        """Fetches listings for Roku's Barker channel for the next 3-4 hours, listed in the given time zone.

        Valid time zones can be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"""
        async with ctx.typing():
            tz = pytz.timezone(options.timezone)
            listings = await self._get_roku_sched()

        if listings:
            ss = []
            for l in listings:
                ss.append(isoparse(l['date']).astimezone(tz).strftime('%I:%M%p') + ' - ' + l['content']['title'][-4:] + 'D')
            ss = '\n'.join(f'`{s}`' for s in ss)
            await ctx.send(f'>>> {ss}')
        else:
            await ctx.send("`Couldn't find listings.`")

    async def cog_command_error(self, ctx, e):
        if isinstance(e, commands.BadArgument):
            if isinstance(e.__cause__, pytz.exceptions.UnknownTimeZoneError):
                await ctx.send('`Invalid time zone.`', ephemeral=True)
            elif isinstance(e.__cause__, commands.BadLiteralArgument):
                el = ', '.join(e.__cause__.literals)
                await ctx.send(f'Unsupported channel for this command. Supported channels are: `{el}`')
            else:
                await ctx.send(f'`{e}`', ephemeral=True)
        elif isinstance(e, commands.CommandError) and isinstance(e.__cause__, pytz.exceptions.UnknownTimeZoneError):
            await ctx.send('`Invalid time zone.`', ephemeral=True)
        else:
            await ctx.send(f'`{e}`', ephemeral=True)


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
    pyperclip.copy(t)
    j = orjson.loads(t)
    for jj in j:
        if jj['number'] == 1010:
            print(jj['timelines'])
            pyperclip.copy(str(jj['timelines']))
            break
    else:
        print('Channnel not found')

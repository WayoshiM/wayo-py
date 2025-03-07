# putty root@45.77.158.30 -pw ')Q2wdNM}g(vGTMG]'
# immortal -d wayo-py -l log.log ../wayo-py-python/bin/python wayo.py
# immortalctl to get PID name, then immortalctl stop PID, overwrite files, start PID
# can also immortalctl halt PID to remove task entirely
# wayo-py-python/bin/pip install -r wayo-py/requirements.txt --upgrade

import asyncio
import io
import itertools
import re
import signal
import sys
import logging
import traceback

import aiohttp
import discord

# import discord.opus
from discord.ext import commands

# try:
#     import uvloop

#     uvloop.install()
#     asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
# except ImportError:
#     pass
from datetime import *

import polars as pl
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from requests_html import AsyncHTMLSession

from dropboxwayo import dropboxwayo
from secretswayo import WAYO_PY_TOKEN, PASTEBIN_TOKEN, PASTEBIN_PW
from util import SCHEDULER_TZ

# https://discord.com/api/oauth2/authorize?client_id=280172398974730250&permissions=0&scope=applications.commands%20bot

DEBUG = 'DEBUG' in sys.argv
_log = logging.getLogger('wayo_log')


class WayoPyBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.all()
        super().__init__(
            command_prefix='$' if DEBUG else '!',
            case_insensitive=True,
            chunk_guilds_at_startup=False,
            intents=intents,
        )
        self.ready = False
        self.initial_extensions = (
            'cog_help',
            'cog_joke',
            'cog_tpir',
            'cog_playalong',
            'cog_game',
            'cog_clue',
            'cog_mod',
            'cog_background',
            'cog_pluto',
            'cog_compendium',
            'cog_lineup',
        )
        self.pastebin_key = None

    async def on_ready(self):
        if self.ready:
            return
        _log.info('Python {0}\ndiscord.py {1}'.format(sys.version_info, discord.version_info))
        self.ac = await self.application_info()
        # _log.info(self.ac)
        # self.gr_server = discord.utils.get(self.guilds, id=314598609591074816)
        # self.gr_member_role = self.gr_server.get_role(540585764275093513)

        self.owner = self.ac.owner

        # if sys.platform == 'linux':
        # try:
        # discord.opus.load_opus('.heroku/vendor/lib/libopus.so')
        # _log.info('voice loaded')
        # except Exception as e:
        # _log.info('voice not loaded: ' + str(e))
        # _log.info('voice not loaded: ' + str(e))

        self.dig_emoji = self.get_emoji(350882871743086592)
        self.x_emoji = self.get_emoji(894575889629736980)

        _log.info(f'fetched owner as {self.owner}, dig_emoji as {self.dig_emoji}, x_emoji as {self.x_emoji}')
        self.ready_time = datetime.now().astimezone(SCHEDULER_TZ)
        _log.info('Ready as {0} (ID {1}) at {2}'.format(self.user.name, self.user.id, self.ready_time))
        self.ready = True

    async def setup_hook(self):
        self.session = aiohttp.ClientSession()
        self.asession = AsyncHTMLSession()
        self.SCHEDULER = AsyncIOScheduler(timezone=SCHEDULER_TZ)
        self.SCHEDULER.start()

        if not DEBUG:
            # @self.scheduler.scheduled_job('date', run_date=datetime.now(tz=self.usetz) + dt)
            # @self.SCHEDULER.scheduled_job('cron', hour='5', minute='30')
            # async def timed_quit():
            #     try:
            #         await self.close()
            #     except:
            #         pass

            # _log.info('timed_quit set up')
            await self.login_pastebin()
        else:
            self.loop.set_debug(True)
            _log.info('Local debug is on instead of pastebin.')

        for ext in self.initial_extensions:
            await self.load_extension(ext)

        # build quick sub dict
        self.sub_dict = {}
        for c in self.walk_commands():
            if c.root_parent:
                self.sub_dict[c.name] = c.qualified_name
                for a in c.aliases:
                    self.sub_dict[a] = c.qualified_name

    async def login_pastebin(self):
        login_data = {
            'api_dev_key': PASTEBIN_TOKEN,
            'api_user_name': 'Wayoshi',
            'api_user_password': PASTEBIN_PW,
        }

        login = await self.session.post("https://pastebin.com/api/api_login.php", data=login_data)
        if login.status == 200:
            self.pastebin_key = await login.text()
            _log.info(f"Got pastebin token as: {self.pastebin_key}")
        else:
            self.pastebin_key = None
            _log.info(f"Failed to get pastebin token. login status was: {login.status}")

    async def do_pastebin(self, txt, fn):
        if self.pastebin_key:
            data = {
                'api_option': 'paste',
                'api_dev_key': PASTEBIN_TOKEN,
                'api_paste_code': txt,
                'api_paste_name': fn,
                'api_paste_private': 1,
                # 'api_paste_expire_date': '1H',
                'api_user_key': self.pastebin_key,
                'api_folder_key': 'pR23GEqX',
            }
            r = await self.session.post("https://pastebin.com/api/api_post.php", data=data)
            if r.status == 200:
                link = await r.text()
                slash_idx = link.rfind('/')
                return link[:slash_idx] + '/raw' + link[slash_idx:]
            else:
                return None
        else:
            return None

    async def on_command_error(self, ctx, error):
        if not issubclass(type(error), commands.CommandError):
            if hasattr(error, 'original'):
                original = error.original
                if not isinstance(original, AssertionError):
                    _log.error(f'In {ctx.command.qualified_name}: {original}')
                    traceback.print_tb(original.__traceback__)
            else:
                if ctx.command:
                    _log.error(f'In {ctx.command.qualified_name}: {error}')
                else:
                    _log.error(f'Non-command error: {error}')
                traceback.print_tb(error.__traceback__)

            if ctx.message:
                await self.owner.send(f'>>> Error from message {ctx.message.jump_url}\n`{error}`')
            else:
                await self.owner.send(f'>>> Error from not a message (ephemeral interaction?)\n`{error}`')
        else:
            if ctx.command:
                if DEBUG:
                    if hasattr(error, 'original'):
                        original = error.original
                        if not isinstance(original, AssertionError):
                            _log.error(f'In {ctx.command.qualified_name}: {original}')
                            traceback.print_tb(original.__traceback__)
                    else:
                        _log.error(error)
                        traceback.print_tb(error.__traceback__)
                else:
                    _log.info(f'Standard commands.CommandError in {ctx.command.qualified_name}: {error}')
            else:
                if isinstance(error, commands.CommandNotFound):
                    try:
                        com = re.match('Command "(.+?)" is not found', str(error)).group(1)
                        prefixes = await self.get_prefix(ctx.message)
                        if type(prefixes) is str:
                            prefixes = [prefixes]
                        if any(re.fullmatch(fr'\{p}+', com) for p in prefixes):
                            return
                        else:
                            q = self.sub_dict.get(com)
                            extra = f' Perhaps you meant `{q}`?' if q else ''
                    except:
                        extra = ''
                    await ctx.send(f'{error}.{extra}')
                else:
                    _log.info(f'\tStandard commands.CommandError, {error.__class__.__name__}: {error}', file=sys.stderr)

    async def close(self):
        try:
            await self.get_cog('Clue').cog_unload()
        except:
            pass
        await super().close()
        await self.session.close()
        await self.asession.close()
        self.SCHEDULER.shutdown(wait=False)
        for t in asyncio.all_tasks(self.loop):
            t.cancel()


WB = WayoPyBot()

# debug check
if DEBUG:

    @WB.check
    async def debug_check(ctx):
        return hasattr(WB, 'owner') and ctx.author == WB.owner


@WB.command(hidden=True)
async def ready_time(ctx):
    await ctx.send(f'{ctx.bot.ready_time:%m/%d/%y %I:%M:%S %p}')


# @WB.command(hidden=True)
# async def reset_time(ctx):
#     await ctx.send(
#         f'{ctx.bot.ready_time + timedelta(days=1):%m/%d/%y %I:%M:%S %p} to {ctx.bot.ready_time + timedelta(days=1, minutes=216):%m/%d/%y %I:%M:%S %p}'
#     )


@WB.command(hidden=True)
@commands.is_owner()
@commands.dm_only()
async def login_pastebin(ctx):
    await ctx.message.add_reaction('🚧')
    stat = await asyncio.to_thread(WB.login_pastebin)
    await ctx.message.remove_reaction('🚧', ctx.bot.user)
    await ctx.message.add_reaction('✅' if WB.pastebin_key else '❌')


@WB.command(hidden=True)
@commands.is_owner()
@commands.dm_only()
async def reload_ext(ctx, cog):
    try:
        await ctx.bot.reload_extension(f'cog_{cog}')
        await ctx.message.add_reaction('✅')
    except commands.ExtensionError as e:
        await ctx.send('`{e}`')


import random

_PATTERNS = ["✉️", '✅', "🔷"]


@WB.command(aliases=['cp', 'cgp', 'greco_pattern'], hidden=True)
@commands.is_owner()
async def choose_pattern(ctx):
    await ctx.send(random.choice(_PATTERNS))


# https://gist.github.com/AbstractUmbra/a9c188797ae194e592efe05fa129c57f?permalink_comment_id=4121434#gistcomment-4121434
from typing import Literal, Optional


@WB.command(hidden=True)
@commands.is_owner()
@commands.dm_only()
async def sync(ctx, guilds: commands.Greedy[commands.GuildConverter], spec: Optional[Literal["~", "~~"]] = None) -> None:
    if not guilds:
        if spec:
            if spec == '~~':
                ctx.bot.tree.copy_global_to(guild=ctx.guild)
            fmt = await ctx.bot.tree.sync(guild=ctx.guild)
        else:
            fmt = await ctx.bot.tree.sync()

        await ctx.send(
            f"Synced commands in the tree {'globally' if spec is None else 'to the current guild' + (' (global copied)' if spec == '~~' else '') + '.'}"
        )
        return

    assert guilds is not None
    # fmt = 0
    for guild in guilds:
        try:
            fmt = await ctx.bot.tree.sync(guild=guild)
            await ctx.send(f"Synced commands in the tree to {guild.name}.")
        except discord.HTTPException:
            await ctx.send(f"Failed to sync tree to {guild.name}.")
            # pass
        # else:
        # fmt += 1

    # await ctx.send(f"Synced the tree to {fmt}/{len(guilds)} guilds.")


if __name__ == '__main__':
    pl.enable_string_cache()

    (
        pl.Config.set_tbl_cols(-1)
        .set_tbl_rows(-1)
        .set_tbl_width_chars(1_000)
        .set_fmt_str_lengths(100)
        .set_tbl_formatting('NOTHING')
        .set_tbl_cell_alignment('RIGHT')
        .set_tbl_hide_dataframe_shape()
        .set_tbl_hide_column_data_types()
        .set_tbl_hide_dtype_separator()
    )

    # if sys.platform.startswith('linux') and datetime.now(tz=pytz.timezone('US/Eastern')).weekday() > 4:
    # quit()

    # https://stackoverflow.com/questions/27981545/suppress-insecurerequestwarning-unverified-https-request-is-being-made-in-pytho
    import urllib3, bs4

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    import warnings

    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    warnings.filterwarnings("ignore", category=bs4.XMLParsedAsHTMLWarning)

    # logging
    # logger = logging.getLogger('wayo_log')
    lh = logging.StreamHandler(sys.stdout)
    _log.addHandler(lh)  # let discord library do log_formatter (commented out below)
    # dt_fmt = '%Y-%m-%d %H:%M:%S'
    # log_formatter = logging.Formatter('[{asctime}] [{levelname:<8}] {name} ({filename}, {lineno}): {message}', dt_fmt, style='{')
    # lh.setFormatter(log_formatter)
    _log.setLevel(logging.DEBUG if DEBUG else logging.INFO)

    # discord.py official 2.0 way to start a bot (cog loading abstracted to in-class)
    WB.run(
        WAYO_PY_TOKEN,
        log_handler=lh,
        log_level=logging.INFO,
    )  # 204774679582343168

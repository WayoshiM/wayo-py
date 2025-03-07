from collections import deque
from inspect import getmro
from types import SimpleNamespace

import discord
from discord.ext import commands

from cog_lineup import PLAYING_FLAGS_SINGLE

_FLAGS_OPTION_HELP = f"""When a command has "pgFlags" as an option, the syntax for accepted values is:
-`slots`
--Any combination of the single characters `{''.join(PLAYING_FLAGS_SINGLE)}`. `0` corresponds to a playing with no flags. See `!flags` for explanations on the rest of the characters.
--`any` can be specified instead as shorthand for every character above EXCEPT `0`.
-`concurrence`, `concurrenceN`
--Since these commands (can) have multiple `pg` arguments, refer to which PG you want to "attach" flags to with `#:f`. `#` corresponds to the exact `pg#` argument given to the command, e.g. `pg1` for the first PG given, etc., then `f` is simply a listing of flags just like `slots`.
--You can thus specify pgFlags multiple times, up to the number of pg arguments given.
"""


class WayoHelpCommand(commands.DefaultHelpCommand):
    def __init__(self):
        super().__init__(
            width=100,
            no_category='Misc',
            dm_help=False,
            command_attrs={
                'aliases': ['h']
                # 'cooldown': commands.CooldownMapping.from_cooldown(2, 5.0, commands.BucketType.user)
            },
            show_parameter_descriptions=False,
        )
        self.paginator.prefix = '```'
        self.paginator.suffix = '```'
        self.ultra_hidden_cogs = ['Background']

    # https://gist.github.com/InterStella0/b78488fb28cadf279dfd3164b9f0cf96
    # async def send_pages(self):
    #     destination = self.get_destination()
    #     for page in self.paginator.pages:
    #         emby = discord.Embed(description=page)
    #         await destination.send(embed=emby)

    def get_command_signature(self, command):
        orig = super().get_command_signature(command)
        # print(orig)
        # for p in command.parents:
        #     if p.aliases:
        #         orig = orig.replace(p.name, '(' + '|'.join([p.name] + p.aliases) + ')')
        # if command.aliases:
        #     orig = orig.replace(command.name, '(' + '|'.join([command.name] + command.aliases) + ')')
        for p, a in command.clean_params.items():
            if isinstance(a.annotation, type(commands.flags.FlagConverter)):
                hierarchy = filter(
                    lambda c: issubclass(c, commands.flags.FlagConverter) and c.get_flags(),
                    reversed(getmro(a.annotation)),
                )
                add = deque()
                processed = set()
                for h in hierarchy:
                    sub_a = []
                    for s, f in h.get_flags().items():
                        if s not in processed:
                            processed.add(s)
                            if f.aliases:
                                s = '[' + '|'.join([s] + f.aliases) + ']'
                            if callable(f.default):
                                fd_str = '=' + str(f.default(SimpleNamespace(command=command)))  # dummy object
                            else:
                                fd_str = f'={f.default}' if f.default is not None else ''
                            sub_a.append(f'({s}{fd_str})')
                    add.appendleft(sub_a)
                orig = orig.replace(f'<{p}>', '\n  ' + ('\n  '.join([' '.join(sub_a) for sub_a in add])))
        return orig

    # completely hide Background (can be extended to any command/cog sets)
    async def send_command_help(self, command):
        if command.cog and command.cog.qualified_name in self.ultra_hidden_cogs:
            await self.send_error_message(self.command_not_found(command.name))
        else:
            await super().send_command_help(command)
            # title = '!' + ' '.join([p.name for p in command.parents] + [command.name])
            # embed = discord.Embed(title=title, description=command.help or '???')
            # embed.add_field(name="Signature", value=self.get_command_signature(command))

            # channel = self.get_destination()
            # await channel.send(embed=embed)

    # def add_aliases_formatting(self, aliases):
    #     pass

    # def commands_heading(self, aliases):
    #     return 'C'

    # def add_subcommand_formatting(self, subcommand):
    #     self.paginator.add_line(f'  {subcommand.name} – {subcommand.short_doc}')

    # def get_opening_note(self):
    #     return ''

    def get_ending_note(self):
        return ''

    async def send_cog_help(self, cog):
        if cog.qualified_name in self.ultra_hidden_cogs:
            await self.send_error_message(self.command_not_found(cog.qualified_name))
        else:
            await super().send_cog_help(cog)


class HelpCog(commands.Cog, name='Help'):
    """Commands giving help on this bot."""

    def __init__(self, bot):
        self._original_help_command = bot.help_command
        bot.help_command = WayoHelpCommand()
        bot.help_command.cog = self
        self.bot = bot

    def cog_unload(self):
        self.bot.help_command = self._original_help_command

    @commands.command(aliases=['paste', 'pb'])
    async def faq(self, ctx):
        """Gives links to detailed documentation on commands in the TPIR/PG, Lineup, and Wheel sections of the bot."""
        await ctx.send(
            '>>> Price: <https://github.com/WayoshiM/wayo-py/blob/master/faq_price.md>\nWheel: <https://github.com/WayoshiM/wayo-py/blob/master/faq_wheel.md>'
        )

    @commands.command(aliases=['fh'])
    async def flagshelp(self, ctx):
        """Prints a static message with explanations on the "pgFlags" option in the commands of the Lineup section of the bot."""
        await ctx.send('>>> ' + _FLAGS_OPTION_HELP)


async def setup(bot):
    await bot.add_cog(HelpCog(bot))

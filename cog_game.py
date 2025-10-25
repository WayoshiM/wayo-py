import asyncio
import logging
import random
import pickle
from collections import Counter, deque

import discord
import discord.ui as dui
from discord import ButtonStyle
from discord.ext import commands

from dropboxwayo import dropboxwayo
from datetime import datetime, timedelta

from util import CancelButton, SCHEDULER_TZ

_log = logging.getLogger('wayo_log')

COLORS = {
    # 'black' : '⚫',
    'blue': '🔵',
    'brown': '🟤',
    'green': '🟢',
    'orange': '🟠',
    'purple': '🟣',
    'red': '🔴',
    'white': '⚪',
    'yellow': '🟡',
}

# Full Invisible Character: \u2800
# Zero Width Character: \u200b & \uFEFF


class MastermindButton(dui.Button):
    def __init__(self, code_idx, sample_space, row):
        super().__init__(emoji=COLORS[sample_space[0]], row=row)
        self.sample_space = sample_space
        self.code_idx = code_idx

    async def callback(self, interaction):
        self.sample_space.rotate(-1)
        self.emoji = COLORS[self.sample_space[0]]
        self.view.update_guess(self)
        await interaction.response.edit_message(content=self.view.gen_content(False), view=self.view)


class MastermindView(dui.View):
    def __init__(self, code_length, color_count, duplicates, tries, player_id, singlePlayer):
        super().__init__(timeout=900.0)

        self.player_id = player_id
        self.sample_space = random.sample(COLORS.keys(), k=color_count)
        self.duplicates = duplicates
        self.answer = (random.choices if duplicates else random.sample)(self.sample_space, k=code_length)
        _log.debug(self.answer)
        self.code_length = code_length
        self.tries = tries

        self.guess_buttons = [MastermindButton(e, deque(self.sample_space, color_count), e // 4) for e in range(code_length)]
        for s in self.guess_buttons:
            self.add_item(s)

        self.guess_idx = tries - 1
        self.guesses = [
            [
                [self.sample_space[0] if t == self.guess_idx else None] * code_length,
                None,
            ]
            for t in range(tries)
        ]

        self.cancel = CancelButton(row=2, authority_user_id=self.player_id)
        self.add_item(self.cancel)
        self.singlePlayer = singlePlayer

    @dui.button(style=ButtonStyle.primary, label='Guess', emoji='❔', row=2)
    async def guess(self, interaction, button):
        guess = self.guesses[self.guess_idx][0]

        if self.answer == guess:
            self.guesses[self.guess_idx][1] = (len(guess), 0)
            await interaction.response.edit_message(content=self.gen_content(True) + '\n\nSuccess!', view=None)
            self.stop()
        else:
            hit_idxs = {j for j in range(self.code_length) if self.answer[j] == guess[j]}

            answer_blows = [a for e, a in enumerate(self.answer) if e not in hit_idxs]
            guess_blows = [a for e, a in enumerate(guess) if e not in hit_idxs]
            blows = Counter(answer_blows) & Counter(guess_blows)

            self.guesses[self.guess_idx][1] = (len(hit_idxs), sum(blows.values()))
            if self.guess_idx:
                self.guess_idx -= 1
                self.guesses[self.guess_idx][0] = list(self.guesses[self.guess_idx + 1][0])
                await interaction.response.edit_message(content=self.gen_content(False), view=self)
            else:
                await interaction.response.edit_message(content=self.gen_content(True) + '\n\nNot this time.', view=None)
                self.stop()

    def update_guess(self, guess_button):
        self.guesses[self.guess_idx][0][guess_button.code_idx] = guess_button.sample_space[0]

    def gen_content(self, showAnswer):
        return (
            '>>> '
            + ('\u2800'.join([COLORS[c] for c in self.answer] if showAnswer else ['❔'] * self.code_length))
            + '\u2800│\u2800MASTER\n'
            + '\n'.join(
                [
                    '\u2800'.join(COLORS[c] if c else '\u200b' for c in g) + (f'\u2800│\u2800{h}' if h else '')
                    for g, h in self.guesses
                ]
            )
            + '\n\nDuplicates: '
            + ('yes' if self.duplicates else 'no')
            + '\nColors: '
            + '\u2800'.join(COLORS[c] for c in self.sample_space)
        )  # + '\nPlayer{}: '.format('s' if len(self.player_names) > 1 else '') + ', '.join(self.player_names)

    async def interaction_check(self, interaction):
        return interaction.user.id == self.player_id if self.singlePlayer else True


class MastermindFlags(commands.FlagConverter, delimiter='=', case_insensitive=True):
    length: commands.Range[int, 4, 8] = 4
    colors: commands.Range[int, 4, 8] = 4
    tries: commands.Range[int, 4, 16] = 8
    duplicates: bool = commands.flag(aliases=['dup'], default=True)
    singlePlayer: bool = commands.flag(name='single_player', aliases=['single'], default=False)


class GameCog(commands.Cog, name='Game'):
    """Commands related to multiplayer games wayo.py can host. One active game per channel."""

    def __init__(self, bot):
        self.bot = bot
        self.active_games = {}

    # async def cog_load(self):
    #     try:
    #         self.active_games = pickle.loads(await asyncio.to_thread(dropboxwayo.download, f'/heroku/wayo-py/active_games.pickle'))
    #         _log.info(f'{len(self.active_games)} games loaded.')
    #     except:
    #         pass

    # async def cog_unload(self):
    #     await asyncio.to_thread(dropboxwayo.upload, pickle.dumps(self.active_games), f'/heroku/wayo-py/active_games.pickle')

    # async def cog_before_invoke(self, ctx):
    #     if (datetime.now(SCHEDULER_TZ) - ctx.bot.ready_time).seconds >= 79200:
    #         await ctx.send(
    #             "WARNING: The time for me to \"cycle\" and reset is coming. Use `!reset_time` to get the possible time range I'll reset."
    #         )

    @commands.hybrid_group(aliases=['g'], case_insensitive=True)
    async def game(self, ctx):
        """Boot up a game and begins allowing players to join. Subcommand must be valid."""
        if not ctx.invoked_subcommand:
            await ctx.send('Invalid game specified.')
        elif ctx.channel.id in self.active_games:
            msg = 'Already an active game on this channel, host user can cancel it with "!cancelgame" before creating another one.'
            await ctx.send(msg)
            raise commands.DisabledCommand(msg)

    @commands.command()
    async def cancelgame(self, ctx):
        """Prematurely end an active game. Only person who used !game can use."""
        if a := self.active_games.get(ctx.channel.id):
            if a['owner'] == ctx.author.id:
                del self.bot.get_cog(a['cog_name']).active_games[ctx.channel.id]
                del self.active_games[ctx.channel.id]
                # await self.disconnect_voice()
                await ctx.send('Game cancelled. :white_check_mark:')
            else:
                await ctx.send("You didn't start this game!")
        else:
            await ctx.send('No active game on this channel.')

    @game.command(with_app_command=False)
    async def clue(self, ctx, *options):
        """A game of classic Clue. 3-6 players supported, DM access to wayo.py required."""
        if not (isinstance(ctx.channel, discord.TextChannel) or isinstance(ctx.channel, discord.GroupChannel)):
            ctx.send('This is a multiplayer game.')
            return

        cog = self.bot.get_cog('Clue')
        if await cog.register(ctx, options):
            self.active_games[ctx.channel.id] = {
                'owner': ctx.author.id,
                'cog_name': 'Clue',
            }

    @game.command(with_app_command=False)
    async def masterclue(self, ctx, *options):
        """A game of Clue Master Detective. 3-10 players supported, DM access to wayo.py required."""
        if not (isinstance(ctx.channel, discord.TextChannel) or isinstance(ctx.channel, discord.GroupChannel)):
            ctx.send('This is a multiplayer game.')
            return

        cog = self.bot.get_cog('Clue')
        if await cog.register(ctx, options, master=True):
            self.active_games[ctx.channel.id] = {
                'owner': ctx.author.id,
                'cog_name': 'Clue',
            }

    def naturalendgame(self, ctx):
        del self.active_games[ctx.channel.id]

    @game.command(aliases=['mm'])
    async def mastermind(self, ctx, *, options: MastermindFlags):
        """Produces a message & button set to play Mastermind.

        You guess a combination of colors with the color-coded buttons and get a pair of numbers as a hint when submitting a guess: how many are correct and in the right position ("hits"), and how many are correct but in the wrong position ("blows").

        The code "length" must be between 4 and 8 (8 is plenty, trust me).
        The amount of "colors" that could be used in the code must be between 4 and 8, and at least the code length.
        The number of attempts is set with "tries", from 4 to 16.
        If duplicates is off, a color appears at most once in the code.

        There are no restrictions on who can press the buttons by default, letting you collaborate with others directly. if you truly want single player, use single=True or play in a DM. (Note the cancel button will always be restricted to the game starter).

        There is a 15 minute timeout; the answer will be shown if you time out, but not if you cancel. Note the bot resets once a day in the early morning (US eastern).
        """
        # assert 4 <= options.length <= 8, 'Code length must be between 4 and 8.'
        # assert 4 <= options.colors <= 8, 'Color space must be between 4 and 8.'
        if not options.duplicates:
            assert (
                options.length <= options.colors
            ), 'Code length must be at most the number of colors in non-duplicate case.'
        # assert 4 <= options.tries <= 16, 'Tries must be between 4 and 16.'

        v = MastermindView(
            options.length,
            options.colors,
            options.duplicates,
            options.tries,
            ctx.author.id,
            options.singlePlayer,
        )
        m = await ctx.send(v.gen_content(False), view=v)

        self.active_games[ctx.channel.id] = {'owner': ctx.author.id, 'cog_name': 'Game'}

        if await v.wait():
            await m.edit(content=v.gen_content(True) + '\n\n' + 'Timeout.', view=None)

        self.naturalendgame(ctx)

    async def cog_command_error(self, ctx, e):
        await ctx.send(f'`{e.__cause__}`')


async def setup(bot):
    await bot.add_cog(GameCog(bot))

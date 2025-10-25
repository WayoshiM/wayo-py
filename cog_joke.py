import asyncio
import orjson
import random

import discord
from discord.ext import commands
from pg import PG

from dropboxwayo import dropboxwayo


class JokeCog(commands.Cog, name='Jokes'):
    """Simple, mostly static commands with inside jokes in the G-R community."""

    bob_ego_pg_skip_random = {
        PG.CheckGame: 2,
        PG.GrandGame: 5,
        PG.Bump: 9,
        PG.ClockGame: 11,
    }

    async def cog_load(self):
        await self._load_jokes()

    @commands.command(hidden=True)
    @commands.is_owner()
    async def load_jokes(self, ctx):
        await self._load_jokes()
        await ctx.message.add_reaction('✅')

    async def _load_jokes(self):
        self.jokes = orjson.loads(await asyncio.to_thread(dropboxwayo.download, '/heroku/wayo-py/jokes.json'))
        self.jm = {}

        for e, j in enumerate(self.jokes):
            self.jm[j['name']] = e
            for a in j['aliases']:
                self.jm[a] = e

    @commands.command(name='tpirjoke', aliases=['joke', 'tj', 'j'])
    async def joke_text(self, ctx, joke, *args):
        """Make a joke."""
        j = self.jokes[self.jm[joke]]
        command = j['command']

        bob_arg = None
        args = list(args)

        while True:
            match command:
                case str():
                    if command.endswith('mp3'):
                        with open(f'joke_sounds/{command}', 'rb') as f:
                            await ctx.send(file=discord.File(f, filename=command))
                    else:
                        await ctx.send(command)
                case list():
                    if idx := self.bob_ego_pg_skip_random.get(bob_arg):
                        await ctx.send(command[idx])
                    else:
                        await ctx.send(random.choice(command))
                case dict():
                    if command['branchType'] == 'if':
                        for logic, branch in command['branch'].items():
                            if logic == 'if':
                                if random.random() < branch['condition']:
                                    command = branch['command']
                                    break
                            elif logic == 'else':
                                command = branch['command']
                                break
                            else:
                                pass
                        continue
                    elif command['branchType'] == 'subcommand':
                        try:
                            branch = args.pop(0).lower()
                            command = command['branch'][branch]['command']
                            if branch == 'bob':
                                try:
                                    bob_arg = PG.lookup(args.pop(0))
                                except (IndexError, KeyError):
                                    pass
                            continue
                        except:
                            raise ValueError("You can't have an ego without Bob or Mike.")
            break

    @commands.command(aliases=['lj', 'listjokes'])
    async def list_jokes(self, ctx, search: str = None):
        """List information about all jokes available, or just ones that match the search term if given."""
        await ctx.send(
            '\n'.join(
                f'- `{joke["name"]}`'
                + (' (aliases: ' + (', '.join(f'`{a}`' for a in joke['aliases'])) + ')' if joke['aliases'] else '')
                + f': {joke["description"]}'
                for joke in self.jokes
                if (search in joke['name'] or any(search in a for a in joke['aliases']) if search else True)
            )
        )

    @joke_text.error
    async def jt_error(self, ctx, e):
        if hasattr(e, 'original') and isinstance(e.original, KeyError):
            await ctx.send('Joke not found.')
        elif hasattr(e, 'original') and isinstance(e.original, ValueError):
            await ctx.send(e.original)


async def setup(bot):
    pass
    await bot.add_cog(JokeCog(bot))

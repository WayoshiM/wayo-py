from datetime import timedelta

import discord
from discord import app_commands
from discord.ext import commands


def ctx_menu_check(interaction: discord.Interaction) -> bool:
    perms = interaction.user.top_role.permissions
    return perms.administrator or perms.kick_members


class GRModCog(commands.Cog, name='GRMod'):
    """Commands related to moderator duties on golden-road.net server. Restricted to those users only."""

    def __init__(self, bot):
        self.bot = bot
        self.sinbin_length = timedelta(minutes=2)
        # https://github.com/Rapptz/discord.py/issues/7823#issuecomment-1086830458
        self.sinbinmenu = app_commands.ContextMenu(
            name='sinbin', callback=self.sinbin_ctx_menu, guild_ids=[314598609591074816]
        )
        # not sure these next two lines do anything at the moment
        self.sinbinmenu.default_permissions = discord.Permissions(kick_members=True)
        self.sinbinmenu.guild_only = True
        self.sinbinmenu.add_check(lambda i: self.auth_check(i.user))
        self.bot.tree.add_command(self.sinbinmenu)

    async def cog_unload(self) -> None:
        self.bot.tree.remove_command(self.sinbinmenu.name, type=self.sinbinmenu.type)

    @app_commands.checks.check(ctx_menu_check)
    async def sinbin_ctx_menu(self, interaction: discord.Interaction, member: discord.Member) -> None:
        await interaction.response.send_message((await self.sinbin_actual(member)))

    @commands.Cog.listener()
    async def on_ready(self):
        self._gr_guild = self.bot.get_guild(314598609591074816)

    async def cog_check(self, ctx):
        if ctx.guild == self._gr_guild:
            return self.auth_check(ctx.author)
        else:
            return False

    def auth_check(self, author):
        perms = author.top_role.permissions
        return perms.administrator or perms.kick_members

    @commands.hybrid_command()
    @app_commands.guilds(314598609591074816)
    @app_commands.guild_only()
    @app_commands.default_permissions(kick_members=True)
    async def sinbin(self, ctx, members: commands.Greedy[commands.MemberConverter]):
        """Send shameful users (those who fork a won game) to a well-deserved two-minute timeout. Mods only."""
        if members:
            await ctx.send((await self.sinbin_actual(*members)))
        else:
            await ctx.send('Why are you forking nobody, moderator? (Double-check your cases.)')

    async def sinbin_actual(self, *members):
        success = []
        error = []
        for m in members:
            try:
                await m.timeout(self.sinbin_length, reason='Sin bin: forking.')
                success.append(m.display_name)
            except (discord.Forbidden, discord.HTTPException, TypeError) as e:
                # error.append(f'`Could not send {m.display_name} to the sin bin, error was: {e}`')
                error.append(m.display_name)
        return ((', '.join(success) + ': Forking can be a dangerous proposition. 😈 🗑️') if success else '') + (
            (', '.join(error) + ': Forking is never a dangerous proposition for you... :angel:') if error else ''
        )

    async def cog_command_error(self, ctx, e):
        if isinstance(e, commands.CheckFailure):
            await ctx.send('`This command can only be run by a moderator or higher on the G-R.net server.`')


async def setup(bot):
    await bot.add_cog(GRModCog(bot))

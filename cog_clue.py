from discord.ext import commands, tasks
import discord, random, asyncio, pickle, logging
from clue import *
from util import SuspectConverter, WeaponConverter, RoomConverter, send_PIL_image, SCHEDULER_TZ

from dropboxwayo import dropboxwayo

from datetime import datetime, timezone, timedelta

_log = logging.getLogger('wayo_log')

ABOUT_MASTER_CLUE = [
    'Four new suspects, two new weapons, and twelve rooms!',
    'The magnifying glasses are "snoop" spaces. You can randomly reveal one player\'s card only to you. You\'ll be able to continue your roll after the snoop, with the "cannot traverse previously hit spaces" rule reset on the leftover roll.',
    'By default, the "snoopee" will be told what card is snooped from their hand. The `blind_snoopee` can turn this notification off.',
    'You may hop over another player\'s position instead of being totally blocked.',
    'Everyone starts in the Cloak Room, but it can never be the answer and can never be suggested.',
    'There is one secret passage outside of the rooms entirely. Use "secret" upon hitting the arrow.',
    'You can combine movement with secret passages and "hopping over" rooms, counting each as one against your roll. Cover a lot of space at once!',
    'In a combo roll including a room with only one door, "door a" must be explicitly in the move list.',
    'Around the courtyard, there are outside passages between spaces, it only takes a roll of 1 to go between these.',
]


class ClueCog(commands.Cog, name='Clue'):
    """Commands related to an active game of Clue."""

    def __init__(self, bot):
        self.bot = bot
        self.vc = None
        self.debug = _log.getEffectiveLevel() == logging.DEBUG
        self.active_games = {}
        self.valid_options = {'timer': int, 'turn_order': str, 'random_spots': None, 'blind_snoopee': None}

    # async def cog_load(self):
    #     try:
    #         self.active_games = pickle.loads(await asyncio.to_thread(dropboxwayo.download, f'/heroku/wayo-py/active_clue.pickle'))
    #         _log.info(f'{len(self.active_games)} Clues loaded.')
    #     except:
    #         _log.info('Failed to load saved Clue games')

    async def cog_unload(self):
        # await asyncio.to_thread(dropboxwayo.upload, pickle.dumps(self.active_games), f'/heroku/wayo-py/active_clue.pickle')
        for channel_id in self.active_games.keys():
            if www := self.active_games[channel_id].get('www'):
                chan = discord.utils.get(self.bot.get_all_channels(), id=channel_id)
                await chan.send(
                    "I've been asked to reset! This game has been lost, apologies!",
                    embed=discord.Embed(title='WHO HAD WHAT', description=www),
                )

    async def cog_before_invoke(self, ctx):
        # if (datetime.now(timezone.utc) - ctx.bot.ready_time).days > 0:
        #     await ctx.send(
        #         "CRITICAL WARNING: I am within the 216-minute time range that I \"cycle\" and reset! Use `!reset_time` to get the possible time range I'll reset. It is highly recommended you now finish the game quickly!"
        #     )
        # if (task := self.active_games[ctx.channel.id].get('timer_task')) and ctx.command.name not in (
        #     'stoptimer',
        #     'viewboard',
        # ):
        #     task.cancel()
        if (
            not ctx.command.name.startswith('debug')
            and ctx.channel.id in self.active_games
            and self.active_games[ctx.channel.id]['begun']
        ):
            self.active_games[ctx.channel.id]['invoke_in_progress'] = True

    @commands.command(hidden=True)
    @commands.is_owner()
    async def debugclue(self, ctx, arg):
        self.debug = arg == 'on'
        await ctx.send('debug turned {}.'.format('on' if self.debug else 'off'))

    @commands.Cog.listener()
    async def on_ready(self):
        self.wayo_user = (await self.bot.application_info()).owner

    async def cog_check(self, ctx):
        if ctx.channel.id in self.active_games:
            a = self.active_games[ctx.channel.id]['begun']
            b = ctx.command.name.endswith('clue')
            if b:
                return not a
            else:
                return (
                    a
                    and self.active_games[ctx.channel.id]['cg'].cur_player.id == ctx.author
                    and not self.active_games[ctx.channel.id]['invoke_in_progress']
                )
        else:
            return ctx.command.hidden

    async def set_vc(self, host_user):
        try:
            vc = host_user.voice.channel
            if not vc:
                raise AttributeError
            self.vc = await vc.connect(reconnect=False)
        except AttributeError:
            return 'You are not currently in a voice channel for me to join, not starting game.'
        except discord.ClientException:
            return "I'm already busy in another voice channel, sorry! Not starting game."
        except asyncio.TimeoutError:
            return 'I timed out trying to join your voice channel, try again.'
        except discord.opus.OpusNotLoaded:
            return 'Voice not currently supported.'

    async def disconnect_voice(self):
        if self.vc:
            await self.vc.disconnect()
            self.vc = None

    async def play_clip_clue(self, fn, *, vi=0.5, vr_step=10, vr_factor=2, holdup=0):
        if self.vc:
            self.vc.stop()
            source = discord.PCMVolumeTransformer(discord.FFmpegPCMAudio(f'clue_sounds{os.sep}{fn}.mp3'), vi)
            self.vc.play(source, after=lambda e: print('Player error: %s' % e) if e else None)

            async def vr():
                try:
                    while self.vc.is_playing():
                        await asyncio.sleep(vr_step)
                        source.volume /= vr_factor
                except:
                    pass

            self.loop.create_task(vr())
        if holdup:
            await asyncio.sleep(holdup)

    async def register(self, ctx, options, master=False):
        # if 'voice' in options:
        #     e = await self.set_vc(ctx.author)
        #     if e:
        #         await ctx.send(str(e))
        #         return False

        self.active_games[ctx.channel.id] = {
            'begun': False,
            'owner': ctx.author.id,
            'master': master,
            'ct': MasterClueText() if master else ClueText(),
            'sug_count': 1,
            'cur_player': None,
            'options': [],
            'watch': {},
            'invoke_in_progress': None,
        }
        cg = self.active_games[ctx.channel.id]['cg'] = ClueGame(
            MasterSuspect if master else Suspect,
            MasterWeapon if master else Weapon,
            MasterRoom if master else Room,
            MasterClueBoard if master else BasicClueBoard,
        )

        for o in options:
            if any(o.startswith(vo) for vo in self.valid_options.keys()):
                if '=' in o:
                    try:
                        on, val = [oo.strip() for oo in o.split('=')]
                        type_ = self.valid_options[on]
                        self.active_games[ctx.channel.id]['options'].append(f'{on}: {val}')
                        if on == 'timer':
                            val = type_(val)
                            if val % 10:
                                val += 10 - (val % 10)
                            self.active_games[ctx.channel.id]['timer'] = val
                        elif on == 'turn_order':
                            self.active_games[ctx.channel.id]['turn_order'] = val
                    except:
                        raise ValueError(f'Malformed option: {o}')
                else:
                    self.active_games[ctx.channel.id]['options'].append(o)

        if self.debug:
            players = self.active_games[ctx.channel.id]['players'] = {s: ctx.author for s in list(cg.suspect_type)[:3]}
            cg.start(
                players,
                self.active_games[ctx.channel.id].get('turn_order', 'standard'),
                'random_spots' in self.active_games[ctx.channel.id]['options'],
            )
            # for p in cg.players:
            # cg.board.player_positions[p.suspect] = random.choice(list(cg.room_type))

            await ctx.send(
                '```Answer: '
                + str(cg.answer)
                + '\n'
                + '\n'.join(f'{cp.id.display_name} as {cp.suspect}: {cp.cards}' for cp in cg.players)
                + '\n\n'
                + 'now use !beginclue```'
            )
        else:
            self.active_games[ctx.channel.id]['players'] = {}
            await self.play_clip_clue('Mr. Boddy', vi=0.25, vr_step=60)
            # gather players
            await ctx.send(
                'Play now by typing `!playclue` then one of {} (Caps-insensitive)'.format(
                    ', '.join('`' + s.name + '`' for s in cg.suspects)
                )
            )

        if self.active_games[ctx.channel.id]['options']:
            await ctx.send('Set options: ' + ', '.join(str(o) for o in self.active_games[ctx.channel.id]['options']))
        self.active_games[ctx.channel.id]['options'].append('nohints')

        return True

    @commands.command()
    async def updateclue(self, ctx):
        await ctx.send(
            'Updates\n- `twodice` and `nohints` are always on.\n- New option: `random_turn_order`. Does what it says on the tin.\n\nMASTER CLUE!\n'
            + ('\n'.join(f'- {s}' for s in ABOUT_MASTER_CLUE))
        )

    @commands.command()
    async def playclue(self, ctx, s: SuspectConverter):
        """Join a Clue game's list of players."""
        if (
            s not in self.active_games[ctx.channel.id]['players']
            and ctx.author not in self.active_games[ctx.channel.id]['players'].values()
            and ctx.author not in self.active_games[ctx.channel.id]['watch']
        ):
            self.active_games[ctx.channel.id]['players'][s] = ctx.author
            e = discord.Embed(
                description='\n'.join(
                    f'{s}: {a.display_name}' for s, a in self.active_games[ctx.channel.id]['players'].items()
                )
            )
            await ctx.send(f'{ctx.author.display_name}, {s}: :white_check_mark:', embed=e)

    @commands.command()
    async def watchclue(self, ctx, how: SuspectConverter | Literal['all', 'answer'] = 'all'):
        """Watch a Clue game in one of the following ways:

        - answer: Only be given the answer.
        - suspect: Play along with one player's cards (by suspect).
        - all: Be given all cards.

        Please be responsible!"""
        assert (
            ctx.author not in self.active_games[ctx.channel.id]['players'].values()
        ), 'Yes, I thought of this already, you cheater.'
        self.active_games[ctx.channel.id]['watch'][ctx.author] = how
        e = discord.Embed(
            description='\n'.join(f'{p.display_name}: {h}' for p, h in self.active_games[ctx.channel.id]['watch'].items())
        )
        await ctx.send(f'WATCH: {ctx.author.display_name} --> {how} :white_check_mark:', embed=e)

    @commands.command()
    async def removeclue(self, ctx):
        """Remove yourself from an active Clue game's list of players."""
        for sr, a in self.active_games[ctx.channel.id]['players'].items():
            if ctx.author == a:
                del self.active_games[ctx.channel.id]['players'][sr]
                d = '\n'.join(f'{s}: {a.display_name}' for s, a in self.active_games[ctx.channel.id]['players'].items())
                if d:
                    e = discord.Embed(description=d)
                await ctx.send(f'{ctx.author.display_name}, {sr}: :x: ', embed=e if d else None)
                break

    @commands.command()
    async def removewatchclue(self, ctx):
        """Remove yourself from watching a game."""
        del self.active_games[ctx.channel.id]['watch'][ctx.author]
        d = '\n'.join(f'{p.display_name}: {h}' for p, h in self.active_games[ctx.channel.id]['watch'].items())
        if d:
            e = discord.Embed(description=d)
        await ctx.send(f'WATCH: {ctx.author.display_name} :x:', embed=e if d else None)

    @commands.command()
    async def beginclue(self, ctx):
        if not self.debug and len(self.active_games[ctx.channel.id]['players']) < 3:
            await ctx.send('Not enough players yet.')
        elif self.active_games[ctx.channel.id]['owner'] != ctx.author.id:
            await ctx.send('Only the owner of the game can begin it.')
        else:
            if self.vc:
                self.vc.stop()

            if not self.debug:
                self.active_games[ctx.channel.id]['cg'].start(
                    self.active_games[ctx.channel.id]['players'],
                    self.active_games[ctx.channel.id].get('turn_order', 'standard'),
                    'random_spots' in self.active_games[ctx.channel.id]['options'],
                )

                player_count = len(self.active_games[ctx.channel.id]['players'])
                status_embed = discord.Embed(title=f'{player_count}-player game:')
                for cp in self.active_games[ctx.channel.id]['cg'].players:
                    status_embed.add_field(name=f'{cp.id.display_name} as {cp.suspect}', value=f'{len(cp.cards)} cards')

                info_message = self.active_games[ctx.channel.id]['info_message'] = await ctx.send(
                    (
                        'I apologize for any sexism in this playing. :womens:'
                        if not self.active_games[ctx.channel.id]['cg'].even()
                        else ''
                    ),
                    embed=status_embed,
                )
                try:
                    await info_message.pin()
                except discord.Forbidden:
                    pass

                # send cards
                for cp in self.active_games[ctx.channel.id]['cg'].players:
                    try:
                        await cp.id.send(''.join(itertools.repeat('=THE=LINE=', 7)))
                        with ClueCard.multicard_image(cp.cards) as i:
                            await send_PIL_image(cp.id, i, f'{cp.id.display_name}_{cp.suspect.name}_cards')
                    except discord.Forbidden:
                        await ctx.send(
                            f'{cp.id.display_name} cannot receive DMs from me, aborting game. Please use `!cancelgame`.'
                        )
                        return

            self.active_games[ctx.channel.id]['www'] = (
                '\n'.join(
                    f'{cp.id.display_name} as {cp.suspect}: {" ".join(c.name for c in cp.cards)}'
                    for cp in self.active_games[ctx.channel.id]['cg'].players
                )
                + '\n\n**ANSWER: '
                + " ".join(c.name for c in self.active_games[ctx.channel.id]['cg'].answer)
                + '**'
            )

            if not self.debug and not self.wayo_user in {cp.id for cp in self.active_games[ctx.channel.id]['cg'].players}:
                self.active_games[ctx.channel.id]['watch'][self.wayo_user] = 'all'

            for watcher, how in self.active_games[ctx.channel.id]['watch'].items():
                www = self.active_games[ctx.channel.id]['www']
                if how == 'answer':
                    www = www.split('\n')[-1]
                elif how != 'all':
                    for w in www.split('\n'):
                        if str(how) in w:
                            www = w
                            break
                await watcher.send(
                    f'INFO FOR WATCHED GAME ({str(how).upper()})',
                    embed=discord.Embed(title='WHO HAS WHAT' if how == 'all' else '', description=www),
                )

            self.active_games[ctx.channel.id]['stoptimer'] = {
                cp.id: 1 for cp in self.active_games[ctx.channel.id]['cg'].players
            }
            self.active_games[ctx.channel.id]['begun'] = True

    @commands.command()
    async def endturn(self, ctx):
        self.active_games[ctx.channel.id]['cg'].endturn()

    @commands.command()
    async def roll(self, ctx, *override_roll):
        if self.debug:
            # still need to run the command to advance gameplay
            self.active_games[ctx.channel.id]['cg'].roll()
            result = int(override_roll[0]) if override_roll else 12
            self.active_games[ctx.channel.id]['cg'].cur_roll = result
        else:
            result = self.active_games[ctx.channel.id]['cg'].roll()
        extra = 'n' if result in (8, 11) else ''
        await ctx.channel.send(
            self.active_games[ctx.channel.id]['cur_player'].id.display_name
            + f' rolled a{extra} {result}.'
            + (' :oyster:' if result == 12 else '')
        )
        if result == 2:
            await ctx.channel.send('AW! :snake: :eyes:')

    @commands.command()
    async def move(self, ctx, *moves):
        """one or more pairs of directions and step(s) to take in that direction, adding up to the immediately previous roll.
        If in a room with more than one door and rolling out, first specify which door according to the given map image."""
        # use cg_action here for move conversion
        _, move_args = self.active_games[ctx.channel.id]['cg'].translate('move ' + ' '.join(m.upper() for m in moves))
        # print(move_args)
        r = self.active_games[ctx.channel.id]['cg'].move(*move_args)
        if r:
            await self.announce_room(ctx, r)
        elif r is False:  # snoop, this is a very quickfix on 2024-09-17
            pass
        else:
            self.active_games[ctx.channel.id]['cg'].endturn()

    @commands.command()
    async def snoop(self, ctx, suspect: SuspectConverter):
        snooped, card = self.active_games[ctx.channel.id]['cg'].snoop(suspect)
        snooper = self.active_games[ctx.channel.id]['cur_player'].id
        await snooper.send(f"{card} | {snooped.id.display_name}'s snooped card")
        if 'blind_snoopee' not in self.active_games[ctx.channel.id]['options']:
            await snooped.id.send(f'{snooper.display_name} snooped {card} from your hand.')

    @commands.command()
    async def secret(self, ctx):
        await self.announce_room(ctx, self.active_games[ctx.channel.id]['cg'].secret())

    async def announce_room(self, ctx, r):
        extra = (
            f'\n:musical_note: with {"i"*random.randint(5,30)}King! :musical_note:'
            if r is Room.LOUNGE or r is MasterRoom.STUDIO
            else ''
        )
        await ctx.channel.send(self.active_games[ctx.channel.id]['cur_player'].id.display_name + f' entered the {r}.{extra}')

    @commands.command()
    async def suggest(self, ctx, suspect: SuspectConverter, weapon: WeaponConverter):
        """If you are the first to disprove, I'll DM you which card(s) you can send.
        You must confirm by DMing me a message containing which card to send before the game can move forward.
        Creativity is allowed, any message containing one and exactly one of the eligible cards will be accepted."""
        suggestion = (
            suspect,
            weapon,
            self.active_games[ctx.channel.id]['cg'].board.player_positions[
                self.active_games[ctx.channel.id]['cur_player'].suspect
            ],
        )
        hint, disprove_cp, disprove_options = self.active_games[ctx.channel.id]['cg'].suggest(suspect, weapon)
        sug_count = self.active_games[ctx.channel.id]['sug_count']

        cp_embed = discord.Embed()
        if 'nohints' not in self.active_games[ctx.channel.id]['options']:
            cp_embed.title = hint
        cp_embed.description = ''
        cp_embed.set_footer(text=f'SUGGESTION {sug_count}')
        cp_iter = itertools.takewhile(lambda p: p is not disprove_cp, self.active_games[ctx.channel.id]['cg'].players)
        next(cp_iter)  # flush cur_player out
        sug_description = [f'{cp.id.display_name} cannot disprove.' for cp in cp_iter]
        if disprove_cp:
            sug_description.append(f'{disprove_cp.id.display_name} CAN disprove.')

        with ClueCard.multicard_image(suggestion) as i:
            await send_PIL_image(ctx.channel, i, f'suggestion{sug_count}')

        async with ctx.typing():
            await self.play_clip_clue(
                f'Room {suggestion[2].ambience}' if random.random() < 0.5 else str(suggestion[0]), holdup=10
            )
            if 'NOT' not in hint and 'nohints' not in self.active_games[ctx.channel.id]['options']:
                await self.play_clip_clue('Shock!', vi=1.5)

            sug_message = await ctx.channel.send(embed=cp_embed)
            for i in range(1, len(sug_description) + 1):
                cp_embed.description = '\n'.join(sug_description[:i])
                await asyncio.sleep(5)
                await sug_message.edit(embed=cp_embed)

        if disprove_cp:
            card_options = [c.name for c in disprove_options]
            d = await disprove_cp.id.send(
                'You must disprove (one of) the following to '
                + self.active_games[ctx.channel.id]['cur_player'].id.display_name
                + f': {" ".join(card_options)}'
            )

            cards_revealed = lambda mes: sum(bool(re.search(c, mes.content, re.I)) for c in card_options)
            mes_content = None
            try:
                mes = await self.bot.wait_for(
                    'message',
                    check=lambda m: m.channel == d.channel and m.author == disprove_cp.id and cards_revealed(m) == 1,
                    timeout=300,
                )
                mes_content = mes.content
            except asyncio.TimeoutError:
                mes_content = f'TIMEOUT: {random.choice(card_options)}'
                await disprove_cp.id.send(f'{mes_content} was automatically disproved for you.')

            for c in disprove_options:
                if re.search(c.name, mes_content, re.I):
                    await self.active_games[ctx.channel.id]['cur_player'].id.send(
                        f"{mes_content} | {disprove_cp.id.display_name}'s disproval for suggestion {sug_count}: {c.name}"
                    )
                    break
        else:
            await ctx.channel.send(':notes: ***END IT NOW, END IT NOW*** :notes:')

        self.active_games[ctx.channel.id]['sug_count'] += 1

    @commands.command()
    async def accuse(self, ctx, suspect: SuspectConverter, weapon: WeaponConverter, room: RoomConverter):
        """Be sure you're right or it's game over!"""
        assert room != MasterRoom.CLOAK, 'Cloak Room cannot be right.'
        accusation = (suspect, weapon, room)
        result = self.active_games[ctx.channel.id]['cg'].accuse(suspect, weapon, room)

        cp_embed = discord.Embed()

        with ClueCard.multicard_image(accusation) as i:
            await send_PIL_image(
                ctx.channel, i, 'accusation_' + self.active_games[ctx.channel.id]['cur_player'].id.display_name
            )

        texts = self.active_games[ctx.channel.id]['ct'].generate(
            accusation, self.active_games[ctx.channel.id]['cur_player'].suspect
        )
        cp_embed.title = ''
        cp_embed.description = texts[0]
        cp_embed.color = suspect.color
        e = await ctx.channel.send(embed=cp_embed)
        async with ctx.typing():
            await self.play_clip_clue(f'Room {room.ambience}', holdup=8)
            cp_embed.description += '\n' + texts[1]
            await e.edit(embed=cp_embed)
            await self.play_clip_clue(f'{suspect}', holdup=8)
            cp_embed.description += '\n' + texts[2]
            await e.edit(embed=cp_embed)
            await self.play_clip_clue('Shock!', holdup=2)

        if result:
            e = await ctx.channel.send(texts[3])
            cp_embed.description = ''
            await self.play_clip_clue('Terror!', holdup=3)
            for t in texts[4:-1]:
                cp_embed.description += '\n' + t
                await e.edit(embed=cp_embed)
                await asyncio.sleep(3)
            cp_embed.description = texts[-1]
            cp_embed.color = self.active_games[ctx.channel.id]['cur_player'].suspect.color
            if not self.active_games[ctx.channel.id]['master']:
                with self.active_games[ctx.channel.id]['cur_player'].suspect.mosaic_image('win') as i:
                    await send_PIL_image(
                        ctx.channel, i, self.active_games[ctx.channel.id]['cur_player'].suspect.name + '_win'
                    )
            await ctx.channel.send(
                self.active_games[ctx.channel.id]['cur_player'].id.display_name + ' wins!', embed=cp_embed
            )
            await self.play_clip_clue('Elementary', holdup=20, vr_step=20)
            await ctx.channel.send(
                'gg shitheads!' if random.random() < 0.25 else 'Good game everyone!',
                embed=discord.Embed(title='WHO HAD WHAT', description=self.active_games[ctx.channel.id]['www']),
            )
            await asyncio.sleep(10)
        else:
            if not self.active_games[ctx.channel.id]['master']:
                with self.active_games[ctx.channel.id]['cur_player'].suspect.mosaic_image('gameover') as i:
                    await send_PIL_image(
                        ctx.channel, i, self.active_games[ctx.channel.id]['cur_player'].suspect.name + '_gameover'
                    )
            cp_embed.title = 'There is evidence that you are wrong!'
            cp_embed.description = ''
            cp_embed.color = self.active_games[ctx.channel.id]['cur_player'].suspect.color
            m = await ctx.channel.send(embed=cp_embed)
            await self.play_clip_clue('Disbelief!', holdup=4)
            cp_embed.description = 'GAME OVER ' + str(self.active_games[ctx.channel.id]['cur_player'].suspect)
            await m.edit(embed=cp_embed)
            await self.play_clip_clue('A Flaw in Your Theory', holdup=5)
            if self.active_games[ctx.channel.id]['cg'].accuse_count < len(self.active_games[ctx.channel.id]['cg'].players):
                self.active_games[ctx.channel.id]['cg'].endturn()
            else:
                await ctx.channel.send('... :disappointed:')
                await asyncio.sleep(5)
                await ctx.channel.send(
                    'GAME OVER',
                    embed=discord.Embed(title='WHO HAD WHAT', description=self.active_games[ctx.channel.id]['www']),
                )

    @commands.command(aliases=['showmethefuckingboardyoushit'])
    async def viewboard(self, ctx):
        """View the entire game board, zoomed out. can be done anytime it's your turn."""
        with self.active_games[ctx.channel.id]['cg'].board.image() as i:
            await send_PIL_image(ctx.channel, i, 'board_full')

    @commands.command(aliases=['stop'])
    async def stoptimer(self, ctx):
        """Stop the timer. Once a game."""
        if task := self.active_games[ctx.channel.id].get('timer_task'):
            if not self.active_games[ctx.channel.id]['stoptimer'][ctx.author.id]:
                await ctx.send('You are out of stops for this game.')
            else:
                task.cancel()
                await ctx.send('Timer stopped.')
                self.active_games[ctx.channel.id]['stoptimer'][ctx.author.id] -= 1
                del self.active_games[ctx.channel.id]['timer_task']
        else:
            await ctx.send('No timer is running right now.')

    async def cog_after_invoke(self, ctx):
        if not ctx.command.name.startswith('debug') and self.active_games[ctx.channel.id]['begun']:
            if options := self.active_games[ctx.channel.id]['cg'].next_options:
                changed_turn = (
                    self.active_games[ctx.channel.id]['cur_player'] != self.active_games[ctx.channel.id]['cg'].cur_player
                    or 'roll' in options
                )
                self.active_games[ctx.channel.id]['cur_player'] = self.active_games[ctx.channel.id]['cg'].cur_player

                roll = self.active_games[ctx.channel.id]['cg'].board.leftover_roll
                post_snoop = ctx.command.name == 'snoop' and roll

                if changed_turn or post_snoop:
                    with self.active_games[ctx.channel.id]['cg'].board_zoomed_image() as i:
                        await send_PIL_image(
                            ctx.channel,
                            i,
                            'board_' + self.active_games[ctx.channel.id]['cg'].cur_player.suspect.name + '_zoomed',
                        )

                cp_embed = discord.Embed(color=self.active_games[ctx.channel.id]['cur_player'].suspect.color)

                player_name = self.active_games[ctx.channel.id]['cur_player'].id.display_name

                cp_embed.title = f'Current player: {player_name}'
                if (
                    type(
                        self.active_games[ctx.channel.id]['cg'].board.player_positions[
                            self.active_games[ctx.channel.id]['cur_player'].suspect
                        ]
                    )
                    is self.active_games[ctx.channel.id]['cg'].room_type
                ):
                    cp_embed.title += ' in ' + str(
                        self.active_games[ctx.channel.id]['cg'].board.player_positions[
                            self.active_games[ctx.channel.id]['cur_player'].suspect
                        ]
                    )
                cp_embed.description = 'You can: ' + ', '.join(options)

                message = f'{player_name} has a leftover roll of {roll}.' if post_snoop else ''

                await ctx.channel.send(message, embed=cp_embed)

                if timer := self.active_games[ctx.channel.id].get('timer'):

                    @tasks.loop(time=(datetime.now().astimezone(SCHEDULER_TZ) + timedelta(seconds=timer * 9 // 10)).timetz())
                    async def timeout_endturn():
                        t = timer // 10
                        await ctx.channel.send(f'{t} seconds remain, {player_name}!')
                        await asyncio.sleep(t)
                        await ctx.channel.send(f'{player_name} timed out and has had their turn ended forcibly.')
                        self.active_games[ctx.channel.id]['cg'].force_endturn()
                        await self.cog_after_invoke(ctx)

                    self.active_games[ctx.channel.id]['timer_task'] = timeout_endturn
                    timeout_endturn.start()

                self.active_games[ctx.channel.id]['invoke_in_progress'] = False
            else:  # close and cleanup.
                await self.disconnect_voice()
                if (
                    'info_message' in self.active_games[ctx.channel.id]
                    and self.active_games[ctx.channel.id]['info_message'].pinned
                ):
                    await self.active_games[ctx.channel.id]['info_message'].unpin()
                del self.active_games[ctx.channel.id]
                self.bot.get_cog('Game').naturalendgame(ctx)

    async def cog_command_error(self, ctx, error):
        e = error.__cause__
        if ctx.command.name in ('move', 'suggest', 'accuse', 'snoop') and isinstance(
            e, (ValueError, KeyError, AssertionError, IndexError)
        ):
            await ctx.channel.send('Improperly formatted action, try again. The problem was: `' + str(e) + '`')
        elif isinstance(e, AssertionError):
            await ctx.channel.send(e)
        elif isinstance(error, (commands.BadArgument, commands.BadUnionArgument)):
            await ctx.channel.send(error)
        elif isinstance(error, commands.CheckFailure):
            await ctx.channel.send('`Stop going too fast, you meddler.`')


async def setup(bot):
    await bot.add_cog(ClueCog(bot))

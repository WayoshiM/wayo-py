from enum import Enum, unique
from abc import ABCMeta, abstractmethod
from collections import deque, Counter, OrderedDict, namedtuple
from PIL import Image, ImageDraw, ImageFont
from functools import wraps, lru_cache, cached_property
from typing import *
from sortedcontainers import *
import random, itertools, copy, re, os
from string import ascii_uppercase, ascii_lowercase
import numpy as np


class ClueCard(Enum):
    def __str__(self):
        return self.value[0]

    def __repr__(self):
        return self.value[0]

    @property
    def rect_index(self):
        return self.value[1]

    def image_size(self):
        return (54, 62)

    def x0_y0(self):
        return (13, 12)

    def gap(self):
        return (2, 2)

    def image(self):
        if not self.value[1]:
            return None
        x0, y0 = self.x0_y0()
        xo, yo = self.value[1]
        w, h = self.image_size()
        gw, gh = self.gap()
        x, y = x0 + xo * (w + gw), y0 + yo * (h + gh)
        return Image.open(f'clue_images{os.sep}{self.__class__.__name__.lower()}s.png').crop((x, y, x + w, y + h))

    @classmethod
    def multicard_image(cls, cards, magnitude=2):
        i = None
        x0 = 0
        for c in cards:
            with c.image() as j:
                if not i:
                    xc, yc = [round(magnitude * d) for d in c.image_size()]
                    spacing = xc // 9
                    i = Image.new('RGBA', (len(cards) * xc + spacing * (len(cards) - 1), yc), (0, 0, 0, 0))
                if magnitude != 1:
                    j = j.resize((xc, yc), Image.BILINEAR)
                i.paste(j, (x0, 0))
            x0 += xc + spacing
        return i


@unique
class Suspect(ClueCard):
    SCARLET = ('Ms. Scarlet', (0, 2), 0)
    MUSTARD = ('Col. Mustard', (0, 0), 1)
    WHITE = ('Mrs. White', (1, 2), 2)
    GREEN = ('Mr. Green', (0, 1), 3)
    PEACOCK = ('Mrs. Peacock', (1, 1), 4)
    PLUM = ('Prof. Plum', (1, 0), 5)

    def __ge__(self, other):
        return self.value[2] >= other.value[2] if self.__class__ is other.__class__ else NotImplemented

    def __gt__(self, other):
        return self.value[2] > other.value[2] if self.__class__ is other.__class__ else NotImplemented

    def __le__(self, other):
        return self.value[2] <= other.value[2] if self.__class__ is other.__class__ else NotImplemented

    def __lt__(self, other):
        return self.value[2] < other.value[2] if self.__class__ is other.__class__ else NotImplemented

    @cached_property
    def color(self) -> int:
        with self.image() as i:
            r, g, b = i.getpixel((3, 3))
            return (r << 16) + (g << 8) + b

    def mosaic_image(self, mosaic, magnitude=2) -> Image:
        xo, yo = self.value[1]
        w, h = (187, 154)
        x0, y0 = (w * xo, h * yo)
        i = Image.open(f'clue_images{os.sep}{mosaic}.png').crop((x0, y0, x0 + w, y0 + h))
        if magnitude != 1:
            xc, yc = [round(magnitude * d) for d in i.size]
            i = i.resize((xc, yc), Image.BILINEAR)
        return i


class Weapon(ClueCard):
    KNIFE = ('Knife', (0, 0))
    REVOLVER = ('Revolver', (0, 1))
    PIPE = ('Lead Pipe', (0, 2))
    CANDLESTICK = ('Candlestick', (1, 0))
    ROPE = ('Rope', (1, 1))
    WRENCH = ('Wrench', (1, 2))


class Room(ClueCard):
    STUDY = ('Study', (2, 2), 'Quiet')
    HALL = ('Hall', (0, 0), 'Quiet')
    LOUNGE = ('Lounge', (1, 0), 'Stately')
    LIBRARY = ('Library', (1, 2), 'Stately')
    BILLIARD = ('Billiard Room', (0, 2), 'Quiet')
    DINING = ('Dining Room', (2, 0), 'Quiet')
    CONSERVATORY = ('Conservatory', (2, 1), 'Stately')
    BALL = ('Ballroom', (1, 1), 'Quiet')
    KITCHEN = ('Kitchen', (0, 1), 'Stately')

    @property
    def ambience(self) -> str:
        return self.value[2]


class ClueText:
    def __init__(self, fn: str = 'clue_text.txt') -> None:
        master_lists = [[]]
        for s in open(fn).read().split('\n'):
            if s:
                master_lists[-1].append(s)
            else:
                master_lists.append(list())
        self.room_text, self.suspect_text, self.weapon_text, self.murder_text = master_lists[:4]
        self.reaction_texts = master_lists[4:]

    def generate(self, accusation: Tuple[Suspect, Weapon, Room], accuser: Suspect) -> Sequence[str]:
        s, w, r = accusation
        generated = list()

        def flatten_index(c):
            x, y = c.rect_index
            return (3 if type(c) is Room else 2) * y + x

        generated.append(self.room_text[flatten_index(r)])
        si = flatten_index(s)
        generated.append(self.suspect_text[si])
        generated.append('{} {} the {}.'.format('He' if 0 <= si <= 2 else 'She', self.weapon_text[flatten_index(w)], w))
        generated.append(self.murder_text[si])

        if accuser is s:
            rtext = self.reaction_texts[si]
            generated.append(rtext[0])
            generated.append(rtext[2])
            generated.append(rtext[1])
        else:
            generated.append(self.reaction_texts[si][2])
            generated.append(self.reaction_texts[flatten_index(accuser)][0])

        return generated


class MasterClueCard(ClueCard):
    def image_size(self):
        return (40, 56)

    def x0_y0(self):
        return (16, 16)

    def gap(self):
        return (24, 40)


@unique
class MasterSuspect(MasterClueCard):
    SCARLET = ('Ms. Scarlet', (2, 1), 0, 0xD81840)
    MUSTARD = ('Col. Mustard', (0, 0), 1, 0xD8A038)
    WHITE = ('Mrs. White', (3, 1), 2, 0xE0E0E0)
    GREEN = ('Mr. Green', (2, 0), 3, 0x006000)
    PEACOCK = ('Mrs. Peacock', (0, 1), 4, 0x006070)
    PLUM = ('Prof. Plum', (1, 0), 5, 0x802050)
    PEACH = ('Ms. Peach', (4, 0), 6, 0xFFCC99)
    BRUNETTE = ('M. Brunette', (3, 0), 7, 0x6D4730)
    ROSE = ('Mme. Rose', (1, 1), 8, 0xFF48A5)
    GRAY = ('Sgt. Gray', (4, 1), 9, 0x777777)

    @property
    def color(self) -> int:
        return self.value[3]

    def mosaic_image(self, mosaic, magnitude=2) -> Image:
        raise NotImplemented


class MasterWeapon(MasterClueCard):
    KNIFE = ('Knife', (2, 0))
    REVOLVER = ('Revolver', (0, 1))
    PIPE = ('Lead Pipe', (3, 0))
    CANDLESTICK = ('Candlestick', (0, 0))
    ROPE = ('Rope', (1, 1))
    WRENCH = ('Wrench', (2, 1))
    HORSESHOE = ('Horseshoe', (1, 0))
    POISON = ('Poison', (4, 0))


class MasterRoom(MasterClueCard):
    def x0_y0(self):
        return (16, 24)

    def gap(self):
        return (8, 40)

    STUDIO = ('Studio', (4, 1), 'Quiet')
    COURTYARD = ('Courtyard', (3, 0), 'Quiet')
    TROPHY = ('Trophy Room', (5, 1), 'Stately')
    LIBRARY = ('Library', (3, 1), 'Stately')
    BILLIARD = ('Billiard Room', (0, 0), 'Quiet')
    DINING = ('Dining Room', (4, 0), 'Quiet')
    CONSERVATORY = ('Conservatory', (2, 0), 'Stately')
    CARRIAGE = ('Carriage House', (1, 0), 'Quiet')
    KITCHEN = ('Kitchen', (2, 1), 'Stately')
    GAZEBO = ('Gazebo', (1, 1), 'Quiet')
    DRAWING = ('Drawing Room', (5, 0), 'Quiet')
    FOUNTAIN = ('Fountain', (0, 1), 'Quiet')
    CLOAK = ('Cloak Room', None, None)

    @property
    def ambience(self) -> str:
        return self.value[2]


class MasterClueText(ClueText):
    def __init__(self, fn: str = 'clue_master_text.txt') -> None:
        super().__init__(fn=fn)
        master_lists = [[]]
        for s in open(fn).read().split('\n'):
            if s:
                master_lists[-1].append(s)
            else:
                master_lists.append(list())
        self.room_text, self.suspect_text, self.weapon_text, self.murder_text = master_lists[:4]
        self.reaction_texts = master_lists[4:]

    def generate(self, accusation: Tuple[MasterSuspect, MasterWeapon, MasterRoom], accuser: MasterSuspect) -> Sequence[str]:
        s, w, r = accusation
        generated = list()

        generated.append(self.room_text[list(MasterRoom).index(r)])
        si = list(MasterSuspect).index(s)
        generated.append(self.suspect_text[si])
        generated.append('{} {} the {}.'.format('He' if si % 2 else 'She', self.weapon_text[list(MasterWeapon).index(w)], w))
        generated.append(self.murder_text[si])

        if accuser is s:
            rtext = self.reaction_texts[si]
            generated.append(rtext[0])
            generated.append(rtext[2])
            generated.append(rtext[1])
        else:
            generated.append(self.reaction_texts[si][2])
            generated.append(self.reaction_texts[list(MasterSuspect).index(accuser)][0])

        return generated


class MoveDirection(Enum):
    UP = ('up', -1, 0, 'DOWN')
    LEFT = ('left', 0, -1, 'RIGHT')
    DOWN = ('down', 1, 0, 'UP')
    RIGHT = ('right', 0, 1, 'LEFT')
    SECRET = ('secret', 0, 0)
    DOOR = ('door', 0, 0)

    def __str__(self):
        return self.value[0]

    def translate(self, x: int, y: int) -> Tuple[int, int]:
        return x + self.value[1], y + self.value[2]

    @property
    def reverseDirection(self):
        return MoveDirection[self.value[3]] if len(self.value) == 4 else None


class Move(NamedTuple):
    direction: MoveDirection
    length: int


@lru_cache(maxsize=6)
def basic_piece_image(s: Suspect) -> Image:
    pieces_image = Image.open(f'clue_images{os.sep}clue_pieces.png')
    i = list(Suspect).index(s)
    w, h = pieces_image.width / 3, pieces_image.height / 2
    x, y = w * (i // 2), h * (i % 2)
    return pieces_image.crop((x, y, x + w, y + h))


class ClueBoard(metaclass=ABCMeta):
    @abstractmethod
    def __init__(
        self,
        room_type,
        suspect_type,
        get_piece_image,
        boardfn: str,
        player_positions: Dict[Suspect, Any],
        imagefn: str,
        image_info: Tuple[int, int, float, float],
        *,
        secret_pairs: Dict[int, int] = {},
        entrance_exceptions: Iterable[Tuple[int, MoveDirection]] = set(),
        full_block=True,
        allow_combo_rolls=False,
    ) -> None:
        self.suspect_type = suspect_type
        self.room_type = room_type
        self.get_piece_image = get_piece_image
        self.full_block = full_block
        self.allow_combo_rolls = allow_combo_rolls

        self.board = tuple(tuple(s) for s in open(boardfn).read().split('\n'))

        self.X = len(self.board)  # vertical position
        self.Y = len(self.board[0])  # horizontal position
        assert all(len(self.board[i]) == self.Y for i in range(1, self.X))

        self.imagefn = imagefn
        self.image_offset_x, self.image_offset_y, self.space_size_x, self.space_size_y = image_info

        self.entrance_map = {i: r for i, r in zip(ascii_uppercase, list(room_type))}  # type: Dict[str, Room]
        self.entrance_exceptions = frozenset(entrance_exceptions)
        self.secret_map = secret_pairs | {v: k for k, v in secret_pairs.items()}
        self.player_positions = player_positions

        self.door_map = {r: list() for r in self.entrance_map.values()}  # type: Dict[Room, List[Tuple[int,int]]]
        self.door_blocks = {r: set() for r in self.entrance_map.values()}  # type: Dict[Room, Set[Tuple[int,int]]]
        room_letter_map = {c: r for r, c in zip(list(room_type), ascii_lowercase)}  # type: ignore
        self.room_player_map = {r: list() for r in self.entrance_map.values()}  # type: Dict[Room, List[Tuple[int,int]]]
        for y in range(0, self.Y):
            for x in range(0, self.X):
                if self.board[x][y] in self.entrance_map:
                    r = self.entrance_map[self.board[x][y]]
                    self.door_map[r].append((x, y))
                    for d in ('UP', 'LEFT', 'RIGHT', 'DOWN'):
                        m = MoveDirection[d]
                        x2, y2 = m.translate(x, y)
                        if self.board[x2][y2] == 'O' and (r, m.reverseDirection) not in entrance_exceptions:
                            self.door_blocks[r].add((x2, y2))
                elif self.board[x][y] in room_letter_map.keys():
                    self.room_player_map[room_letter_map[self.board[x][y]]].append((x, y))

        self.fs = int(min(self.space_size_x, self.space_size_y))
        self.df = ImageFont.truetype('fonts/MYRIADPRO-REGULAR.OTF', self.fs)
        self.leftover_roll = 0
        self.leftover_visited = set()

    def move(self, player: Suspect | MasterSuspect, roll: int, moves: Sequence[Move]) -> Optional[Room | MasterRoom]:
        if player not in self.player_positions:
            raise ValueError(str(player) + ' not in this game')
        elif not moves:
            raise ValueError('Must have at least one move')

        visited = {self.player_positions[player]}

        if self.leftover_roll:
            visited |= {v for v in self.leftover_visited if type(v) is self.room_type}

        moves = deque(moves)
        current_pos = self.player_positions[player]

        # assert (m.dir not in (MoveDirection.SECRET, MoveDirection.DOOR) and m.length > 0 for m in moves)

        other_player_positions = set(v for k, v in self.player_positions.items() if k is not player and type(v) is tuple)

        # print(moves)

        while moves:
            if roll <= 0:
                raise ValueError('Too many moves for this roll')

            m = moves.popleft()

            if m.direction is MoveDirection.SECRET:
                if not self.allow_combo_rolls and moves:
                    raise ValueError("'secret' must be only move")
                oldr = current_pos
                if oldr not in self.secret_map:
                    raise ValueError('This position does not have a secret passage')
                else:
                    current_pos = self.secret_map[oldr]

                    if moves:
                        if current_pos in visited:
                            raise ValueError('Cannot traverse a particular space or room more than once in a roll.')
                        elif (not self.allow_combo_rolls or not moves) and type(current_pos) is self.room_type:
                            self.player_positions[player] = current_pos
                            return current_pos
                    elif type(current_pos) == self.room_type:
                        self.player_positions[player] = current_pos
                        return current_pos

                    roll -= 1
                    visited.add(current_pos)
            elif type(current_pos) == self.room_type:
                if m.direction is MoveDirection.DOOR:
                    if not moves:
                        raise ValueError('Cannot end a move at a door')
                    elif moves[0].direction in (MoveDirection.DOOR, MoveDirection.SECRET):
                        raise ValueError('Must move out of a door')
                    else:
                        if self.allow_combo_rolls and current_pos != self.player_positions[player]:
                            roll -= 1
                        try:
                            current_pos = self.door_map[current_pos][m.length]
                        except IndexError:
                            raise ValueError(f'{current_pos} does not have that many doors')

                    if (current_pos, moves[0].direction.reverseDirection) in self.entrance_exceptions:
                        raise ValueError(f'Illegal move out of {current_pos}')
                else:
                    raise ValueError('Must do secret or door (if multiple) in room')
            else:
                for i in range(0, m.length):
                    x, y = current_pos = m.direction.translate(*current_pos)

                    # "sliding"
                    while self.board[x][y] == 's':
                        x, y = current_pos = m.direction.translate(*current_pos)

                    if current_pos in visited:
                        raise ValueError('Cannot traverse a particular space more than once in a roll.')
                    else:
                        visited.add(current_pos)

                    if self.board[x][y] in ascii_uppercase:
                        if self.board[x][y] not in 'OX':
                            newr = self.entrance_map[self.board[x][y]]
                            if (newr, m.direction) in self.entrance_exceptions:
                                raise ValueError(f'Illegal move into {newr}')
                            if newr in visited:
                                raise ValueError(
                                    'Cannot traverse a particular room more than once in a roll (including starting room).'
                                )
                            current_pos = newr
                            if not self.allow_combo_rolls or not moves:
                                self.player_positions[player] = current_pos
                                return current_pos
                            else:
                                roll -= 1
                                if roll < 0:
                                    raise ValueError('Too many moves for this roll')
                                break
                        else:
                            roll -= 1
                            if roll < 0:
                                raise ValueError('Too many moves for this roll')
                            if (x, y) in other_player_positions:
                                if not roll or self.full_block:
                                    raise ValueError("Illegal move into another player's position")
                                elif not roll and self.board[x][y] == 'X':
                                    raise ValueError("Illegal move into another player's position")
                            elif self.board[x][y] == 'X':
                                self.leftover_roll = roll
                                self.leftover_visited = visited
                                break
                    else:
                        # print('{}: {}'.format((x, y), self.board[x][y]))
                        where = 'into a room' if self.board[x][y] == 'r' else 'out of bounds'
                        raise ValueError('Illegal move ' + where)

        if type(current_pos) == tuple:
            if roll < 0:
                raise ValueError('Too many moves for this roll')
            elif roll > 0 and self.board[x][y] != 'X':
                raise ValueError('Roll not fully used up')

        self.player_positions[player] = current_pos
        if self.board[x][y] == 'X':
            return False  # this is a very quickfix on 2024-09-17
        elif type(current_pos) == self.room_type:
            return current_pos

    def isBlocked(self, r: Room) -> bool:
        return self.full_block and self.door_blocks[r].issubset(
            self.player_positions.values()
        )  # self.door_blocks[r] <= set(self.player_positions.values())

    def __str__(self):
        bp_rev = self.gen_pretty_positions()
        return '\n'.join(
            ''.join(bp_rev[(x, y)].color if (x, y) in bp_rev else str(self.board[x][y]) for y in range(0, self.Y))
            for x in range(0, self.X)
        )

    def gen_pretty_positions(self) -> Dict[Tuple[int, int], Suspect]:
        pp = dict(self.player_positions)
        pp_rooms = {k: v for k, v in pp.items() if type(v) is self.room_type}
        room_count = Counter(pp_rooms.values())
        pp_room_xy = {v: random.sample(self.room_player_map[v], room_count[v]) for v in pp_rooms.values()}
        for k, v in pp_rooms.items():
            pp[k] = pp_room_xy[v].pop()
        return {v: k for k, v in pp.items()}

    def image(self, *, suspect_return: Suspect = None):
        bi = Image.open(self.imagefn).convert('RGBA')

        xp, yp = (0, 0)
        for p, s in self.gen_pretty_positions().items():
            y, x = p
            pi = self.get_piece_image(s)
            pi.thumbnail((round(self.space_size_x * 0.9), round(self.space_size_y * 0.9)))
            # find top left corner, then center it within square
            x0 = self.image_offset_x + round(x * self.space_size_x) + round((self.space_size_x - pi.width) / 2)
            y0 = self.image_offset_y + round(y * self.space_size_y) + round((self.space_size_y - pi.height) / 2)
            if suspect_return is s:
                xp, yp = x0, y0
            bi.paste(pi, (x0, y0), mask=pi)
        bi = Image.alpha_composite(bi, self.door_help)
        return (bi, (xp, yp)) if suspect_return else bi

    @cached_property
    def door_help(self) -> Image:
        bi = Image.open(self.imagefn)
        di = Image.new('RGBA', bi.size, (0, 0, 0, 0))
        did = ImageDraw.Draw(di)
        for dl in [dl for dl in self.door_map.values() if len(dl) > 1]:
            c = 65
            for y, x in dl:
                x0 = self.image_offset_x + round((x + 0.25) * self.space_size_x)
                y0 = self.image_offset_y + round(y * self.space_size_y)
                did.text((x0, y0), chr(c), font=self.df, fill=(0, 0, 0, 255))
                bi.paste(Image.new('RGBA', (self.fs, self.fs), (255, 255, 255, 127)), (x0, y0))
                c += 1
        return di


class BasicClueBoard(ClueBoard):
    def __init__(self, players: AbstractSet[Suspect], random_locs: bool = False) -> None:
        locs = [(0, 16), (7, 23), (24, 14), (24, 9), (18, 0), (5, 0)]
        if random_locs:
            random.shuffle(locs)
        pp = {s: l for s, l in zip(list(Suspect), locs)}

        for p in set(Suspect) - players:
            del pp[p]

        super().__init__(
            Room,
            Suspect,
            basic_piece_image,
            'clue_basicboard.txt',
            pp,
            f'clue_images{os.sep}clue_basicboard2.png',
            (15, 15, 29.625, 27.875),
            secret_pairs={Room.CONSERVATORY: Room.LOUNGE, Room.KITCHEN: Room.STUDY},
            entrance_exceptions=[
                (Room.STUDY, MoveDirection.LEFT),
                (Room.LOUNGE, MoveDirection.RIGHT),
                (Room.CONSERVATORY, MoveDirection.DOWN),
            ],
        )


class CluePlayer(NamedTuple):
    id: Any
    suspect: Suspect | MasterSuspect
    cards: Tuple[ClueCard]


class ClueWWW(NamedTuple):
    suspect: Suspect | MasterSuspect
    weapon: Weapon | MasterWeapon
    room: Room | MasterRoom


def limitCallsTo(limitedMethods: Iterable[str], option_var: str, option_gen: str, resetLogic: Tuple[str]):
    def srDecorator(cls):
        def srSenderDecorator(f):
            @wraps(f)
            def srSelfDecorator(self, *a, **k):
                o = getattr(self, option_var)
                if f.__name__ in o:
                    g = getattr(self, option_gen)
                    r = f(self, *a, **k)  # let exception propagate if necessary
                    setattr(self, option_var, g.send(f.__name__))
                    return r
                else:
                    raise RuntimeError('Limited method not within {} at the moment'.format(o))

            return srSelfDecorator

        for s in limitedMethods:
            setattr(cls, s, srSenderDecorator(getattr(cls, s)))

        resetMethod, resetOptions = resetLogic

        def srResetDecorator(f):
            @wraps(f)
            def srSelfDecorator2(self, *a, **k):
                setattr(self, option_gen, getattr(self, resetOptions)())
                g = getattr(self, option_gen)
                g.send(None)
                r = f(self, *a, **k)  # actually change turn before fake-sending start
                setattr(self, option_var, g.send('start'))
                return r

            return srSelfDecorator2

        setattr(cls, resetMethod, srResetDecorator(getattr(cls, resetMethod)))

        return cls

    return srDecorator


@limitCallsTo(
    limitedMethods=('start', 'roll', 'move', 'snoop', 'secret', 'suggest', 'accuse', 'endturn'),
    option_var='next_options',
    option_gen='_option_gen',
    resetLogic=('force_endturn', '_gameplay_options'),
)
class ClueGame:
    def __init__(self, suspect_type, weapon_type, room_type, board_type, die_count: int = 2) -> None:
        self.suspect_type, self.weapon_type, self.room_type = suspect_type, weapon_type, room_type

        self.suspects = copy.deepcopy(list(self.suspect_type))
        self.weapons = copy.deepcopy(list(self.weapon_type))
        self.rooms = copy.deepcopy(list(self.room_type))

        if self.room_type == MasterRoom:
            self.rooms.remove(MasterRoom.CLOAK)
        all_cards = (self.suspects, self.weapons, self.rooms)
        for l in all_cards:
            random.shuffle(l)
        self.www = [ClueWWW(s, w, r) for s, w, r in zip(*all_cards)]  # who what where
        self.answer = random.choice(self.www)
        self._option_gen = self._gameplay_options()
        self.next_options = self._option_gen.send(None)
        self.die_count = die_count
        self.board_type = board_type

    def start(
        self, players: Dict[Suspect | MasterSuspect, Any], turn_order: str = 'standard', random_locs: bool = False
    ) -> bool:
        player_cards = list(itertools.chain(self.suspects, self.weapons, self.rooms))
        for ce in self.answer:
            player_cards.remove(ce)
        random.shuffle(player_cards)

        # thanks stackoverflow, forgot about step splicing. what an elegant solution!
        player_cards_chunked = [player_cards[i :: len(players)] for i in range(len(players))]
        if len(player_cards) % len(players):
            random.shuffle(
                player_cards_chunked
            )  # so on uneven chunks, the bigger chunks aren't always frontloaded to the first players in turn order

        order = players.keys() if turn_order.lower() == 'signup' else list(self.suspect_type)
        if turn_order.lower() == 'random':
            random.shuffle(order)

        self.players = deque(
            (CluePlayer(players[s], s, tuple(c)) for s, c in zip((s for s in order if s in players), player_cards_chunked)),
            maxlen=len(players),
        )
        self.board = self.board_type({cp.suspect for cp in self.players}, random_locs)
        self.gameover = set()
        self.accuse_count = 0
        self.last_suggest_room = {}

    def even(self) -> bool:
        # https://docs.python.org/3/library/itertools.html#itertools-recipes
        g = itertools.groupby(len(p.cards) for p in self.players)
        return next(g, True) and not next(g, False)

    def roll(self) -> int:  # generalize this for now
        self.cur_roll = sum(random.randint(1, 6) for i in range(0, self.die_count))
        return self.cur_roll

    def move(self, *moves: Move) -> Optional[Room | MasterRoom]:
        return self.board.move(self.cur_player.suspect, self.board.leftover_roll or self.cur_roll, moves)

    def snoop(self, suspect: Suspect | MasterSuspect) -> ClueCard:
        try:
            player = [cp for cp in self.players if cp.suspect == suspect][0]
        except:
            raise ValueError(f'{suspect} is not in this game.')
        assert player != self.cur_player, 'Cannot snoop yourself'
        return player, random.choice(player.cards)

    def secret(self) -> Room | MasterRoom:
        return self.board.move(self.cur_player.suspect, 1, (Move(MoveDirection.SECRET, 0),))

    def suggest(
        self, s: Suspect | MasterSuspect, w: Weapon | MasterWeapon
    ) -> Tuple[str, CluePlayer, Optional[AbstractSet[ClueCard]]]:
        assert s in self.suspects and w in self.weapons

        r = self.board.player_positions[self.cur_player.suspect]
        if type(r) is not self.room_type:
            raise ValueError('Cannot suggest outside a room')

        if s in self.board.player_positions:
            self.board.player_positions[s] = r

        suggestion = ClueWWW(s, w, r)

        hints = [
            (c1, c2, any(c1 in www and c2 in www for www in self.www)) for c1, c2 in itertools.combinations(suggestion, 2)
        ]
        hint = random.choice(hints)
        hint_types = tuple(type(t) for t in hint[0:2])
        verb = 'did' if hint_types == (Suspect, Weapon) else 'was'
        afterverb = 'have' if verb == 'did' else 'in'
        if hint[2]:
            verb = verb.upper()
        hint_str = f"{'The ' if hint_types[0] is Weapon else ''}{hint[0]} {verb}{' ' if hint[2] else ' NOT '}{afterverb} the {hint[1]}."

        for i in range(1, len(self.players)):
            u = set(suggestion) & set(self.players[i].cards)
            if u:
                return hint_str, self.players[i], u
        return hint_str, None, None

    def accuse(self, s: Suspect | MasterSuspect, w: Weapon | MasterWeapon, r: Room | MasterRoom) -> bool:
        assert s in self.suspects and w in self.weapons and r in self.rooms

        self.accuse_count += 1

        correct = self.answer == ClueWWW(suspect=s, weapon=w, room=r)
        if not correct:
            del self.board.player_positions[self.cur_player.suspect]
            self.gameover.add(self.cur_player)
        return correct

    @property
    def cur_player(self) -> CluePlayer:
        return self.players[0]

    def board_zoomed_image(self, space_radius=7) -> Image:
        bi, t = self.board.image(suspect_return=self.cur_player.suspect)
        r = self.board.player_positions[self.cur_player.suspect]
        if type(r) is self.room_type:
            space_radius = int(space_radius * (2 if r in (MasterRoom.CARRIAGE, MasterRoom.COURTYARD) else 1.5))
        sx, sy = self.board.space_size_x, self.board.space_size_y
        xc, yc = t
        bz = bi.crop(
            (
                max(0, xc - space_radius * sx),
                max(0, yc - space_radius * sy),
                min(bi.width, xc + space_radius * sx),
                min(bi.height, yc + space_radius * sy),
            )
        )
        bi.close()
        return bz

    def endturn(self) -> None:
        self.board.leftover_roll = 0
        self.board.leftover_visited.clear()

        self.players.rotate(-1)
        while self.cur_player in self.gameover:
            self.players.rotate(-1)

    def force_endturn(self) -> None:
        self.board.leftover_roll = 0
        self.board.leftover_visited.clear()

        self.players.rotate(-1)
        while self.cur_player in self.gameover:
            self.players.rotate(-1)

    def translate(self, command: str) -> Tuple[Callable, Tuple[Any]]:
        args = command.split()
        action = args.pop(0)

        if action == 'endturn':
            return self.endturn, ()
        elif action == 'roll':
            return self.roll, ()
        elif action == 'snoop':
            return self.snoop, (self.suspect_type[args[0].upper()],)
        elif action == 'secret':
            return self.secret, ()
        elif action == 'move':
            m = []

            r = self.board.player_positions[self.cur_player.suspect]

            # on one-door rooms, automatically start with "door a" if not given.
            if type(r) is self.room_type and not re.fullmatch('DOOR', args[0], re.I) and len(self.board.door_map[r]) == 1:
                m.append(Move(MoveDirection.DOOR, 0))

            i = 0
            while i < len(args):
                md = MoveDirection[args[i].upper()]
                i += 1
                if md is not MoveDirection.SECRET:
                    l = args[i]
                    if md is MoveDirection.DOOR:
                        try:
                            l = ord(l.upper()) - ord('A')
                            i += 1
                        except TypeError:
                            l = 0
                    else:
                        i += 1
                else:
                    l = 0
                m.append(Move(md, int(l)))
            return self.move, m
        elif action == 'suggest':
            return self.suggest, (self.suspect_type[args[0].upper()], self.weapon_type[args[1].upper()])
        elif action == 'accuse':
            return self.accuse, (
                self.suspect_type[args[0].upper()],
                self.weapon_type[args[1].upper()],
                self.room_type[args[2].upper()],
            )
        else:
            raise ValueError('Improper command')

    # used in conjunction with limitCallsTo. enforces gameplay logic.
    def _gameplay_options(self) -> Generator[Tuple[str], str, None]:
        yield ('start,')

        while True:
            options = ['roll', 'accuse', 'endturn']
            r = self.board.player_positions[self.cur_player.suspect]
            if type(r) is self.room_type:
                if r != MasterRoom.CLOAK and r is not self.last_suggest_room.get(self.cur_player):
                    options.insert(1, 'suggest')
                if r in self.board.secret_map:
                    options.insert(1, 'secret')
                if self.board.isBlocked(r):
                    options.remove('roll')

            choice = yield tuple(options)

            if choice == 'accuse':
                if self.cur_player in self.gameover and self.accuse_count < len(self.players):
                    yield ('endturn',)
                    continue
                else:
                    yield tuple()  # game over
            elif choice == 'endturn':
                continue
            elif choice != 'suggest':
                self.last_suggest_room.pop(self.cur_player, None)
                if choice == 'roll':
                    choice = yield ('move',)
                    # get in the case of wrong accusation removing player
                    while type(pos := self.board.player_positions.get(self.cur_player.suspect)) is tuple:
                        x, y = pos
                        if self.board.board[x][y] == 'X':
                            yield ('snoop',)
                            if self.board.leftover_roll:
                                choice = yield ('move', 'accuse')
                            else:
                                choice = yield ('accuse', 'endturn')
                                break
                        else:
                            break
                    if choice == 'accuse':
                        if self.cur_player in self.gameover and self.accuse_count < len(self.players):
                            yield ('endturn',)
                            continue
                        else:
                            yield tuple()  # game over
                    elif choice == 'endturn' or type(pos) is not self.room_type:
                        continue
                if self.board.player_positions[self.cur_player.suspect] != MasterRoom.CLOAK:
                    yield ('suggest',)

            self.last_suggest_room[self.cur_player] = self.board.player_positions[self.cur_player.suspect]

            choice = yield ('accuse', 'endturn')
            if choice == 'accuse':
                if self.cur_player in self.gameover and self.accuse_count < len(self.players):
                    yield ('endturn',)
                else:
                    yield tuple()  # game over


@lru_cache(maxsize=10)
def master_piece_image(s: MasterSuspect) -> Image:
    pieces_image = Image.open(f'clue_images{os.sep}clue_pieces.png')
    w, h = pieces_image.width / 3, pieces_image.height / 2

    im = pieces_image.crop((0, 0, w, h))
    data = np.array(im)
    red, green, blue, alpha = data.T

    colored_areas = (alpha == 255) & ((red > 10) | (green > 10) | (blue > 10))
    data[..., :-1][colored_areas.T] = (s.color >> 16, (s.color & 0x00FF00) >> 8, s.color & 0xFF)

    return Image.fromarray(data)


class MasterClueBoard(ClueBoard):
    def __init__(self, players: AbstractSet[MasterSuspect], random_turn_order: bool = False) -> None:
        super().__init__(
            MasterRoom,
            MasterSuspect,
            master_piece_image,
            'clue_masterboard.txt',
            {s: MasterRoom.CLOAK for s in list(MasterSuspect) if s in players},
            f'clue_images{os.sep}clue_masterboard.jpg',
            (50, 42, 28.375, 28.0625),
            secret_pairs={
                MasterRoom.CONSERVATORY: MasterRoom.DRAWING,
                MasterRoom.KITCHEN: MasterRoom.LIBRARY,
                (12, 14): (12, 37),
            },
            entrance_exceptions=[(MasterRoom.KITCHEN, MoveDirection.UP), (MasterRoom.CONSERVATORY, MoveDirection.DOWN)],
            full_block=False,
            allow_combo_rolls=True,
        )


if __name__ == '__main__':
    ct = MasterClueText()
    for w in MasterWeapon:
        for s in ct.generate((MasterSuspect.PEACH, w, MasterRoom.GAZEBO), MasterSuspect.MUSTARD):
            print(s)
        print()

    quit()

    p = {
        Suspect.PLUM: 'Wayo',
        Suspect.PEACOCK: 'jj',
        Suspect.SCARLET: 'Jess',
        Suspect.GREEN: 'Torgo',
        Suspect.WHITE: 'Kev',
        Suspect.MUSTARD: 'Guint',
    }
    cg = ClueGame()
    cg.start(players=p)
    print(cg.even())
    # cg.board.player_positions[Suspect.GREEN] = (4,6)
    # cg.board.player_positions[Suspect.WHITE] = (15,6)
    # cg.board.player_positions[Suspect.MUSTARD] = cg.answer.room

    print(cg.www)
    print(cg.answer)
    print(cg.endturn())
    print(cg.endturn())
    cg.roll()
    cg.cur_roll = 6
    print(cg.cur_player)
    print(cg.board.player_positions[cg.cur_player.suspect])
    m, a = cg.translate('move up 1 right 2 up 3')
    print(a)
    print(m(*a))
    print(cg.suggest(cg.answer.suspect, cg.answer.weapon))
    print(cg.next_options)
    # print(cg.roll())
    # print(cg.next_options)
    # print(cg.move(Move(MoveDirection.LEFT, cg.cur_roll)))
    # print(cg.endturn())
    print('{}\t{}'.format(cg.cur_player, cg.next_options))

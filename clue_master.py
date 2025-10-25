from clue import *
import numpy as np

from cachetools import LFUCache, cachedmethod


class ClueMasterCard(ClueCard):
    def image_size(self):
        return (40, 56)

    def x0_y0(self):
        return (16, 16)

    def gap(self):
        return (24, 40)


@unique
class MasterSuspect(ClueMasterCard, Suspect):
    SCARLET = ('Ms. Scarlet', (2, 1), 0, 0xD81840)
    MUSTARD = ('Col. Mustard', (0, 0), 1, 0xD8A038)
    WHITE = ('Mrs. White', (3, 1), 2, 0xE0E0E0)
    GREEN = ('Mr. Green', (2, 0), 3, 0x006000)
    PEACOCK = ('Mrs. Peacock', (0, 1), 4, 0x006070)
    PLUM = ('Prof. Plum', (1, 0), 5, 0x802050)
    BRUNETTE = ('M. Brunette', (3, 0), 6, 0x6D4730)
    PEACH = ('Ms. Peach', (4, 0), 7, 0xFFCC99)
    ROSE = ('Mme. Rose', (1, 1), 8, 0xEDAEC0)
    GRAY = ('Sgt. Gray', (4, 1), 9, 0x808080)

    @property
    def color(self) -> int:
        return self.value[3]

    def mosaic_image(self, mosaic, magnitude=2) -> Image:
        raise NotImplemented


class MasterWeapon(ClueMasterCard):
    KNIFE = ('Knife', (2, 0))
    REVOLVER = ('Revolver', (0, 1))
    PIPE = ('Lead Pipe', (3, 0))
    CANDLESTICK = ('Candlestick', (0, 0))
    ROPE = ('Rope', (1, 1))
    WRENCH = ('Wrench', (1, 2))
    HORSESHOE = ('Horseshoe', (1, 0))
    POISON = ('Poison', (4, 0))


class MasterRoom(ClueMasterCard):
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


@lru_cache(maxsize=10)
def master_piece_image(s: MasterSuspect) -> Image:
    pieces_image = Image.open(f'clue_images{os.sep}clue_pieces.png')
    w, h = pieces_image.width / 3, pieces_image.height / 2

    im = pieces_image.crop((0, 0, w, h))
    data = np.array(im)
    red, green, blue, alpha = data.T

    colored_areas = (alpha == 255) & ((red > 10) | (green > 10) | (blue > 10))
    data[..., :-1][colored_areas.T] = (
        s.color >> 16,
        (s.color & 0x00FF00) >> 8,
        s.color & 0xFF,
    )

    return Image.fromarray(data)


class MasterClueBoard(ClueBoard):
    def __init__(self, players: AbstractSet[MasterSuspect]) -> None:
        super().__init__(
            MasterRoom,
            MasterSuspect,
            master_piece_image,
            'clue_masterboard.txt',
            {s: MasterRoom.CLOAK for s in list(MasterSuspect) if s in players},
            f'clue_images{os.sep}clue_masterboard.jpg',
            (50, 42, 28.375, 28.1875),
            secret_pairs={
                MasterRoom.CONSERVATORY: MasterRoom.DRAWING,
                MasterRoom.KITCHEN: MasterRoom.LIBRARY,
            },
            entrance_exceptions=[
                (MasterRoom.KITCHEN, MoveDirection.UP),
                (MasterRoom.CONSERVATORY, MoveDirection.DOWN),
            ],
            full_block=False,
        )


if __name__ == '__main__':
    # ClueCard.multicard_image([MasterSuspect.PEACOCK, MasterWeapon.HORSESHOE, MasterRoom.DRAWING]).show()

    from PIL import Image

    b = MasterClueBoard(list(MasterSuspect))
    Image.alpha_composite(b.image(), b.door_help()).show()

    # for s in list(MasterSuspect):
    # get_piece_image(s).show()

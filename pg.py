import enum
import itertools
import operator
from datetime import datetime
from functools import cached_property, reduce
from typing import *

import portion as P
from cachetools.func import lfu_cache
from requests.structures import CaseInsensitiveDict

Range = NewType('Range', range)
Portion = NewType('Portion', P.Interval)

CURRENT_SEASON = 51


@enum.unique
class PG(enum.Enum):
    Accelerator = ('Accelerator', ((44,), (48,)), 'Acc')
    AddEmUp = ("Add 'em Up", ((15, 17),), 'Add', 'Addy', 'AEU')
    AnyNumber = ('Any Number', ((1, P.inf),), 'A#', 'Any' 'Any#')
    BackToXX = (
        "Back to '7X",
        ((50, P.inf),),
        'Back',
        'Backy',
        'BT7X',
        "BT'7X",
        *itertools.chain.from_iterable(
            [f'{y}', f"'{y}", f'BT{y}', f"BT'{y}", f"Back to '{y}"] for y in range(72, (datetime.now().year % 100) + 51)
        ),
    )
    BalanceGame84 = ("Balance Game '84", ((12, 14),), 'Balance84')
    BalanceGame = ('Balance Game', ((34, P.inf),), 'Balance', 'Bal')
    BargainGame = ('Bargain Game', ((8, 37), (40, P.inf)), 'Bargain', "Barker's Bargain Bar", 'Barg', 'Bar', 'BBB')
    Bonkers = ('Bonkers', ((30, P.inf),), 'Bonk', 'Bkrs')
    BonusGame = ('Bonus Game', ((1, P.inf),), 'Bonus', 'Bnus')
    BullseyeI = ('Bullseye I', ((1,),), 'BullseyeI', 'Bullseye72', 'Bull72', 'BullI', 'BullyI', 'BullsyI')
    Bullseye = ('Bullseye', ((4, P.inf),), 'Bullseye', 'Bull', 'Bully', 'Bullsy')
    Bump = ('Bump', ((14, 20),), 'Bumpy')
    BuyOrSell = ('Buy Or Sell', ((20, 36),), 'BoS', 'Buy', 'Sell')
    CardGame = ('Card Game', ((2, 40), (42, P.inf)), 'New Card Game', 'Card', 'Cardy')
    CarPong = ('Car Pong', ((44,), (48,)), 'Pong', 'Pongy')
    CheckGame = ('Check Game', ((10, 37), (41, P.inf)), 'Blank Check', 'Check', 'Checky', 'Chec')
    CheckOut = ('Check-Out', ((10, P.inf),), 'C-O', 'CO', 'Checkout', 'Out')
    ClearanceSale = ('Clearance Sale', ((27, 37),), 'Clearance', 'Sale', 'Saley')
    Cliffhangers = ('Cliff Hangers', ((4, P.inf),), 'Cliffhangers', 'Cliff', 'Cliffy', 'Clif')
    ClockGame = ('Clock Game', ((1, P.inf),), 'Clock', 'Clocky', 'Cloc')
    ComingOrGoing = ('Coming or Going', ((32, P.inf),), 'CoG', 'Coggy')
    CoverUp = ('Cover Up', ((22, P.inf),), 'Cover', 'COVR', 'CU')
    CreditCard = ('Credit Card', ((16, 37),), 'Credit')
    DangerPrice = ('Danger Price', ((4, P.inf),), 'Danger', 'Dngr')
    DiceGame = ('Dice Game', ((4, P.inf),), 'Deluxe Dice Game', 'Dice', 'Dicey', 'Deluxe')
    DoTheMath = ('Do the Math', ((42, P.inf),), 'Math', 'DTM')
    DoubleBullseye = ('Double Bullseye', ((1,),), 'DB', 'DBullseye', 'DBull', 'DBully', 'DBullsy')
    DoubleCross = ('Double Cross', ((40, P.inf),), 'Cross', 'Cros', 'DX')
    DoubleDigits = ('Double Digits', ((1,),), 'DD', 'Digits')
    DoublePrices = ('Double Prices', ((1, P.inf),), 'DP', 'Double')
    EazyAz123 = ('Eazy az 1 2 3', ((24, P.inf),), 'Eazy az 123', 'Eazy', 'Easy', '123')
    FinishLine = ('Finish Line', ((6, 7),), 'Finish')
    FivePriceTags = ('5 Price Tags', ((1, P.inf),), 'Five Price Tags', '5PT', 'FPT', 'Tags')
    FlipFlop = ('Flip Flop', ((28, P.inf),), 'Flip', 'Flippy')
    FortuneHunter = ('Fortune Hunter', ((26, 28),), 'Fortune', 'Hunter')
    FreezeFrame = ('Freeze Frame', ((23, P.inf),), 'Freeze', 'Freezy', 'Frze')
    GalleryGame = ('Gallery Game', ((19,),), 'Gallery')
    GasMoney = ('Gas Money', ((37, P.inf),), 'Gas', 'Gassy')
    GiveOrKeep = ('Give or Keep', ((1, 19),), 'GoK', 'Give', 'Keep')
    GoForASpin = ('Go For A Spin', ((44,), (48,)), 'Spin', 'Spinny')
    GoldRush = ('Gold Rush', ((44, 45), (48,)), 'Rush', 'Rushy')
    GoldenRoad = ('Golden Road', ((3, P.inf),), 'Gold', 'Road', 'Goldy', 'Roady', 'Golden', 'GR')
    GrandGame = ('Grand Game', ((8, P.inf),), 'Grand', 'Grandy', 'Grnd')
    Gridlock = ('Gridlock!', ((46, P.inf),), 'Gridlock', 'Grid', 'Griddy')
    GroceryGame = ('Grocery Game', ((1, P.inf),), 'Grocery', 'Groc')
    HalfOff = ('1/2 Off', ((32, P.inf),), 'Half', 'Halfy')
    HiLo = ('Hi Lo', ((1, P.inf),), 'H-L', 'Hi-Lo', 'HL', 'HiLo')
    HitMe = ('Hit Me', ((9, 35),), 'Hit', 'Hitty')
    HoleInOne = ('Hole in One', ((5, P.inf),), 'Hole in One or Two', 'Hole')
    HotSeat = ('Hot Seat', ((45, P.inf),), 'Seat', 'Hot', 'Seaty')
    Hurdles = ('Hurdles', ((4, 11),), 'Hurd')
    ItsInTheBag = ("It's in the Bag", ((26, P.inf),), 'Bag', 'Baggy')
    ItsOptional = ("It's Optional", ((7, 11),), 'Option', 'Optional')
    Joker = ('Joker', ((22, 35),), 'Joke')
    LetEmRoll = ("Let 'em Roll", ((28, P.inf),), 'Let em Roll', 'Roll', 'Rolly')
    LineEmUp = ('Line em Up', ((26, P.inf),), 'Line', 'LEU')
    LuckySeven = ('Lucky $even', ((1, P.inf),), 'Lucky Seven', 'L7', 'Lucky', 'Seven', '$even', '7')
    MagicNumber = ('Magic #', ((21, P.inf),), 'M#', 'Magic')
    MakeYourMark = ('Make Your Mark', ((23, 37),), "Barker's Marker$", "Barker's Markers", 'Mark', 'Marky', 'Markers')
    MakeYourMove = ('Make Your Move', ((18, P.inf),), 'Move', 'Movey', 'MYM')
    MasterKey = ('Master Key', ((11, P.inf),), 'Key')
    MoneyGame = ('Money Game', ((1, P.inf),), 'Big Money Game', 'Money', 'Mony', 'BigMoney')
    MoreOrLess = ('More or Less', ((35, P.inf),), 'MoL', 'Moley')
    MostExpensive = ('Most Expen$ive', ((1, P.inf),), 'Most Expensive', 'ME')
    MysteryPrice = ('Mystery Price', ((2,),), 'Mystery')
    NowOrThen = ('Now....or Then', ((9, P.inf),), 'Now....and Then', 'NoT', 'NaT')
    OneAway = ('One Away', ((13, P.inf),), 'OA', 'Away')
    OnTheNose = ('On the Nose', ((13, 14),), 'Nose', 'Nosey')
    OnTheSpot = ('On the Spot', ((31, 33),), 'Spot', 'Spotty')
    OneRightPrice = ('1 Right Price', ((4, P.inf),), 'ORP', '1RP')
    OneRightPrice3 = ('ORP Three Furs', ((1,),), 'ORF', 'ORPF', '1RPF', 'ORP3', '1RP3', 'ORP3F', '1RP3F')
    OneWrongPrice = (u'One Wr\u00f8ng Price', ((27, P.inf),), '1 Wrong Price', 'OWP', '1WP')
    PassTheBuck = ('Pass the Buck', ((30, P.inf),), 'Buck', 'PtB')
    Pathfinder = ('Pathfinder', ((15, P.inf),), 'Path')
    PayTheRent = ('Pay the Rent', ((39, P.inf),), 'Rent', 'PtR')
    PennyAnte = ('Penny Ante', ((7, 30),), 'Penny79', 'PennyAnte', 'Ante')
    PhoneHomeGame = ('Phone Home Game', ((12, 18),), 'Phone', 'Phoney', 'Home', 'Homey', 'Phg')
    PickANumber = ('Pick-a-Number', ((20, P.inf),), 'Pa#', 'PaN')
    PickAPair = (
        'Pick-a-Pair',
        (
            (10, 17),
            (19, P.inf),
        ),
        'Pick a Pair',
        'Pair',
        'PaP',
        'Papsmear',
        'Smear',
        'Pappy',
    )
    Plinko = ('Plinko', ((11, P.inf),), 'Dinko', 'Plnk')
    PocketChange = (u'Pocket \u00a2hange', ((33, P.inf),), 'Pocket', 'Pckt')
    PokerGame = ('Poker Game', ((4, 35),), 'Poker')
    ProfessorPrice = ('Professor Price', ((6,),), 'Professor', 'Prof')
    PunchABunch = ('Punch a Bunch', ((7, P.inf),), 'Punch', 'Punchy', 'Pnch', 'PAB')
    PushOver = ('Push Over', ((27, P.inf),), 'Push', 'Pushy')
    RaceGame = ('Race Game', ((2, P.inf),), 'Race', 'Racey')
    RangeGame = ('Range Game', ((1, P.inf),), 'Range', 'Rangey', 'Rang')
    RatRace = ('Rat Race', ((38, P.inf),), 'Rat', 'Ratty')
    SafeCrackers = ('Safe Crackers', ((4, P.inf),), 'Safe', 'Safey')
    SecretX = ('Secret "X"', ((6, P.inf),), '"X"', 'X', 'Secret')
    ShellGame = ('Shell Game', ((2, P.inf),), 'Shell', 'Shelly', 'Shel')
    ShoppingSpree = ('Shopping Spree', ((24, P.inf),), 'Spree', 'Spre')
    ShowerGame = ('Shower Game', ((7,),), 'Shower')
    SideBySide = ('Side by Side', ((22, P.inf),), 'Side', 'Sidey', 'SbS')
    SmashForCash = ('Smash for Ca$h', ((44,), (48,)), 'Smash For Cash', 'Smash', 'Smashy')
    SpellingBee = ('Spelling Bee', ((17, P.inf),), 'Bee')
    SplitDecision = ('Split Decision', ((24, 25),), 'Split', 'Splitty', 'Decision')
    SqueezePlay = ('Squeeze Play', ((6, P.inf),), 'Squeeze', 'Squeezy', 'Sqze')
    StackTheDeck = ('Stack the Deck', ((35, P.inf),), 'Stack', 'Stacky', 'Deck', 'Stac', 'STD')
    StepUp = ('Step Up', ((30, 43),), 'Step', 'Steppy')
    SuperBall = ('Super Ball!!', ((9, 26),), 'SuperBall!!', 'SuperBall', 'Ball', 'SB')
    SuperSaver = ('$uper $aver', ((17, 24),), 'Super Saver', 'Saver', 'Savery')
    SwapMeet = ('Swap Meet', ((20, P.inf),), 'Swap', 'Swappy')
    Switch = ('Switch?', ((20, P.inf),), 'Switch?', 'Switch', 'S?', 'Sw?')
    Switcheroo = ('Switcheroo', ((5, P.inf),), 'Switcheroo', 'Roo')
    TakeTwo = ('Take Two', ((6, P.inf),), 'T2')
    TelephoneGame = ('Telephone Game', ((7,),), 'Telephone')
    Temptation = ('Temptation', ((1, P.inf),), 'Temptation', 'Tempt', 'Tempty', 'TMPT')
    TenChances = ('10 Chances', ((3, P.inf),), 'Ten Chances', '10C')
    ThatsTooMuch = ("That's Too Much!", ((29, P.inf),), 'TTM')
    ThreeStrikes = ('3 Strikes', ((4, P.inf),), '3 Strikes +', 'Strikes', '3X', '3X+')
    TimeIsMoney03 = ("Time Is Money '03", ((32,),), 'Time Is Money', 'Time03', 'TiM03')
    TimeIsMoney = ('Time I$ M\u00f8ney', ((43, P.inf),), 'Time', 'TiM')
    ToThePenny = ("To The Penny", ((50, P.inf),), 'Penny', 'Peny', 'TTP')
    TraderBob = ('Trader Bob', ((8, 14),), 'Trader', 'Trade', 'Tradey')
    TriplePlay = ('Triple Play', ((29, P.inf),), 'Triple', 'TP')
    TwoForThePriceOfOne = ('2 for the Price of 1', ((18, P.inf),), '241', '2for1', 'twoforone', 'twofor1')
    VendOPrice = ('Vend-O-Price', ((44, P.inf),), 'Vend', 'Vendy')
    WalkOfFame = ('Walk of Fame', ((12, 14),), 'Walk', 'Walky', 'Fame', 'Famey', 'WOF')
    _UNKNOWN = ('??????????', tuple(), 'UNKNOWN')

    def __init__(self, sheetName, activeSeasons, *altNames):
        self.sheetName = sheetName
        self.altNames = altNames
        self.activeSeasons = reduce(
            operator.or_,
            [P.singleton(*sRange) if len(sRange) == 1 else P.closed(*sRange) for sRange in activeSeasons],
            P.empty(),
        )

    @cached_property
    def sheet_abbr(self):
        for a in self.altNames:
            if len(a) <= 3:
                return a
        return self.sheetName[:3]

    @cached_property
    def greco_abbr(self):
        for a in self.altNames:
            if len(a) <= 4:
                return a.upper()
        return self.sheetName[:4].upper()

    def activeIn(self, r: Union[Range, Portion, int]):
        return self.activeSeasons.overlaps(
            P.closedopen(r.start, r.stop) if type(r) == range else P.singleton(r) if type(r) == int else r
        )

    def fullyActiveIn(self, r: Union[Range, Portion]):
        return (P.closedopen(r.start, r.stop) if type(r) == range else r) in self.activeSeasons

    @property
    def retired(self):
        return self.activeSeasons.upper != P.inf and CURRENT_SEASON not in self.activeSeasons

    @property
    def firstSeason(self):
        return self.activeSeasons.lower

    @property
    def lastSeason(self):
        return self.activeSeasons.upper

    def __str__(self):
        return self.sheetName

    def __repr__(self):
        return f'<PG.{self.sheetName}>'

    def __reduce_ex__(self, proto):
        return f'{self.__class__.__name__}.{self.name}'

    @classmethod
    def lookup(cls, n):
        return PG.lookup_table[n]


# START LOOKUP TABLE SETUP

PG.lookup_table = CaseInsensitiveDict()
for e in PG:
    PG.lookup_table[e.sheetName] = e
    PG.lookup_table[e.greco_abbr] = e
    for a in e.altNames:
        PG.lookup_table[a] = e

PG.partition_lookup = CaseInsensitiveDict()
from bidict import bidict

PG.partition_table = bidict()


def _build_table_entry(link_id, lookup_ids, pgs):
    for e in lookup_ids:
        PG.partition_lookup[e] = link_id
    PG.partition_table[link_id] = pgs


def _pg_strs_convert(pg_strs):
    return frozenset([PG.lookup_table[pg] for pg in pg_strs.split(' ')])


_build_table_entry('any game', [], frozenset(PG) - frozenset((PG._UNKNOWN,)))
_build_table_entry(
    'SP/CAR',
    ('car/sp', 'sp/car', 'car&sp', 'sp&car', 'carsp', 'spcar'),
    frozenset(
        (
            PG.DoubleDigits,
            PG.FivePriceTags,
            PG.MasterKey,
            PG.OnTheSpot,
            PG.Pathfinder,
            PG.RatRace,
            PG.SpellingBee,
            PG.Switcheroo,
        )
    ),
)
_build_table_entry(
    'GP/CAR',
    ('car/gp', 'gp/car', 'car&gp', 'gp&car', 'cargp', 'gpcar'),
    frozenset((PG.HoleInOne, PG.LetEmRoll, PG.PassTheBuck, PG.StackTheDeck, PG.TelephoneGame)),
)
_build_table_entry(
    'SP/CASH',
    ('cash/sp', 'sp/cash', 'cash&sp', 'sp&cash', 'spcash', 'cashsp'),
    frozenset((PG.HalfOff, PG.HotSeat, PG.Plinko, PG.PunchABunch)),
)
_build_table_entry(
    'GP/CASH',
    ('cash/gp', 'gp/cash', 'cash&gp', 'gp&cash', 'gpcash', 'cashgp'),
    frozenset((PG.GrandGame, PG.ItsInTheBag, PG.PayTheRent, PG.PhoneHomeGame, PG.TimeIsMoney, PG.ToThePenny)),
)
_build_table_entry(
    'REG. SP',
    ('sp/reg', 'sp/regular', 'regular/sp', 'reg/sp', 'regularsp', 'spregular', 'regsp', 'spreg'),
    frozenset(
        (
            PG.BackToXX,
            PG.BalanceGame84,
            PG.BonusGame,
            PG.Cliffhangers,
            PG.FinishLine,
            PG.GiveOrKeep,
            PG.Joker,
            PG.MysteryPrice,
            PG.SecretX,
            PG.ShellGame,
            PG.SuperBall,
            PG.TraderBob,
        )
    ),
)
_build_table_entry(
    'SP',
    ('sp', 'allsp', 'spall', 'all/sp', 'sp/all'),
    PG.partition_table['REG. SP'] | PG.partition_table['SP/CAR'] | PG.partition_table['SP/CASH'],
)
_build_table_entry(
    'REG. GP',
    ('gp/reg', 'gp/regular', 'regular/gp', 'reg/gp', 'regulargp', 'gpregular', 'reggp', 'gpreg'),
    frozenset(
        (
            PG.Bullseye,
            PG.CheckOut,
            PG.GroceryGame,
            PG.HiLo,
            PG.HitMe,
            PG.Hurdles,
            PG.NowOrThen,
            PG.PennyAnte,
            PG.PickAPair,
            PG.SuperSaver,
            PG.TimeIsMoney03,
            PG.VendOPrice,
        )
    ),
)
_build_table_entry(
    'GP',
    ('gp', 'allgp', 'gpall', 'all/gp', 'gp/all'),
    PG.partition_table['REG. GP'] | PG.partition_table['GP/CAR'] | PG.partition_table['GP/CASH'],
)
_build_table_entry(
    'CAR+',
    ('car+', 'carplus', 'carother'),
    frozenset(
        (
            PG.AnyNumber,
            PG.GoldenRoad,
            PG.LineEmUp,
            PG.MasterKey,
            PG.MoreOrLess,
            PG.SplitDecision,
            PG.RatRace,
            PG.TelephoneGame,
            PG.Temptation,
            PG.TenChances,
        )
    ),
)
_build_table_entry(
    'REG. CAR',
    ('regcar', 'reg/car', 'carreg', 'car/reg'),
    frozenset(
        (
            PG.AddEmUp,
            PG.BullseyeI,
            PG.CardGame,
            PG.CoverUp,
            PG.DiceGame,
            PG.DoubleBullseye,
            PG.GasMoney,
            PG.Gridlock,
            PG.ItsOptional,
            PG.LuckySeven,
            PG.MoneyGame,
            PG.OnTheNose,
            PG.OneAway,
            PG.PocketChange,
            PG.ProfessorPrice,
            PG.ShowerGame,
            PG.ThatsTooMuch,
            PG.ThreeStrikes,
            PG.TriplePlay,
        )
    ),
)
_build_table_entry(
    '4+ PRIZER',
    ('4+p', '4+prize', '4+prizer'),
    frozenset(
        (
            PG.CreditCard,
            PG.DangerPrice,
            PG.FortuneHunter,
            PG.PokerGame,
            PG.RaceGame,
            PG.ShoppingSpree,
            PG.StepUp,
            PG.SwapMeet,
            PG.TakeTwo,
            PG.WalkOfFame,
        )
    ),
)
_build_table_entry(
    '1+ PRIZER',
    ('1+p', '1p+', '1+prize', '1prize+', '1+prizer', '1prizer+'),
    frozenset((PG.ClockGame, PG.MakeYourMove, PG.SafeCrackers, PG.TwoForThePriceOfOne)),
)
_build_table_entry(
    '4 PRIZER', ('4p', '4prize', '4prizer'), PG.partition_table['4+ PRIZER'] - frozenset((PG.CreditCard, PG.FortuneHunter))
)
_build_table_entry(
    '3 PRIZER',
    ('3p', '3prize', '3prizer'),
    frozenset(
        (
            PG.BuyOrSell,
            PG.ClearanceSale,
            PG.EazyAz123,
            PG.MakeYourMark,
            PG.MostExpensive,
            PG.OneWrongPrice,
            PG.OneRightPrice3,
        )
    ),
)
_build_table_entry(
    '2 PRIZER',
    ('2p', '2prize', '2prizer'),
    frozenset((PG.BargainGame, PG.Bump, PG.DoTheMath, PG.DoubleCross, PG.MagicNumber, PG.OneRightPrice, PG.Switch)),
)
_build_table_entry(
    'MULTIPRIZER',
    ('mp', 'multiprize', 'multiprizer', 'multi-prizer'),
    PG.partition_table['1+ PRIZER']
    | PG.partition_table['2 PRIZER']
    | PG.partition_table['3 PRIZER']
    | PG.partition_table['4+ PRIZER'],
)
_build_table_entry(
    '2+ PRIZER',
    ('2+p', '2+prize', '2+prizer'),
    PG.partition_table['2 PRIZER'] | PG.partition_table['3 PRIZER'] | PG.partition_table['4+ PRIZER'],
)
_build_table_entry(
    '1 PRIZER',
    ('1p', '1prize', '1prizer'),
    frozenset(
        (
            PG.BalanceGame,
            PG.Bonkers,
            PG.CheckGame,
            PG.ComingOrGoing,
            PG.DoublePrices,
            PG.FlipFlop,
            PG.FreezeFrame,
            PG.GalleryGame,
            PG.PickANumber,
            PG.PushOver,
            PG.RangeGame,
            PG.SideBySide,
            PG.SqueezePlay,
        )
    ),
)
_build_table_entry(
    'CAR',
    ('car', 'allcar', 'carall', 'car/all', 'all/car'),
    PG.partition_table['CAR+']
    | PG.partition_table['REG. CAR']
    | PG.partition_table['SP/CAR']
    | PG.partition_table['GP/CAR'],
)
_build_table_entry(
    'CASH',
    ('cash', 'allcash', 'cashall', 'cash/all', 'all/cash'),
    PG.partition_table['SP/CASH'] | PG.partition_table['GP/CASH'] | frozenset((PG.FortuneHunter,)),
)
_build_table_entry(
    'FEE', ('fee', 'allfee', 'feeall', 'fee/all', 'all/fee'), PG.partition_table['SP'] | PG.partition_table['GP']
)
_build_table_entry(
    'NON-CAR',
    ('non-car', 'noncar'),
    (PG.partition_table['FEE'] | PG.partition_table['1 PRIZER'] | PG.partition_table['1+ PRIZER'])
    - frozenset((PG.Bonkers, PG.CheckGame, PG.FlipFlop, PG.SideBySide, PG.ComingOrGoing, PG.MakeYourMove)),
)
_build_table_entry(
    'NON-FEE',
    ('notfee', 'nofee', 'not-fee', 'nonfee', 'non-fee'),
    PG.partition_table['any game'] - PG.partition_table['FEE'],
)
_build_table_entry(
    'CAR/FEE', ('carfee', 'feecar', 'car/fee', 'fee/car'), PG.partition_table['SP/CAR'] | PG.partition_table['GP/CAR']
)
_build_table_entry(
    'REG/FEE', ('regfee', 'feereg', 'reg/fee', 'fee/reg'), PG.partition_table['REG. SP'] | PG.partition_table['REG. GP']
)
_build_table_entry('BIG3', ('big3', 'bigthree'), frozenset((PG.GoldenRoad, PG.ThreeStrikes, PG.TriplePlay)))
_build_table_entry(
    'DEAL',
    ('lmad', 'deal', 'letsmakeadeal'),
    frozenset((PG.Accelerator, PG.CarPong, PG.GoForASpin, PG.GoldRush, PG.SmashForCash)),
)
_build_table_entry(
    'RETIRED',
    ('retire', 'retired'),
    frozenset([pg for pg in list(PG) if pg.retired and not pg.activeSeasons.empty and pg not in PG.partition_table['DEAL']]),
)
_build_table_entry(
    'ACTIVE', ('active',), PG.partition_table['any game'] - PG.partition_table['RETIRED'] - PG.partition_table['DEAL']
)

_build_table_entry(
    'TURNTABLE',
    ('turntable', 'tt'),
    _pg_strs_convert(
        'a# bonus bullseye72 bullseye bump card check clock cog cover db digits flip give grocery joker mym key money mystery not push shell smash bee split squeeze saver roo vend'
    ),
)
_build_table_entry(
    'GPT',
    ('gpt',),
    _pg_strs_convert('add back c-o dice freeze grand h-l bag markers oa pa# pap penny pocket professor punch x trader walk'),
)
_build_table_entry('RGC', ('rgc',), _pg_strs_convert('buy mol race t2 time'))
_build_table_entry(
    'DOOR2',
    ('door2', 'doortwo'),
    _pg_strs_convert(
        'bargain clearance credit danger dp eazy 5pt fortune gallery rush half m# me orp orp3 owp rent safe spree side step swap switch 241'
    ),
)
_build_table_entry(
    'HOMEBASE',
    ('homebase', 'nowhere'),
    _pg_strs_convert(
        'balance84 balance bonkers pong cliff cross finish gas gold spin grid hit hole hot hurdles optional roll line l7 spot buck path ante phone plinko poker rat shower stack superball telephone temptation 10c ttm 3x time03 tp'
    ),
)
_build_table_entry('MIDDLE', ('middle',), _pg_strs_convert('range nose'))
_build_table_entry(
    'NO_OPENING_ACT',
    (),
    _pg_strs_convert('cross hole buck seat grid mol race t2 time rat path stack balance check magic line grid key'),
)
_build_table_entry('NO_FIRST', (), _pg_strs_convert('gas 10c time03 trader spot card'))
_build_table_entry('BAILOUT', ('bailout',), _pg_strs_convert('gas bag buck rent seat bee tempt step penny punch roll'))

PG.partition_table['CAR_BOATABLE'] = frozenset(
    (PG.MoneyGame, PG.GoldenRoad, PG.BullseyeI, PG.DoubleBullseye, PG.AnyNumber, PG.FivePriceTags, PG.LuckySeven)
)

# END LOOKUP TABLE SETUP

# couple more constants
MAX_PG_NAME_LEN = max([len(str(pg)) for pg in list(PG)])
PG_WILDCARD = PG.partition_table['any game']

from dataclasses import dataclass

# namedtuple was not so compatible with pandas, so dataclass it is
from util import PLAYING_FLAGS


@dataclass(eq=False)
class PGPlaying:
    pg_str: str
    flag: int
    pg: PG = None

    def __post_init__(self):
        if not self.pg:
            self.pg = PG.lookup_table[self.pg_str]
        # if self.pg == PG.PickAPair:
        #     self.pg_str = self.pg_str.replace('A', 'a')
        # elif "'S" in self.pg_str:
        #     self.pg_str = self.pg_str.replace("'S", "'s")
        # elif "And" in self.pg_str:
        #     self.pg_str = self.pg_str.replace("And", "and")
        # elif 'In' in self.pg_str and 'Or' in self.pg_str:
        #     self.pg_str = self.pg_str.replace("In", "in").replace("Or", "or")

    @lfu_cache(maxsize=int(len(PG) * len(PLAYING_FLAGS) / 4))
    def __str__(self):
        s = self.pg_str + (
            (' ({})'.format(''.join(pf if int(b) else '' for pf, b in zip(PLAYING_FLAGS, '{:010b}'.format(self.flag)))))
            if self.flag
            else ''
        )
        return s.replace('car', 'boat') if self.flag & 0b1 and self.pg in PG.partition_table['CAR_BOATABLE'] else s

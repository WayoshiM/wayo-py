# COMMAND OPTIONS

At the end of each parameter list for many commands, varying options can be set in a **key=value** format, in any order. In general coding terms these are usually called keyword-only arguments.

The help command shows the command signature and exactly what options are available per command, including all available aliases. For example:

	!help p c
    ![played|playings|playing|play|p] [concurrence|conflict|c] <pg1> <pg2> [pg3] [pg4] [pg5] [pg6]
    ([byseason|by]=0) ([showlineup|show]=False) ([sortby|sort]=prod) (since=False)
    ([excludeeducated|exclude]=False) ([pgflags|flags|f])
    ([time|version]=daytime) (start=46) (end=50) ([dateformat|format]=%m/%d/%y)

First, the help was invoked with a shorthand **p c**. Any command/subcommand can be shortened with any of the aliases, such as the above.

Two pricing games are required for concurrence, as specified by the <> arguments. But up to six are allowed (pg3-6, in [], are optional). The parenthesised () parameters afterwards are the prealluded to options, that may come in any order after all the other <> and [] arguments.

For search, each condition must start with "condition=" (can be shortened to "cond="). This is more verbose than before, but also more readable and flexible.

# TRUE/FALSE SPECIFICATION

To specify a true under the discord library I'm using (discord.py), use one of the following words (case-insensitive): yes, y, true, t, 1, enable, on. For false: no, n, false, f, 0, disable, off.

# WHAT DATASET IS USED

This covers the meaning of the **time**, **start** and **end** keyword-only options.

Start and end are most often natural numbers denoting the season number.

By default, the standard daytime dataset (S1-50) is used. If "prime" or "primetime" is submitted as the **time** parameter, the dataset will be all primetime episodes (no seasons here) instead. There are also syndicated episodes (S1-8,23).

If daytime, data is gathered from the premiere of season **start** to the finale of season **end** (or today if the current season), inclusive. Defaults for the season range are according to Guint's convention: the current season and the four most recent completed seasons. For a single season, simply input N1=N2.

For the search command, the entire daytime history is the default (S1-).

Start and end can also be dates, formatted by the **dateFormat** parameter (default m/d/yy). The date endpoints are inclusive at both ends, similar to season numbers. If only one of the two are dates, the other is converted automatically.

Depending on inactive seasons for the given game(s), the season range may automatically be reduced (only in the case of season numbers given, not dates).

# BY SEASON

For the **concurrence** and **slots** commands, you can specify a non-zero number, the bySeason option, of seasons to "break up" the results by. For example:

    !played slots L7 by=6 start=1 end=48
    Lucky $even, S1-48 (6):

            PG1  PG2  PG3  PG4  PG5  PG6  PG^  PG? |  ALL
    S1-6     54    4    0    0    0    0  161    4 |  223
    S7-12   106    5    1    0    0    0   90    1 |  203
    S13-18   61   12    3    1   23   17   63    0 |  180
    S19-24   56    7   19    0   41   74    0    0 |  197
    S25-30  126    7    7    0   13   31    0    0 |  184
    S31-36   85   32   12    2    9    3    0    0 |  143
    S37-42   54   49   40    6   26   25    0    0 |  200
    S43-48   44   30   29   11   31   27    0    0 |  172
    -----------------------------------------------|-----
    S1-48   586  146  111   20  143  177  314    5 | 1502

This is not compatible with the case of date start/end points.

# PGGROUPS IN SLOTS

For **slots**, you can specify a PGGroup to give one big sum, with bySeason or without:

    !played slots big3 gold triple 3x start=36 end=49 by=7
    BIG3, S36-49 (7):

            PG1  PG2  PG3  PG4  PG5  PG6 | ALL
    S36-42   61   10    3    0    1    2 |  77
    S43-49   43    8   15    1    6    3 |  76
    -------------------------------------|----
    S36-49  104   18   18    1    7    5 | 153

    ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

    Golden Road, S36-49 (7):

            PG1  PG2  PG3  PG4  PG5  PG6 | ALL
    S36-42   26    0    0    0    0    0 |  26
    S43-49   25    1    1    0    0    0 |  27
    -------------------------------------|----
    S36-49   51    1    1    0    0    0 |  53

    ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

    Triple Play, S36-49 (7):

            PG1  PG2  PG3  PG4  PG5  PG6 | ALL
    S36-42   20    0    0    0    0    1 |  21
    S43-49   11    5    6    0    0    1 |  23
    -------------------------------------|----
    S36-49   31    5    6    0    0    2 |  44

    ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

    3 Strikes, S36-49 (7):

            PG1  PG2  PG3  PG4  PG5  PG6 | ALL
    S36-42   15   10    3    0    1    1 |  30
    S43-49    7    2    8    1    6    2 |  26
    -------------------------------------|----
    S36-49   22   12   11    1    7    3 |  56

By specifying onlyPGs (only) as True, all PGGroups are considered as a listing of single PGs instead, for shorthand:

    !played slots 3p start=1 end=49
    3 PRIZER, S1-49: 486, 357, 482, 830, 140, 209 | 423^, 4? | 2931

    !played slots 3p start=1 end=49 only=T
    Buy Or Sell, S20-36: 0, 17, 55, 74, 3, 6 | 155
    Clearance Sale, S27-37: 44, 8, 23, 18, 3, 3 | 99
    Eazy az 1 2 3, S24-49: 113, 61, 60, 92, 18, 21 | 365
    Make Your Mark, S23-37: 1, 13, 26, 74, 7, 4 | 125
    Most Expen$ive, S1-49: 230, 161, 235, 429, 68, 117 | 423^, 4? | 1667
    ORP Three Furs was not played in S1.
    One Wrøng Price, S27-49: 98, 97, 83, 143, 41, 58 | 520

# SHOWING LINEUPS

In the **search** command, all matching lineups are printed automatically. If the result is over the character limit for one message (2000), a downloadable text file will be generated (that is previewable within the first 100 lines, effectively making the character limit much greater - thanks for that update in early 2021, Discord!).

In the **concurrence** command, you have the option to print all the lineups that match with the showLineup option. By default, it is false.

    !p c check 241 show=y
    Check Game, 2 for the Price of 1, S46-50: 3

            S      AIRDATE    INT. DATE                   PG1            PG2                   PG3                   PG4         PG5           PG6
    8523K  47  Jun 24 2019  Dec 05 2018  2 for the Price of 1     Money Game            Check Game              One Away  Grand Game  Squeeze Play
    9343K  49  Mar 10 2021  Mar 10 2021          Let 'em Roll  Double Prices              Take Two  2 for the Price of 1  Check Game    Master Key
    9401K  49  Apr 20 2021  Apr 19 2021          Vend-O-Price      Gas Money  2 for the Price of 1             Flip Flop    Rat Race    Check Game

## SORT AND SINCE

Going further for these two commands are two more options:

You can sort the resulting lineups by date with **sort=d** or **sort=date** (case-insensitive), instead of the default, in-production-order listing (**sort=p** or **sort=prod**).

By specifying **since=True**, an additional column will appear to the right of AIRDATE when **sort=date** (only on daytime & primetime) showing the number of days since the previous line's lineup. When **sort=prod**, this column will be to the right of the production number and show the number of shows, based on production show ordering (see Valid Production Codes section below), since.

    7 lineups found in DAYTIME for all of
    * SEASON is 50
    * Shell Game played in any slot

    PROD    S      AIRDATE    SINCE    INT. DATE              PG1              PG2                   PG3               PG4               PG5            PG6
    9543K   50  Oct 13 2021           Oct 13 2021      Lucky $even    1 Right Price            Shell Game         Gas Money         Swap Meet   Vend-O-Price
    9592K   50  Nov 02 2021  20 days  Nov 16 2021         One Away  Coming or Going  2 for the Price of 1  That's Too Much!   One Wrøng Price     Shell Game
    9665K   50  Jan 07 2022  66 days  Jan 07 2022    1 Right Price        Card Game            Shell Game         Flip Flop        Clock Game  Pass the Buck
    9683K   50  Jan 19 2022  12 days  Jan 19 2022         Cover Up      Do the Math            Shell Game      Squeeze Play  That's Too Much!     Grand Game
    9742K   50  Mar 01 2022  41 days  Mar 01 2022      Lucky $even    Eazy az 1 2 3               Bonkers     Pass the Buck         Push Over     Shell Game
    9814K   50  Apr 21 2022  51 days  Apr 21 2022  One Wrøng Price        Card Game          Grocery Game         Gas Money        Shell Game  Double Prices
    9861K   50  May 23 2022  32 days  May 23 2022      Lucky $even        Flip Flop            Grand Game     1 Right Price        Shell Game     Line em Up

## DATES AND SEASONS

The dataset includes the intended airdate (based off of the production code) and the actual airdate. If all the intended and actual dates match in a certain output, just one column of dates will be shown, deleting the extraneous information.

If only one season is specified for the dataset, the season column will be removed.

The dataset includes notes on special shows. If there are no notes given for all shows in a certain output, the notes column will be removed.

# VALID PRODUCTION CODES

[G-R FAQ on Codes](http://www.golden-road.net/gr_faq/index.php?title=General_Questions#How_do_I_read_an_episode.27s_production_number.3F)

To date, a daytime production number will be a four-digit number (zero padding included), starting with 0011 and ending in 1-5, followed by a 'D' or 'K'. Essentially, the show began on 0011D, and once they got to 9995D they "reset" to 0011K. Therefore 0011K is essentially 10011D, and so on.

A primetime production number is one of the following: 001-006P, 0001S, 0001LV, 001-040SP (except for 39).

There are a handful of unaired episodes with odd production codes.

Malformed production codes will automatically be thrown out. All letters will be automatically transformed to uppercase.

# FLAGS

A particular playing of a PG can have one of these flags:

- **C**: non-car for a non-restored car (NCFAC), or a car game for a boat. Shows up as __car__ or __boat__ in output.
- **T**: 3+ multiprizer for trips, or in the one instance of the all-Plinko show, Plinko for a trip.
- **&**: any playing for 2 or more cars that isn't It's Optional or Triple Play. Shows up as __cars__ in output.
- **\***: A playing with rule changes just for that playing (such as a Big Money Week version of a casher, or 35th Anniversary Plinko), a LMAD Mashup game, or in two syndicated cases, a "vintage car reproduction". Note for MDS shows, an increase on the top prize on cashers was so common that these instances are not denoted with this flag.
- **@**: A playing for a restored car, or a 4-prizer played for LA sports season tickets.
- **R**: A playing for really unusual prize(s).
- **$**: Mainly a non-cash game played for cash, hence the use of the dollar sign here. In a couple instances instead, a car game for a trailer, or in the one instance of the all-Plinko show, Plinko for 2 regular prizes.
- **^**: The slotting of the game is uncertain. Some old records are incomplete and slotted by best guess.
- **?**: The identity of the game is uncertain. Some old records are incomplete and this is a best guess on what the game was. Often it is known which set of two or three games occurred within a small subset of shows, with no further certainty.
- **M**: This was the Million Dollar Game in a Drew MDS (primetime) in 2008. Shows up as __MDG__ in output.

A select couple of playings have two flags: there were two NCFAC playings that were MDGs, and one playing of "drop the ball" (rule change) Range for a car in 2021.

To give a further example on the ^ and ? flags, this was the listing for 4841D and 4842D prior to Pluto TV's airings on Feb 3rd, 2021:

            S      AIRDATE                 PG1              PG2              PG3                       PG4                  PG5              PG6
    4841D  11  Mar 14 1983  Most Expensive (^)      Hurdles (?)   Money Game (^)          Squeeze Play (^)       Secret "X" (^)  Ten Chances (^)
    4842D  11  Mar 15 1983     Lucky Seven (^)  Blank Check (^)  Pick a Pair (?)  Barker's Bargain Bar (^)  Five Price Tags (^)    Race Game (^)

We knew Hurdles and Pick a Pair were on these two shows, just not exactly which order. It turned out the "best guess" on these GPs was wrong:

            S      AIRDATE             PG1         PG2            PG3           PG4                   PG5              PG6
    4841D  11  Mar 14 1983  Most Expensive  Secret "X"    Ten Chances  Squeeze Play           Pick a Pair       Money Game
    4842D  11  Mar 15 1983     Lucky Seven     Hurdles  Race Game (@)   Blank Check  Barker's Bargain Bar  Five Price Tags

# UNKNOWN GAMES

There are 7 instances of truly missing games, that go beyond the **?** flag above. Thanks to Pluto TV, some prior instances have been resolved, and the latest instance now is season 5.

    !l s cond=unknown

    7 lineups found in DAYTIME for ?????????? played in any slot

        S      AIRDATE    INT. DATE                  PG1                  PG2                  PG3             PG4                  PG5               PG6
    0484D  1  Sep 10 1973  Aug 02 1973  Five Price Tags (^)       ?????????? (^)       Range Game (^)
    1022D  2  Aug 13 1974  Aug 13 1974       Range Game (^)  Five Price Tags (^)       ?????????? (^)
    1233D  3  Jan 08 1975  Jan 08 1975       ?????????? (^)      Lucky Seven (^)   Most Expensive (^)
    1525D  3  Aug 01 1975  Aug 01 1975       Clock Game (^)       ?????????? (^)  Five Price Tags (^)
    1655D  4  Oct 31 1975  Oct 31 1975       ?????????? (^)            Hi Lo (^)      Ten Chances (^)
    1881D  4  Apr 05 1976  Apr 05 1976      Lucky Seven (^)       Clock Game (^)       ?????????? (^)  Poker Game (^)  Five Price Tags (^)  Grocery Game (^)
    2343D  5  Apr 20 1977  Apr 20 1977     Give or Keep (^)        Race Game (^)      Ten Chances (?)    Bullseye (^)       ?????????? (^)    Range Game (^)

The "unknown" PG cannot be used in slots or conflict; it is mostly there for completion's sake.

# THE SEARCH COMMAND

Search is a much more powerful form of conflict and slots. Only by default will search will simply give the conflict of any number of games, its true use is for custom logic.

## UPDATE LATE 2022 / EARLY 2023

**Conditions for SEASON (S, numeric column), PROD (string column), AIRDATE / INT. DATE (date columns), NOTES (string column) are now like the equivalents for `wc search`. See that pastebin for more info on how to do those conditions. All other conditions are assumed to match the formatting given below.**

## SEARCH FORMATTING

Each condition in a search is a PG or PGGroup (see later section in this doc), followed by any combination of slots the PG or PGGroup must be in to match, followed by any combination of flags the PG playing must have at least one of to match, followed by any combination of the number of playings the PG, or a PG within a PGGroup, must have in one lineup to match. By default, any slot, flag or count (frequency) is matched.

With only one instance in the dataset of a PG playing with two flags (the aforementioned Safe MDG), flag searching is simplified to just "have any one of the given flags".

As mentioned in the help command, each "condition" after setting up the dataset and logic is a 4-tuple of words (separated by a comma or semicolon), with all but the PG optional: pricing game or pricing game group, slots, flags, and counts. The general format is

>cond=(pg|pggroup),s[slots],f[flags],c[counts]

where [slots] and [counts] each is any combo of 123456, no repeats, [flags] is any combo of __0CT&^*@R$^?M__, no repeats. Flag "0" corresponds to a regular playing with no actual flags.

Specifying slots with one of the __^?__ flags will result in an error: these flags by definition imply uncertain slotting.

## EXAMPLE 1: SLOTS & FLAGS

For example, to search for all lineups with Punch in the first half and Balance Game for a car in the second half:

    !lineup search cond=punch,s123 cond=balance,s456,fC

    1 lineup found in DAYTIME for all of
    * Punch a Bunch played 1st, 2nd, or 3rd
    * Balance Game played 4th, 5th, or 6th, with the "C" flag

            S      AIRDATE           PG1            PG2       PG3           PG4                 PG5        PG6
    8621K  47  Feb 11 2019  Side by Side  Punch a Bunch  Cover Up  Vend-O-Price  Balance Game (car)  Swap Meet

## EXAMPLE 2: PG WILDCARD

If any combination of dashes or stars is used in the PG part of a condition, it will match any game. This is useful when looking for all instances of certain flag(s) in a dataset, or if you just want the entire dataset.

For example, every instance of 3-prize trippers played last in S30-38:

    !l s cond=**,s6,fT cond=s;[30,38]

    9 lineups found in DAYTIME for all of
    * any game played 6th, with the "T" flag
    * SEASON is at least 30 and at most 38

            S      AIRDATE                   PG1            PG2               PG3                   PG4               PG5                 PG6
    2323K  31  Nov 27 2002      Range Game (car)   Let 'em Roll           Bonkers                Plinko          Cover Up  Most Expensive (T)
    2814K  32  Feb 19 2004        Shopping Spree  Punch a Bunch  That's Too Much!               Switch?      Let 'em Roll  Most Expensive (T)
    3102K  33  Dec 21 2004                Plinko      Race Game  That's Too Much!            Check Game     Pass the Buck  Most Expensive (T)
    3435K  34  Dec 02 2005           Golden Road     Grand Game        Clock Game          Freeze Frame        Master Key  Most Expensive (T)
    3493K  34  Jan 18 2006  Barker's Bargain Bar         Plinko       Ten Chances               Bonkers     Pass the Buck  Most Expensive (T)
    3631K  34  May 22 2006               1/2 Off    Hole in One        Range Game  Barker's Bargain Bar        Any Number   1 Wrong Price (T)
    3985K  35  May 11 2007            Clock Game   Let 'em Roll      Freeze Frame               Magic #        Pathfinder  Most Expensive (T)
    4831K  38  Sep 21 2009           Lucky $even         Plinko         Push Over          Grocery Game  That's Too Much!  Most Expensive (T)
    5131K  38  Apr 19 2010            Temptation   Grocery Game           Switch?             Flip Flop         Dice Game  Most Expen$ive (T)

## EXAMPLE 3: COUNTS

The only lineup to date that has repeated a PG is the all Plinko show. We can look for 6 occurrences of Plinko in a lineup to find it:

    !l s cond=plinko,c6

    1 lineup found in DAYTIME for Plinko (x6), played in any slot

            S      AIRDATE    INT. DATE     PG1           PG2         PG3           PG4         PG5         PG6
    6435K  42  Sep 27 2013  Oct 04 2013  Plinko  Plinko (car)  Plinko ($)  Plinko (car)  Plinko (T)  Plinko ($)

Using a singular PG in a count condition is useless otherwise. This is where PGGroups shine.

2-prizers generally conflict with each other, but Double Cross has been played a handful of times with another one:

    !l s cond=s;>=44 cond=2p,c2

    5 lineups found in DAYTIME for all of
    * SEASON is at least 44
    * any 2 PRIZER (x2), played in any slot

     PROD    S      AIRDATE    INT. DATE                    NOTES           PG1              PG2            PG3            PG4             PG5           PG6
    7215K   44  Sep 25 2015  Sep 25 2015      Decades Week – 10s   Do the Math        Card Game  Time I$ Møney   Double Cross        Rat Race  Vend-O-Price
    7304K   44  Nov 30 2015  Nov 26 2015             Cyber Monday    Money Game          Switch?   Vend-O-Price    Lucky $even         1/2 Off  Double Cross
    7451K   44  Mar 14 2016  Mar 14 2016                               Cover Up     Side by Side  Punch a Bunch    Do the Math  Stack the Deck  Double Cross
    8015K   46  Sep 21 2017  Sep 22 2017  Drew's 10th Anniversary  Vend-O-Price      Do the Math      Gas Money  Time I$ Møney    Double Cross      Rat Race
    9304K   49  Feb 11 2021  Feb 11 2021                                Switch?  It's in the Bag      Dice Game   Balance Game      Master Key  Double Cross

Ever seen a lineup overloaded with 1-prizers? This April Fool's show was the highest:

    !l s cond=s;>36 cond=1p,c456

    1 lineup found in DAYTIME for all of
    * SEASON is greater than 36
    * any 1 PRIZER (x4,5,6), played in any slot

     PROD    S      AIRDATE                     NOTES                 PG1           PG2            PG3           PG4           PG5          PG6
    5505K   39  Apr 01 2011  April Fool's – 10,000th  Squeeze Play (car)  Freeze Frame  Double Prices  Balance Game  Side by Side  Lucky $even

How many times in S48 was there a SP/cash in the 1st half, then the GP was a car game in the 2nd half? Simple:

    !l s cond=s;48 cond=sp/cash,s123 cond=gp/car,s456

    9 lineups found in DAYTIME for all of
    * SEASON is 48
    * any SP/CASH game played 1st, 2nd, or 3rd
    * any GP/CAR game played 4th, 5th, or 6th

     PROD      AIRDATE    INT. DATE            PG1            PG2            PG3              PG4             PG5              PG6
    8845K  Oct 11 2019  Oct 11 2019        1/2 Off   Squeeze Play     Line em Up     Side by Side     Hole in One  One Wrøng Price
    8851K  Oct 14 2019  Oct 14 2019     Plinko (*)    Do the Math      Dice Game  Coming or Going    Danger Price   Stack the Deck
    8884K  Nov 07 2019  Nov 07 2019        1/2 Off  Pick-a-Number  Pocket ¢hange        Race Game    Squeeze Play     Let 'em Roll
    8954K  Dec 26 2019  Dec 26 2019         Plinko     Clock Game      Gridlock!    Double Prices   Eazy az 1 2 3   Stack the Deck
    9011K  May 29 2020  Feb 03 2020  Punch a Bunch   Bargain Game       Cover Up     Freeze Frame  Stack the Deck  One Wrøng Price
    9021K  Feb 10 2020  Feb 10 2020       One Away      Push Over        1/2 Off       Check Game    Bargain Game     Let 'em Roll
    9053K  Mar 04 2020  Mar 04 2020  1 Right Price      Card Game         Plinko        Swap Meet    Let 'em Roll     Freeze Frame
    9084K  Apr 02 2020  Apr 02 2020  Punch a Bunch    Do the Math     Money Game  Coming or Going  Stack the Deck     Danger Price
    9141K  May 11 2020  May 11 2020        1/2 Off   Squeeze Play      Gridlock!     Freeze Frame   1 Right Price      Hole in One

Flags and counts can be combined as well. This is most applicable to a full-on wildcard. Note searching this may take longer. Shows with exactly 2 slottings uncertain:

    !l s cond=*,f^,c2

    4 lineups found in DAYTIME for any game (x2), played in any slot, with the "^" flag

     PROD   S      AIRDATE    INT. DATE              PG1                 PG2         PG3                PG4                PG5                PG6
    6123D  14  May 14 1986  May 14 1986   Temptation (^)          Plinko (^)    Take Two               Bump         Penny Ante           One Away
    6594D  16  Oct 01 1987  Oct 08 1987       Clock Game  Hole in One or Two  Check Game  Money Game (boat)  1 Right Price (^)     Switcheroo (^)
    6643D  16  Nov 11 1987  Nov 11 1987             Bump           3 Strikes       Hi Lo      Double Prices     Any Number (^)  Cliff Hangers (^)
    7225D  17  Apr 07 1989  Apr 07 1989  Lucky $even (^)          Plinko (^)    Take Two         Check Game              Hi Lo         Any Number

Finally, a neat trick to filter out half hour shows or full hour shows: *,c3 or *,c6.

## WARNINGS

There are five warnings that can appear when using search (see two sections below):

- *Missing games have uncertain slots by definition.*
Occurs when using "unknown,s#" in a condition. See below section.
- *Slotting of uncertainly slotted playings specified in a condition.*
Occurs when using "f^,s#" in a condition. A reminder that the slotting is best guess.
- *Uncertain playing flag specified in a condition. Playings marked with the ? flag belong to a lineup, at worst, that is close to the given production number.*
Occurs when using "f?" in a condition. The given lineup that matches may not be correct.
- *Slotting of uncertainly slotted playings factored into results.'*
Occurs when slotting, but not **^**, is in a condition, and at least one matching lineup has a **^** flag in a matching slot.
- *Playings marked with the ? flag belong to a lineup, at worst, that is close to the given production number.*
Occurs when **?** is not in a condition, but at least one matching lineup has that **?** flag on the condition's PG/PGGroup.

## THE USE OF ANY

By default, the logic expression is __all__, where all the conditions must match. Use __any__ to make it so only one of the conditions need to match:

    !l s logic=any cond=telephone cond=professor

    5 lineups found in DAYTIME for any of
    * Telephone Game played in any slot
    * Professor Price played in any slot

     PROD  S      AIRDATE             PG1               PG2                 PG3                PG4                 PG5                 PG6
    2561D  6  Nov 14 1977      Bonus Game        Poker Game     Professor Price      Double Prices          Any Number  Grocery Game (car)
    2571D  6  Nov 21 1977    Danger Price   Professor Price          Shell Game      1 Right Price           3 Strikes             Hurdles
    3013D  7  Nov 01 1978  Clock Game (^)  Squeeze Play (^)  Telephone Game (^)  1 Right Price (^)      Poker Game (^)      Switcheroo (^)
    3035D  7  Nov 17 1978  Bonus Game (^)      Take Two (^)     Ten Chances (^)  1 Right Price (^)  Telephone Game (^)      Clock Game (^)
    3053D  7  Nov 29 1978      Range Game    Telephone Game          Poker Game     Most Expensive       Punch a Bunch          Clock Game

## FULL POWER: CUSTOM LOGIC

Custom logic is supported for up to 26 conditions (you should never need this many). When building a custom logic expression, remember that each condition is internally labeled A, B, C ... in order. The following boolean operators are allowed as simple words, or their boolean logical symbols:

- **AND, &**. all conditions must match, e.g. A & B & C for all three
- **OR, |**. any condition must match, e.g. A | B | C | D for any one of these four.
- **NOT, ~**. the condition must not match. e.g. ~ ( A | B ) for both these conditions failing.
- **XOR, ^**. exactly one condition must match out of two. e.g. ~ (A ^ B) matches either both conditions matching or both failing.
-- Technically, for n arguments to XOR, an odd number of conditions must match if n is even, or an even number of arguments must match if n is odd. In general, it's simpler to limit XOR to two conditions.

With parentheses and up to 26 conditions (letters), as well as PGGroups effectively making an OR of any set of PGs, many powerful expressions are possible.

The logic expression is allowed as one word with no spaces, but this can get unreadable fast. Surround an expression with quotes, and spaces will be allowed.

Note searching this may take longer with a complicated custom logic expression, but you can get very specific results with them - examples now follow.

Here's an example: Throughout Rent's history, how many times was it played in the first two, followed by either Dice, Card, or Money 3rd, and in the second half, Master Key was played but not Race Game?

    !l s logic="A & (B | C | D) & E & ~F" cond=rent,s12 cond=dice,s3 cond=card,s3 cond=money,s3 cond=key,s456 cond=race,s456

    3 lineups found in DAYTIME for
    A & E & ~F & (B | C | D) ; where

    A = Pay the Rent played 1st or 2nd
    B = Dice Game played 3rd
    C = Card Game played 3rd
    D = Money Game played 3rd
    E = Master Key played in the second half
    F = Race Game played in the second half

     PROD    S      AIRDATE    INT. DATE              PG1           PG2         PG3             PG4           PG5           PG6
    5995K   40  May 16 2012  Jun 01 2012     Pay the Rent     Push Over  Money Game  Most Expen$ive    Master Key       Bonkers
    7915K   45  Apr 28 2017  Apr 28 2017  Coming or Going  Pay the Rent   Card Game         Switch?  Balance Game    Master Key
    9091K   48  Apr 06 2020  Apr 06 2020    Double Prices  Pay the Rent  Money Game         Bonkers    Master Key  Double Cross

Another fun example, this time with PGGroups for even more power: there's been a dozen lineups with no GP or SP game in the show's history, discarding Let's Make a Deal games as well as one instance of an unknown game:

    !l s logic=~(A|B|C|D) cond=gp cond=sp cond=deal cond=unknown

    DISCLAIMER: Playings marked with the ? flag belong to a lineup that is, at worst, close to the given production number.

    12 lineups found in DAYTIME for
    ~(A | B | C | D) ; where

    A = any GP game played in any slot
    B = any SP game played in any slot
    C = any DEAL game played in any slot
    D = ?????????? played in any slot

     PROD   S      AIRDATE    INT. DATE                 PG1                 PG2                 PG3            PG4              PG5            PG6
    0854D   2  Apr 18 1974  Apr 18 1974          Temptation          Range Game      Most Expensive
    0884D   2  May 09 1974  May 09 1974      Clock Game (^)      Any Number (^)      Range Game (^)
    0981D   2  Jul 15 1974  Jul 15 1974      Clock Game (^)     Any Number (?^)  Most Expensive (^)
    1004D   2  Aug 01 1974  Aug 01 1974  Most Expensive (^)      Clock Game (^)      Temptation (^)
    1035D   2  Aug 23 1974  Aug 23 1974      Temptation (^)  Most Expensive (^)      Clock Game (^)
    1411D   3  May 12 1975  May 12 1975  Most Expensive (^)      Temptation (^)      Clock Game (^)
    1635D   4  Oct 17 1975  Oct 17 1975      Range Game (^)      Any Number (^)  Most Expensive (^)
    1651D   4  Oct 27 1975  Oct 27 1975          Any Number          Poker Game      Most Expensive
    4495K  37  Oct 17 2008  Nov 07 2008         Lucky $even        Balance Game             Step Up    Ten Chances          Magic #  Double Prices
    4884K  38  Oct 08 2009  Oct 29 2009        Freeze Frame            Cover Up          Clock Game  Eazy az 1 2 3     Balance Game     Any Number
    5505K  39  Apr 01 2011  Apr 01 2011  Squeeze Play (car)        Freeze Frame       Double Prices   Balance Game     Side by Side    Lucky $even
    5551K  39  May 02 2011  May 02 2011       1 Right Price             Bonkers          10 Chances   Balance Game  Coming or Going       Cover Up

## EXCLUDE UNCERTAIN

For search, there is a separate exclude option from conflictN. It is the same as specifying "f0CT&\*@R$M" invisibly for every condition. Of course, it can be overwritten for any command.

    !l s cond=s;17 cond=t2,s2
    WARNING: Uncertainly slotted games may be included in results.

    6 lineups found in DAYTIME for all of
    * SEASON is 17
    * Take Two played 2nd

     PROD      AIRDATE    INT. DATE            PG1           PG2              PG3             PG4              PG5                   PG6
    6953D  Sep 21 1988  Sep 21 1988    Lucky $even      Take Two  Phone Home Game      Check Game  Five Price Tags  Barker's Bargain Bar
    6972D  Oct 04 1988  Oct 04 1988     Master Key      Take Two       Clock Game   1 Right Price        Card Game            Grand Game
    7041D  Nov 28 1988  Nov 28 1988   Squeeze Play      Take Two      Hole in One      Check Game           Plinko             3 Strikes
    7182D  Mar 07 1989  Mar 07 1989         Plinko  Take Two (^)      Ten Chances  Check Game (^)    Check-Out (^)        Any Number (^)
    7212D  Mar 28 1989  Mar 28 1989  Safe Crackers      Take Two     Spelling Bee   1 Right Price        Dice Game            Grand Game
    7283D  May 18 1989  May 17 1989      3 Strikes      Take Two  Phone Home Game            Bump       Range Game            Switcheroo

    !l s cond=s;17 cond=t2,s2 exclude=y
    5 lineups found in DAYTIME for all of
    * SEASON is 17
    * Take Two played 2nd, with no guess flags

     PROD      AIRDATE    INT. DATE            PG1       PG2              PG3            PG4              PG5                   PG6
    6953D  Sep 21 1988  Sep 21 1988    Lucky $even  Take Two  Phone Home Game     Check Game  Five Price Tags  Barker's Bargain Bar
    6972D  Oct 04 1988  Oct 04 1988     Master Key  Take Two       Clock Game  1 Right Price        Card Game            Grand Game
    7041D  Nov 28 1988  Nov 28 1988   Squeeze Play  Take Two      Hole in One     Check Game           Plinko             3 Strikes
    7212D  Mar 28 1989  Mar 28 1989  Safe Crackers  Take Two     Spelling Bee  1 Right Price        Dice Game            Grand Game
    7283D  May 18 1989  May 17 1989      3 Strikes  Take Two  Phone Home Game           Bump       Range Game            Switcheroo

# PG GROUPS

For the search command, it is useful to define an arbitrary set of games as a PGGroup. You can use the pggroup command to make your own mappings, or use the defaults, which generally partitions every PG by type of game.

The bot comes with many single words that map to these groups. Use the **pg list_groups** and **pg groupAbbr** commands to list them, case insensitive.

# VALID PG STRINGS

Use the **pg list** and **pg abbr** commands to list every available PG, and every single "word" that maps to a PG object in my code, case insensitive. Balance Game, Bullseye, and Time is Money are considered different games between their first and second iterations.
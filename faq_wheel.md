# INTRO

The conditions allowed in the `wheelcompendium search` set of commands are not final in that more may be added, or the syntax may be changed depending upon community needs. Nothing will be subtracted. This document will stay up to date with changes.

The command signature:

`!wheelcompendium [search|s] ([logicexpr|logic]=all) ([condition|cond|c]=...) ([time|version]=syndicated) ([dateformat|format]=%m/%d/%y)`

There is also `[search_choices|sc]` and `[search_sched|ss]`.

The custom logic is identical to `lineup search` on the Price is Right side of the bot, refer to that FAQ for more.

Currently, the accepted `time` parameters are: `syndicated`, `daytime`, `primetime`, `gb`, `au`, `kids`.

Examples given below may have output truncated for length.

# TRUE/FALSE SPECIFICATION

To specify a true in a condition, use one of the following words (case-insensitive): yes, y, true, t, 1. For false: no, n, false, f, 0.

# SEPARATING "WORDS" IN A CONDITION

Conditions are "words" separated by semicolons (`;`). You can have up to 26 conditions, labeled A-Z in custom logic. This is more strict than `lineup search` because `,` shows up in a few bonus answers on puzzles (and I used that liberty to use `,` in some forms of words). The first word corresponds to the type of query, each of which has a dedicated section below. All condition text will be translated to uppercase. The first word corresponds to the type of query, each of which has a dedicated section below. Some conditions have secondary "subconditions" as the second word.

# COLUMNS

Condition types mostly correspond to filtering data based on a column. wayo.py's compendium has the following columns on the puzzle data:

- season `S`, from 1 to 39 (and onward!)
- date `DATE`, the airdate / intended date (alias `D`)
- overall episode number `EP`
- episode of the season `E/S` (alias `ES`), this is calculated by wayo.py
- date / episode number uncertainty `UC`
- round `ROUND` (alias `RD`)
- `PP` marker for prize puzzle
- `RL` marker for red letter puzzle
- `PR` marker for puzzler
- the actual puzzle, `PUZZLE` (alias `P`)
- the category, `CATEGORY` (alias `CAT`)
- `CLUE/BONUS`, the bonus money question for certain categories, or crossword clue (aliases `CLUE`, `BONUS`, `CB`, `B`)

For `search_choices`, there are the `S`, `DATE`, `EP`, `E/S`, and `PUZZLE` columns like the above, along with:

- All three category choices (`CHOICE#` where `#` is one of `123`, aliases `C#`)
- The actual category chosen, `CHOSEN` (alias `C`), always matching with `CATEGORY` above
- The position `POS` of the category chosen in the list of all the choices (one of `123`)

For `search_sched`, there are `EP` and `DATE` columns like the above, along with:

- Some `DATE`s are incomplete and be searched as a string column via `DATE_STR` (alias `DS`). These are mixed in with proper `DATE`s in one column in output.
- `RED` (alias `R`), `YELLOW` (aliases `YEL`, `Y`), `BLUE` (alias `BL`) for contestant name(s) on that podium
- `PODIUM` (aliases `P`, `CONTESTANT`, `CON`) as shorthand for any of `RED/YELLOW/BLUE`
- `THEME` (alias `T`) for theme of the episode if it exists.

# NUMBER EXPRESSIONS

For any condition based on a (calculated) whole number, the following can be put as a word of the condition:

- `N`, equal to N. Can also be written as `=N`
- `N1,N2,...`, equal to one of N1,N2 ...
- `~=N`, not equal to N
- `>N`, greater than N
- `<N`, less than N
- `>=N`, at least N
- `<=N`, at most N
- `[N1,N2]`, between N1 and N2 inclusive
- `(N1,N2)`, between N1 and N2 exclusive
--the `[]` and `()` can be mixed and matched for inclusive / exclusive on those bounds. This matches conventional mathematical notation describing ranges.

Any of these sub-words above can be combined in the same condition. Any one of them being true will then match the condition.

# STRING EXPRESSIONS

A string in programming terms is, pretty much, simply an alias for text. Obviously there is a lot of text every row in the compendium. For conditions involving text:

- `regex`, there is a match of `regex` within the entire string.
--Regular expressions are very powerful and you can get a lot out of your searchs if you learn the basics: https://regexone.com https://www.regular-expressions.info/tutorial.html
- `regex;num`. There are `num` number of matches of `regex` within the entire string. `num` can be any number expression from the previous section. (So `regex` is simply shorthand for `regex;>=1`.)

Any of these sub-words above can be combined in the same condition. Any one of them being true will then match the condition. A lot of "any of these being true" can be done implicitly if regular expressions are leveraged correctly.

## LITERALS & EXACT

For the string columns, the following can be appended right after the `regex` word in the condition, as its own additional word in the condition:

- `LITERAL`, `LIT`, `L`: Look that the column in question has the given text somewhere in it, with no regular expressions applied, essentially just turning `regex` into `text`.
- `EXACT`, `E`: Look that the column in question is exactly the given text.

These are helpers to ignore regular expressions if so desired.

# TYPES OF CONDITIONS

With that out of the way, here's what you can search for in the compendium, each section groups the same type of condition:

## S, EP, E/S

These columns are whole numbers, so any combo of number expressions can be done on them.

`!wc search cond=s;30 cond=e/s; 90,135`
```
17 puzzles found for all of

* S is 30
* E/S is one of 90,135

 S         DATE    EP  E/S  RD  PP                                        PUZZLE        CATEGORY
30  Jan 18 2013  5745  090  T1                           HEAD OUT ON THE HIGHWAY     SONG LYRICS
30  Jan 18 2013  5745  090  T2                                UPSTANDING CITIZEN          PERSON
...
30  Mar 22 2013  5790  135  R5                                  ANIMAL SANCTUARY           PLACE
30  Mar 22 2013  5790  135  BR                                    PAY IT FORWARD          PHRASE
```

`!wc search logic=A&(B|C) cond=s;31;(35,39] cond=ep;[6010,6015] cond=e/s;195`
```
86 puzzles found for
A & (B | C); where

A = S is 31, or greater than 35 and at most 39
B = EP is at least 6010 and at most 6015
C = E/S is 195

 S         DATE    EP  E/S  RD  PP                                  PUZZLE             CATEGORY       CLUE
31  Apr 25 2014  6010  160  T1                               NEW HAMPSHIRE           ON THE MAP
...
36  Jun 07 2019  7020  195  R2                     BISHOP KNIGHT KING ROOK            CROSSWORD  CHECKMATE
...
39  Jun 10 2022  7620  195  BR                             PACK OF DINGOES        LIVING THINGS
```
## DATE

A date given in the given format (default mm/dd/yy, and can be changed with the format argument to the command) acts just like a number, and any number expression works:

`!wc s cond=d; [1/1/11, 1/4/11)`
```
8 puzzles found for DATE is on or after 01/01/11 and earlier than 01/04/11

 S         DATE    EP  E/S  RD  PP                           PUZZLE             CATEGORY
28  Jan 03 2011  5346  081  T1                           BALD EAGLE         LIVING THING
...
28  Jan 03 2011  5346  081  BR                             HAZY SKY                THING
```

There are four "subconditions" which you can query: the `YEAR` (alias `Y`), the `MONTH` (alias `M`, three letter abbreviations Jan, Feb, ..., Dec accepted), the `DAY` (alias `D`), and the weekday (aliases `WKDAY` and `DOW`, Monday=0 and Friday=4, one and three letter abbreviations accepted). These are, of course, all number expressions.

```
40 puzzles found for all of

* MONTH of DATE is one of January, March
* DAY of DATE is at least 26 and at most 30
* WEEKDAY of DATE is on or after a Wednesday
* S is 39

 S         DATE    EP  E/S  RD  PP                              PUZZLE               CATEGORY        CLUE
39  Jan 26 2022  7523  098  T1                  THE TRUTH IS OUT THERE               TV QUOTE
...
39  Jan 26 2022  7523  098  R2                 BUGGY SHAMPOO SEAT BOOM              CROSSWORD  BABY _____
...
39  Mar 30 2022  7568  143  R2                     BILL RICE BOAR LIFE              CROSSWORD  WILD _____
...
39  Mar 30 2022  7568  143  BR                           BROWSE AROUND                 PHRASE
```

## PP, RL, PR

These are "boolean" columns in that they're either True or False. So simply `cond=column` looks if the puzzle is marked with this column, `cond=column;False` looks if the row is specifically not marked with this column. (`cond=column;True` is also allowed, but there is the shorthand above.) And any alias for True/False as per the top of this document is allowed.

Note that in output, since there can be a long output and there is only one header line, if True the row will just repeat the column name in that spot to signify True. Also, if all of a column is False in a search result, it will be omitted from the output.

`!wc s cond=rl cond=e/s;<40;>=170`
```
11 puzzles found for all of

* is a RL puzzle
* E/S is less than 40, or at least 170

 S         DATE    EP  E/S  RD  RL                                        PUZZLE   CATEGORY   BONUS
11  Oct 12 1993  1977  027  R3  RL                             PROSPECTIVE BUYER     PERSON  EUROPE
11  May 13 1994  2120  170  R4  RL                             CHICKEN A LA KING     PHRASE   KHAKI
...
12  Oct 12 1994  2173  028  R4  RL                  GETTING A GOOD NIGHT'S SLEEP      EVENT   GHOST
12  Oct 27 1994  2184  039  R4  RL                     STARING UP AT THE CEILING     PHRASE    TENT
```

## UC

This used to be a boolean column, but a summer 2023 update to the compendium added specificity to what is uncertain, so it became a string (text) column. It can be empty, the DATE (`D`), EP (`#`), or both (`B`).

## ROUND, CATEGORY

These are string (text) columns. TBD: The option of enumerations like Pricing Games on the Price side of the bot.

Here are some helpful regular expressions to get you started on `ROUND`:

- All tossup categories: search for `T` in `ROUND`
- All regular round categories: search for `R` in `ROUND` (note the preview puzzle (`PP`) in S17 as an exception)
- All post-tossup maingame rounds: `R[4-7]`
- All puzzles but BR: `^[^B]` (does not start with a B).(Alternatively, custom logic with a NOT on `BR`)

And some more on `CATEGORY`:
- for singular or plural on most categories: end with `S?` (once or not at all on the `S`)
- THING(S) but NOT LIVING: `^THINGS?` (starts with TH...)
- PERSON or PEOPLE: `(PERSON|PEOPLE)` (can be shorthanded as `^PE[RO]`, starts with `PER` or `PEO`)
- CROSSWORD or MEGAWORD: `WORD$` (ends with `WORD`)

## PUZZLE, CLUE / BONUS

These are string (text) columns.

Here are some helpful regular expressions to get you started here:

- Any vowel: `[AEIOU]`
- Any consonant: `[^AEIOU]` (not a vowel essentially)

`!wc s logic=any cond=p;E;>=9 cond=p;M;>=9 cond=p;A;>=9`

```
26 puzzles found for any of

* PUZZLE matches "E" at least 9 time(s)
* PUZZLE matches "M" at least 9 time(s)
* PUZZLE matches "A" at least 9 time(s)

 S         DATE    EP  E/S  RD  PP                                            PUZZLE        CATEGORY                                         BONUS
 7  Nov 01 1989  1213  043  R3                   SEPTEMBER OCTOBER NOVEMBER DECEMBER          THINGS
...
21  Sep 12 2003  3905  005  R3                  QUEEN ELIZABETH CELEBRATES MILESTONE        HEADLINE  ANNIVERSARY OF CORONATION: 40TH? 50TH? 60TH?
21  May 25 2004  4087  187  R3              A LONG TIME AGO IN A GALAXY FAR FAR AWAY       QUOTATION
22  Dec 22 2004  4173  078  T3                            FA LA LA LA LA LA LA LA LA     SONG LYRICS
...
24  May 21 2007  4666  181  R3            TALL YELLOW SESAME STREET FEATHERED FRIEND      WHO IS IT?                                      BIG BIRD
...
31  Oct 11 2013  5870  020  R2              SUMMERTIME SUMMERTIME SUM SUM SUMMERTIME     SONG LYRICS
31  Dec 31 2013  5927  077  R1                    I FEEL THE NEED THE NEED FOR SPEED     MOVIE QUOTE
...
37  Feb 04 2020  7127  101  R3  PP                  EVERY DAY FEELS LIKE THE WEEKEND          PHRASE
38  Dec 18 2020  7300  070  R2              NOW DASH AWAY! DASH AWAY! DASH AWAY ALL!       QUOTATION
39  May 16 2022  7601  176  R2         WHERE THE DEER AND THE ANTELOPE PLAY CHECKERS  BEFORE & AFTER
```

`!wc s cond=s;>=25 cond=rd;R[1-7] cond=p;[AEIOU];>=18;<=2`
```
9 puzzles found for all of

* S is at least 25
* RD matches "R[1-7]"
* PUZZLE matches "[AEIOU]" at least 18, or at most 2 times

 S         DATE    EP  E/S  RD                                                PUZZLE           CATEGORY         BONUS
25  Jan 15 2008  4772  092  R5                                             THUMBTACK   AROUND THE HOUSE
25  Jan 16 2008  4773  093  R6                                          HURRY-SCURRY         RHYME TIME
26  Mar 31 2009  5022  147  R3     VOTING FOR YOUR FAVORITE AMERICAN IDOL CONTESTANT           SHOW BIZ
27  Oct 23 2009  5100  030  R6                                             LOCKSMITH         OCCUPATION
28  Dec 21 2010  5337  072  R3  OH WHAT FUN IT IS TO RIDE IN A ONE-HORSE OPEN SLEIGH  WHAT'S THAT SONG?  JINGLE BELLS
28  Apr 01 2011  5410  145  R2   FOOL ME ONCE SHAME ON YOU FOOL ME TWICE SHAME ON ME             PHRASE
30  Jan 15 2013  5742  087  R4                                           PASTRY CHEF         OCCUPATION
34  Feb 20 2017  6551  116  R5                                       CHARTS & GRAPHS             THINGS
39  May 13 2022  7600  175  R4                                           SNACK SHACK         RHYME TIME
```

For the BONUS, it can be queried as a boolean column to signify a non-empty string or not (NOT possible with CLUE, even though both sets of data are in the same column - you can just search for CROSSWORD in a category query in that case, it is 1-to-1). Regular expressions aren't as meaningful here as puzzles but can still be used for advanced filtering, like any string column.

## MORE ON PUZZLES

Advanced info on the puzzle column can be queried.

### WORDS

Word count of a puzzle is available as `WORD_COUNT` (alias `WC`):

`!wc s cond=wc;>12`
```
1 puzzle found in SYNDICATED for total word count is greater than 12

 S         DATE    EP  E/S  RD                                         PUZZLE  CATEGORY
14  May 07 1997  2703  168  R2  I HATE TO SAY I TOLD YOU SO BUT I TOLD YOU SO    PHRASE
```

The basic `WORD` condition looks to see if the given `regex` is part of any word (or `text` if literal/exact is given after).

`!wc s cond=w;slow;e`
```
14 puzzles found in SYNDICATED for any word is exactly "SLOW"

 S         DATE    EP  E/S  RD  PR                                            PUZZLE             CATEGORY         CLUE
11  Feb 02 1994  2053  103  BR                                           SLOW MOTION               PHRASE
...
35  Mar 28 2018  6773  143  R2                               SQUARE DISCO WALTZ SLOW            CROSSWORD  LET'S DANCE
...
39  Nov 12 2021  7470  045  R1                         SLOW AND STEADY WINS THE RACE            QUOTATION
```

A positive or negative number (not 0) can be given as the last word in the condition to specify an exact position of the word. 1 = first, -1 = last, 2 = second, -2 = second-to-last, and so on.

`!wc s cond=w;beach(es)?;-1`
```
104 puzzles found in SYNDICATED for last word matches "BEACH(ES)?"

 S         DATE    EP  E/S  RD  PP                                     PUZZLE             CATEGORY
13  Jan 17 1996  2433  093  R4                               WHITE SAND BEACH                PLACE
13  Feb 20 1996  2457  117  R1                            A LUAU ON THE BEACH                EVENT
18  May 21 2001  3501  186  T1                             A DAY AT THE BEACH                EVENT
20  Apr 04 2003  3860  155  T1                                 SECLUDED BEACH                PLACE
20  Apr 07 2003  3861  156  R4                                WEST PALM BEACH           ON THE MAP
21  Sep 25 2003  3914  014  R1  PP                      WHITE POWDERY BEACHES               PLACES
21  Jan 19 2004  3996  096  BR                                  WAIKIKI BEACH           ON THE MAP
21  Apr 01 2004  4049  149  T2                                   MALIBU BEACH           ON THE MAP
...
```

### PUZZLE OF EP

This is an unique condition that groups the compendium's output by episode (equivalent to date) and singles out the Xth puzzle of the show (see the last example on WORDS just above, the argument follows the same format).

In general (in shows from the same era) this isn't too useful (e.g. `-1` will always be BR, `1` will always be `T1` in the tossup era etc., and can be done much easier with a simple `RD` condition). Nonetheless, there is some use: perhaps most importantly, specifying `-2` will always show the speedup puzzle, if the episode has one:

`!wc s cond=p/e;-2 cond=ep;[7721,7725]`
```
5 puzzles found in SYNDICATED ... for all of

* PUZZLE is the 2nd-to-last of EP (DATE)
* EP is at least 7721 and at most 7725

 S         DATE    EP  E/S   RD                    PUZZLE          CATEGORY
40  Jan 30 2023  7721  101   R4   CONVECTION TOASTER OVEN  AROUND THE HOUSE
40  Jan 31 2023  7722  102   R4  BASED ON THE BEST SELLER          SHOW BIZ
40  Feb 01 2023  7723  103   R5                CARTOONIST        OCCUPATION
40  Feb 02 2023  7724  104   R4  AFTER MUCH CONSIDERATION            PHRASE
40  Feb 03 2023  7725  105   R4  SINGLE-SERVE COFFEEMAKER    IN THE KITCHEN
```

### LENGTH, LENGTH_UNIQUE

We can look at the total number of letters with `LENGTH` (alias `L`) as a number expression:

`!wc s cond=length;<=6 cond=rd;R\d cond=pr;F`
```
12 puzzles found for all of

* Length is at most 6
* RD matches "R\d"
* is not a PR puzzle

 S         DATE    EP  E/S  RD   PUZZLE      CATEGORY BONUS
10  Jan 13 1993  1848  093  R1   OZ DOG          CLUE  TOTO
12  Mar 10 1995  2270  125  R6   GLOVES        THINGS
16  Feb 09 1999  3037  112  R5   MARINA         PLACE
16  Apr 09 1999  3080  155  R1   WINERY         PLACE
16  Apr 26 1999  3091  166  R4  TOP COP    RHYME TIME
16  May 04 1999  3097  172  R1   TAILOR    OCCUPATION
16  May 18 1999  3107  182  R5   TEXANS        PEOPLE
16  May 21 1999  3110  185  R5   PAYDAY         EVENT
16  Jun 01 1999  3117  192  R1    ATTIC         PLACE
17  Oct 12 1999  3147  027  R5   MEADOW         PLACE
18  Sep 25 2000  3331  016  R4  TEX-MEX    RHYME TIME
18  Apr 23 2001  3481  166  R5   WALRUS  LIVING THING
```

To count how many different letters actually show up in a puzzle, there's `LENGTH_UNIQUE` (alias `LU`):

`!wc s cond=lu;>=20`
```
10 puzzles found for Total number of unique letters is at least 20

 S         DATE    EP  E/S  RD  PP                                                 PUZZLE        CATEGORY
15  Nov 27 1997  2794  064  R2       UP ABOVE THE WORLD SO HIGH LIKE A DIAMOND IN THE SKY       QUOTATION
16  Dec 29 1998  3007  082  R2      WATCHING THE NEW YEAR'S EVE BALL DROP IN TIMES SQUARE           EVENT
17  Nov 24 1999  3178  058  R2                      A SECOND HELPING OF TURKEY WITH GRAVY           THING
17  Jan 17 2000  3216  096  R2                  HOCKEY HALL OF FAME INDUCTS WAYNE GRETZKY        HEADLINE
20  Nov 14 2002  3759  054  R2             WEEKEND UPDATE ANCHORS JIMMY FALLON & TINA FEY        SHOW BIZ
22  Sep 07 2004  4097  002  R2              JENNIFER LOPEZ WEDS SALSA SINGER MARC ANTHONY        HEADLINE
24  Nov 21 2006  4537  052  R2                     CURLING UP WITH A GOOD BOOK OF MATCHES  BEFORE & AFTER
26  Nov 11 2008  4922  047  R1             I JUST WANT TO CELEBRATE ANOTHER DAY OF LIVING     SONG LYRICS
26  May 18 2009  5056  181  R2  PP              MANDARIN DUCK WITH VEGETABLE SPRING ROLLS    FOOD & DRINK
27  Feb 04 2010  5174  104  R3              OBSERVATION DECK OF THE EMPIRE STATE BUILDING           PLACE
```

### COUNT, COUNT_UNIQUE

While separate condition types, these two can be considered a subset of the `LENGTH` equivalents, doing those queries only on a given subset of letters.

The aliases are obvious as well (`C`, `CU`).

`!wc s cond=count;rstlne;>=9 cond=rd;br cond=d;>=6/3/88`
```
9 puzzles found for all of

* total number of RSTLNE is at least 9
* RD matches "BR"
* DATE is on or after 06/03/88

 S         DATE    EP  E/S  RD                  PUZZLE             CATEGORY
 6  Sep 20 1988  0987  012  BR  THE SPIRIT OF ST LOUIS                TITLE
29  Mar 28 2012  5598  138  BR      BASIC REQUIREMENTS               THINGS
30  Oct 02 2012  5667  012  BR     PREVIOUS EXPERIENCE               PHRASE
30  Nov 29 2012  5709  054  BR     EXPERT IN THE FIELD               PERSON
32  Dec 15 2014  6111  066  BR  LATE-NIGHT INFOMERCIAL                THING
33  Jan 21 2016  6334  094  BR     BROWSING THE AISLES  WHAT ARE YOU DOING?
34  Nov 01 2016  6472  037  BR   UNBEATABLE CONNECTION               PHRASE
35  Nov 24 2017  6685  055  BR     INGENIOUS INVENTION                THING
35  May 22 2018  6812  182  BR     ORDERING APPETIZERS  WHAT ARE YOU DOING?
```

`!wc s cond=cu;jqxz;>=2 cond=rd;br`
```
59 puzzles found for all of

* total unique number of JQXZ is at least 2
* RD matches "BR"

 S         DATE    EP  E/S  RD                    PUZZLE               CATEGORY
18  May 18 2001  3500  185  BR                JAZZ IT UP                 PHRASE
21  Apr 15 2004  4059  159  BR                  JURY BOX                  PLACE
...
39  Oct 21 2021  7454  029  BR           OUTDOOR JACUZZI       AROUND THE HOUSE
39  May 18 2022  7603  178  BR                 JAZZ CLUB                  PLACE
```

As a convenience, "CONSONANT" can be put in as an alias for every consonant:

`!wc s cond=cu;consonant;<=1`
```
9 puzzles found for total unique number of BCDFGHJKLMNPQRSTVWXYZ is at most 1

 S         DATE    EP  E/S  RD      PUZZLE  CATEGORY
 6  Jan 31 1989  1072  097  BR       ONION     THING
10  Oct 08 1992  1779  024  BR         ZOO     PLACE
11  May 12 1994  2119  169  BR       YO-YO     THING
11  Jun 06 1994  2136  186  BR        MEMO     THING
12  Nov 15 1994  2197  052  BR        IDEA     THING
13  Mar 26 1996  2482  142  BR        I DO    PHRASE
13  Apr 05 1996  2490  150  BR        BABE     TITLE
16  Mar 31 1999  3073  148  BR        PIPE     THING
32  May 07 2015  6214  169  T1  MAMMA MIA!     TITLE
```

### MULT

One way to dissect a puzzle is to map every letter to its frequency in the puzzle. For example, let's look at the highest consonant multiple we know of in the compendium:

```
 S         DATE    EP  E/S   RD                                    PUZZLE     CATEGORY
31  Oct 11 2013  5870  020   R2  SUMMERTIME SUMMERTIME SUM SUM SUMMERTIME  SONG LYRICS
```

There are 11 M's (a undecuple of M's, if you will), but also: 5 S's and U's (two quintuples), 3 T's and R's (two triples), and so on.

With this condition (alias `M`), you can filter puzzles by how many of any combination of singles, doubles ..., etc. there are.

There are three words to this condition, two required:

`mult; letters; mult_frequencies; number_expr`

Where:

* `letters` is the subset of letters to consider when looking through the "frequency chart". `CONSONANT` and `ALL` are shorthand for the obvious.
* `mult_frequencies` is a number expression corresponding to the "frequencies", from 1 (single) to 12 (dodecuple).
* `number_expr` is a generalized number expression of how many of `mult_frequencies` should exist in a puzzle to pass the condition.

Some fun questions can be answered with this condition type. For example, the puzzles with a sextuple any one consonant in the speedup:

`!wc s cond=m;consonant;6 cond=p/e;-2`
```
17 puzzles found in SYNDICATED ... for all of

* number of sextuples of "BCDFGHJKLMNPQRSTVWXYZ" is at least 1
* PUZZLE is the 2nd-to-last of EP (DATE)

 S         DATE    EP  E/S   RD                                   PUZZLE             CATEGORY
 2  May 02 1985  0354  159   R3  THERE'S MORE TO THIS THAN MEETS THE EYE               PHRASE
 7  Oct 25 1989  1208  038   R5                      BUSINESS ASSOCIATES               PEOPLE
12  Dec 01 1994  2209  064   R4   SITTING PRETTY PLEASE WITH SUGAR ON IT       BEFORE & AFTER
14  Apr 07 1997  2681  146   R4            GET TO THE CRUX OF THE MATTER               PHRASE
21  Dec 04 2003  3964  064   R4                   REFRIGERATOR REPAIRMAN           OCCUPATION
22  Sep 28 2004  4112  017   R4               MISS USA AND MISS UNIVERSE               PEOPLE
25  Nov 07 2007  4723  043   R4              THE TOUGHEST TICKET IN TOWN               PHRASE
28  Oct 19 2010  5292  027   R5                 OPTIMISTS AND PESSIMISTS               PEOPLE
28  Oct 21 2010  5294  029   R4                     ATTRACTING ATTENTION  WHAT ARE YOU DOING?
28  Nov 24 2010  5318  053   R4                    RUNNING NECK AND NECK               PHRASE
29  Oct 07 2011  5475  015   R4                ALL'S WELL THAT ENDS WELL               PHRASE
29  Feb 15 2012  5568  108   R4                  STRIKES SPARES & SPLITS          SAME LETTER
29  Mar 06 2012  5582  122   R4                   REFRIGERATOR REPAIRMAN           OCCUPATION
35  May 02 2018  6798  168   R4                     REFRIGERATOR ARTWORK       IN THE KITCHEN
37  Aug 03 2020  7221  158   R4                ALL'S WELL THAT ENDS WELL               PHRASE
38  Oct 27 2020  7262  032   R4                    THE HORROR THE HORROR            QUOTATION
39  Jun 09 2022  7619  194   R5                 DOWN TO THE NITTY-GRITTY           RHYME TIME
```

Or the puzzles with the highest number of doubles or higher:

`!wc s cond=m;ALL;>1;>13`

```
7 puzzles found in SYNDICATED ... for number of greater than singles of "ABCDEFGHIJKLMNOPQRSTUVWXYZ" is greater than 13

 S         DATE    EP  E/S   RD                                                  PUZZLE       CATEGORY           BONUS
18  Sep 19 2000  3327  012   R2   HERE YOU'LL FIND GAUCHOS AND THE CITY OF BUENOS AIRES  WHERE ARE WE?       ARGENTINA
22  Jan 13 2005  4189  094   R3  BRITISH FILM SUPERSPY FOND OF GADGETS AND DRY MARTINIS     WHO IS IT?      JAMES BOND
23  Apr 17 2006  4446  156   R3            CINNAMON SWIRL FRENCH TOAST WITH MAPLE SYRUP    ON THE MENU
23  May 12 2006  4465  175   R3       TEACH YOUR CHILDREN BY CROSBY STILLS NASH & YOUNG    SONG/ARTIST
24  Dec 07 2006  4549  064   R3        GOTHAM CITY SUPERHEROES KNOWN AS THE DYNAMIC DUO  WHO ARE THEY?  BATMAN & ROBIN
37  Sep 11 2019  7023  003   R1       WOODY AND BUZZ LIGHTYEAR RETURN TO THE BIG SCREEN       SHOW BIZ
39  Nov 04 2021  7464  039   R1              LISTEN WITH YOUR HEART YOU WILL UNDERSTAND    SONG LYRICS
```
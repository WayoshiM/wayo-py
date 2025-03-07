import asyncio
import csv
import logging
import os
import re
import string
from io import StringIO
from typing import *

from humanize import ordinal
from more_itertools import value_chain
import polars as pl
import polars.selectors as cs
from requests_html import AsyncHTMLSession, HTMLSession

from dropboxwayo import dropboxwayo
from util_expr import (
    build_int_expression,
    build_date_expression,
    build_dt_q_expression,
    transform_str_to_dts,
)

_log = logging.getLogger('wayo_log')

_HEADER_ROW = ['PUZZLE', 'CATEGORY', 'DATE USED', 'WHEN USED']

# this used to be one big method,
# but for more readability even with repeated code
# made it a simple class


def _extract_trs(tables, *idxs):
    if idxs:
        trs = [tables[i].find('tr') for i in idxs]
    else:
        trs = [t.find('tr') for t in tables]
    if not all(trs):
        raise ValueError('Could not find any rows (`<tr>` HTML elements) in a table.')
    return value_chain(*[tr[1:] for tr in trs])


class CompendiumDownloader:
    def __init__(self, asession: AsyncHTMLSession = None):
        self.asession = asession or AsyncHTMLSession()

    async def _get_tables(self, page_suffix: int | str):
        page = await self.asession.get(f'https://buyavowel.boards.net/page/compendium{page_suffix}')

        if page.status_code != 200:
            raise ValueError(
                f'Could not find webpage on my end for page "{page_suffix}". Getting status code {page.status_code}.'
            )

        tables = page.html.find('div.widget-content.content > table > tbody')
        if not tables:
            raise ValueError(
                f'Could not find puzzle HTML table for compendium page "{page_suffix}". I am looking for this HTML element selector: `div.widget-content.content > table > tbody` (a `div` with both the widget-content and content classes, with a `table` as a direct child, which in turn has a `tbody` as a direct child)'
            )

        return tables

    async def dl_page(self, page_suffix: int | str):
        tables = await self._get_tables(page_suffix)

        match page_suffix:
            case 'primetime':
                trs = _extract_trs(tables, 0)
                s = self._dl_primetime(trs)

                # double duty
                trs = _extract_trs(tables, 1)
                ss = self._dl_choices(trs, True)
                await asyncio.to_thread(dropboxwayo.upload, ss, f'/heroku/wayo-py/compendium/choicesprimetime.csv')

                # triple duty
                trs = _extract_trs(tables, 3)
                ss = self._dl_sched(trs, True)
                await asyncio.to_thread(dropboxwayo.upload, ss, f'/heroku/wayo-py/compendium/schedprimetime.csv')
            case 'kids':
                trs = _extract_trs(tables, 0)
                s = self._dl_kids(trs)
            case 'daytime':
                trs = _extract_trs(tables, 0, 2, 3)
                s = self._dl_au_daytime(trs)
            case 'au':
                trs = _extract_trs(tables, -1)
                s = self._dl_au_daytime(trs)
            case 'gb':
                trs = _extract_trs(tables, -1)
                s = self._dl_gb(trs)
            case 'choices40' | 'choices50':
                trs = _extract_trs(tables)
                s = self._dl_choices(trs, False)
            case 'sched10' | 'sched20' | 'sched30' | 'sched40' | 'sched50':
                trs = _extract_trs(tables)
                s = self._dl_sched(trs, False)
            case _:
                trs = _extract_trs(tables, 0)
                s = self._dl_syndicated(trs, page_suffix)

        p_str = f's{page_suffix:02d}' if type(page_suffix) is int else page_suffix
        await asyncio.to_thread(dropboxwayo.upload, s, f'/heroku/wayo-py/compendium/{p_str}.csv')

    def _dl_syndicated(self, trs, season):
        with StringIO() as s:
            f = csv.writer(s)
            f.writerow(['DATE', 'EP', 'UNC', 'ROUND', 'EXTRA', 'PUZZLE', 'CATEGORY', 'BONUS'])

            for row in trs:
                r = row.find('td')

                puzzle = r[0].text
                if not puzzle:
                    break
                elif puzzle.startswith('***'):  # Katrina puzzle
                    continue

                if season == 25 and r[1].find('i'):
                    category = 'People™'
                else:
                    category = r[1].text

                try:
                    date_used, showno, when_used = [td.text for td in r[2:5]]
                except ValueError:
                    raise ValueError(f'Row with puzzle "{puzzle}" has inaccurate number of columns')

                m = re.match(r'(\d{1,2}/\d{1,2}/\d{2})(\*?)', date_used)
                m2 = re.match(r'\#(\d+)(\*?)', showno)
                if m and m2:
                    date_, uncertain_d = m.groups()
                    showno, uncertain_s = m2.groups()

                    if showno in ('2980', '3946'):  # anniversary clip shows
                        continue

                    uncertain = (
                        'B' if uncertain_d and uncertain_s else (('D' if uncertain_d else '') + ('#' if uncertain_s else ''))
                    )

                    round_ = when_used[:2]
                    round_extra = when_used[2:]

                    if '\n' in puzzle:
                        puzzle, answer = puzzle.split('\n')
                        answer = answer[1:-1]  # no ()
                    else:
                        answer = ''

                    f.writerow([date_, showno, uncertain, round_, round_extra, puzzle, category.upper(), answer.upper()])
                elif (tdt := [td.text for td in r]) != _HEADER_ROW:
                    raise ValueError(f'Row not parseable for syndicated S{season}: {tdt}')

            return s.getvalue().encode()

    def _dl_primetime(self, trs):
        with StringIO() as s:
            f = csv.writer(s)
            f.writerow(['DATE', 'EP', 'HH', 'ROUND', 'EXTRA', 'PUZZLE', 'CATEGORY'])

            for row in trs:
                r = row.find('td')

                puzzle = r[0].text
                if not puzzle:
                    continue

                category = r[1].text
                date_used, showno, when_used = [td.text for td in r[2:5]]

                m = re.match(r'(\d{1,2}/\d{1,2}/\d{2})', date_used)
                m2 = re.match(r'\#(\d+)([AB])', showno)
                if m and m2:
                    date_ = m.group()
                    showno, hh = m2.groups()

                    round_ = when_used[:2]
                    round_extra = when_used[2:]

                    if '\n' in puzzle:
                        puzzle, answer = puzzle.split('\n')
                        answer = answer[1:-1]  # no ()
                    else:
                        answer = ''

                    f.writerow([date_, showno, hh, round_, round_extra, puzzle, category.upper(), answer.upper()])
                # elif (tdt := [td.text for td in r]) != _HEADER_ROW:
                # raise ValueError(f'Row not parseable for {s_str}: {tdt}')

            return s.getvalue().encode()

    def _dl_choices(self, trs, prime):
        with StringIO() as s:
            f = csv.writer(s)
            if prime:
                f.writerow(['DATE', 'EP', 'HH', 'C', 'CAT1', 'CAT2', 'CAT3'])
            else:
                f.writerow(['DATE', 'EP', 'C', 'CAT1', 'CAT2', 'CAT3'])

            for row in trs:
                r = row.find('td')

                if not r[0].text and not r[1].text and not r[2].text:
                    if prime:
                        continue
                    else:
                        break

                date_used = r[-2].text
                m = re.match(r'(\d{1,2}/\d{1,2}/\d{2})', date_used)
                showno = r[-1].text
                m2 = re.match(r'#(\d+)([AB]?)', showno)
                if m and m2:
                    # date_, showno, hh
                    start = [m.group()]
                    start.extend([g for g in m2.groups() if g])
                    cats = []
                    choice = None
                    for c, cat in enumerate(r[:3], 1):
                        cats.append(cat.text.upper())
                        if 'style' in cat.attrs:
                            choice = c
                    if choice == None:
                        raise ValueError(f'BR choice not properly set for {start[0]}')

                    f.writerow(value_chain(start, choice, cats))
                else:
                    raise ValueError(f'Improperly formatted date or showno in BR choices: "{date_used}", "{showno}"')

            return s.getvalue().encode()

    def _dl_sched(self, trs, prime):
        with StringIO() as s:
            f = csv.writer(s)
            header = ['EP', 'DATE', 'RED', 'YELLOW', 'BLUE']
            if not prime:
                header.append('THEME')
            f.writerow(header)

            for row in trs:
                r = row.find('td')

                if not r[2].text:
                    continue

                f.writerow([rr.text if e else rr.text.strip('#') for e, rr in enumerate(r)])

            return s.getvalue().encode()

    def _dl_au_daytime(self, trs):
        with StringIO() as s:
            f = csv.writer(s)
            f.writerow(['DATE', 'ROUND', 'PUZZLE', 'CATEGORY', 'BONUS'])

            for row in trs:
                r = row.find('td')

                puzzle = r[0].text
                category = r[1].text
                date_used, when_used = [td.text for td in r[2:4]]

                if '\n' in puzzle:
                    puzzle, answer = puzzle.split('\n')
                    answer = answer[1:-1]  # no ()
                else:
                    answer = ''
                f.writerow([date_used, when_used.replace('^', ''), puzzle, category.upper(), answer.upper()])

            return s.getvalue().encode()

    def _dl_gb(self, trs):
        with StringIO() as s:
            f = csv.writer(s)
            f.writerow(['DATE', 'ROUND', 'EXTRA', 'PUZZLE', 'CATEGORY'])

            for row in trs:
                r = row.find('td')

                puzzle = r[0].text
                category = r[1].text
                date_used, when_used = [td.text for td in r[2:4]]

                f.writerow([date_used, when_used[:2], when_used[2:], puzzle, category.upper()])

            return s.getvalue().encode()

    def _dl_kids(self, trs):
        with StringIO() as s:
            f = csv.writer(s)
            f.writerow(['DATE', 'ROUND', 'PUZZLE', 'CATEGORY'])

            for row in trs:
                r = row.find('td')

                puzzle = r[0].text
                category = r[1].text
                date_used, when_used = [td.text for td in r[2:4]]

                f.writerow([date_used, when_used, puzzle, category.upper()])

            return s.getvalue().encode()


COL_NAME_REMAPPING = {
    'SEASON': 'S',
    'EPISODE': 'EP',
    'ES': 'E/S',
    'ROUND': 'RD',
    'CAT': 'CATEGORY',
    'P': 'PUZZLE',
    'CB': 'CLUE/BONUS',
    'B': 'CLUE/BONUS',
    'CLUE': 'CLUE/BONUS',
    'BONUS': 'CLUE/BONUS',
    'D': 'DATE',
    'CHOICE': 'CHOSEN',
    'C1': 'CHOICE1',
    'C2': 'CHOICE2',
    'C3': 'CHOICE3',
    'C': 'CHOICE',
    'R': 'RED',
    'Y': 'YELLOW',
    'YEL': 'YELLOW',
    'BL': 'BLUE',
    'T': 'THEME',
}

_letters_mapping = {'CONSONANT': 'BCDFGHJKLMNPQRSTVWXYZ', 'ALL': string.ascii_uppercase}
_word_regex = r"\b[A-Z-'\.]+\b"


def _ordinal_adjust(idx):
    if idx > 0:
        sub_cd = ordinal(idx)
        idx -= 1
    elif idx == 0:
        raise ValueError('"Zeroth" is invalid.')
    else:
        sub_cd = ordinal(-idx) + '-to-last' if idx < -1 else 'last'
    return idx, sub_cd


def build_puzzle_search_expr(options):
    f_exprs = []
    cond_descriptions = []

    if len(options.conditions) > 26:
        raise ValueError("Too many conditions given, max is 26. (You shouldn't need close to this many!)")
    # if options.time not in ('syndicated', 'primetime'):
    # raise ValueError('Only syndicated & primetime supported at the moment.')

    join = None

    for cond in options.conditions:
        words = [w.strip().upper() for w in cond.split(';')]

        match words:
            case ['BONUS' | 'B']:
                if options.time in ('primetime', 'kids', 'gb'):
                    raise ValueError(f'BONUS is invalid in {options.time}.')

                col = 'BONUS' if options.time in ('daytime', 'au') else 'CLUE/BONUS'

                f = (pl.col(col) != '') & (~(pl.col('CATEGORY') == 'CROSSWORD'))
                cd = 'has a bonus'
            case ['BONUS' | 'B' as col, '1' | '0' | 'YES' | 'NO' | 'Y' | 'N' | 'T' | 'F' | 'TRUE' | 'FALSE' as b]:
                if options.time in ('primetime', 'kids', 'gb'):
                    raise ValueError(f'BONUS is invalid in {options.time}.')

                col = 'BONUS' if options.time in ('daytime', 'au') else 'CLUE/BONUS'
                b = re.match('[1YT]', b)

                f = (pl.col('BONUS') != '') & (~(pl.col('CATEGORY') == 'CROSSWORD'))
                if not b:
                    f = f.not_()
                cd = 'has a bonus' if b else 'does not have a bonus'
            case [
                'PUZZLE'
                | 'P'
                | 'CLUE/BONUS'
                | 'CLUE'
                | 'BONUS'
                | 'CB'
                | 'B'
                | 'RD'
                | 'R'
                | 'ROUND'
                | 'CAT'
                | 'CATEGORY' as col,
                lit,
                'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as p_q,
            ]:
                col = COL_NAME_REMAPPING.get(col, col)
                if col == 'CLUE/BONUS':
                    if options.time in ('primetime', 'kids', 'gb'):
                        raise ValueError(f'{col} is invalid in {options.time}.')
                    elif options.time in ('daytime', 'au'):
                        col = 'BONUS'

                if p_q.startswith('L'):
                    f = pl.col(col).cast(str).str.contains(lit, literal=True)
                    cd = f'{col} contains "{lit}"'
                else:
                    f = pl.col(col).cast(str) == lit
                    cd = f'{col} is exactly "{lit}"'
            case ['PUZZLE' | 'P' | 'CLUE/BONUS' | 'CLUE' | 'BONUS' | 'CB' | 'B' as col, regex, *e]:
                col = COL_NAME_REMAPPING.get(col, col)
                if col == 'CLUE/BONUS':
                    if options.time in ('primetime', 'kids', 'gb'):
                        raise ValueError(f'{col} is invalid in {options.time}.')
                    elif options.time in ('daytime', 'au'):
                        col = 'BONUS'

                if options.time == 'primetime' and col != 'PUZZLE':
                    raise ValueError(f'{col} is invalid in primetime.')
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)

                if e:
                    f, cd, p = build_int_expression(pl.col(col).str.count_matches(regex), e)
                    cd = f'{col} matches "{regex}" {cd} time' + ('s' if p else '')
                else:
                    f = pl.col(col).str.contains(regex)
                    cd = f'{col} matches "{regex}"'
            case ['RD' | 'ROUND' | 'R' | 'CAT' | 'CATEGORY' as col, regex]:
                col = COL_NAME_REMAPPING.get(col, col)
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
                if col == 'CATEGORY' and ':tm:' in col:
                    regex = regex.replace(':tm:', '™️')

                f = pl.col(col).cast(str).str.contains(regex)
                cd = f'{col} matches "{regex}"'
            case ['PUZZLE/EP' | 'PUZ/EP' | 'P/E' | 'PE', idx]:
                idx, sub_cd = _ordinal_adjust(int(idx))
                f = pl.col('RD') == pl.col('RD').cast(str).implode().over(pl.col('DATE')).list.get(idx)
                cd = f'PUZZLE is the {sub_cd} of EP (DATE)'
            case ['SEASON' | 'S' | 'EPISODE' | 'EP' | 'ES' | 'E/S' as col, *e]:
                col = COL_NAME_REMAPPING.get(col, col)
                if options.time != 'syndicated' and not (options.time == 'primetime' and col == 'EP'):
                    raise ValueError(f'{col} is invalid in {options.time}.')

                f, cd, _ = build_int_expression(pl.col(col), e)
                cd = f'{col} is {cd}'
            case ['HH', 'A' | 'B' as hh]:
                if options.time != 'primetime':
                    raise ValueError('HH is only valid in primetime.')
                f = pl.col('HH') == hh
                cd = f'HH is {hh}'
            case [
                'DATE' | 'D' as col,
                'YEAR' | 'Y' | 'MONTH' | 'M' | 'DAY' | 'D' | 'DOW' | 'WKDAY' | 'WEEKDAY' as dt_q,
                *e,
            ]:
                if options.time in ('kids', 'daytime', 'au', 'gb'):
                    raise ValueError(f'DATE is too incomplete to search specifics on for {options.time}.')
                col = COL_NAME_REMAPPING.get(col, col)
                f, cd = build_dt_q_expression(col, dt_q, e)
            case ['DATE' | 'D' as col, *e]:
                col = COL_NAME_REMAPPING.get(col, col)
                if options.time in ('kids', 'daytime', 'au', 'gb'):
                    regex = ' '.join(e)
                    f = pl.col(col).str.contains(regex)
                    cd = f'{col} matches "{regex}"'
                else:
                    e = transform_str_to_dts(e, options.dateFormat)
                    f, cd = build_date_expression(pl.col(col), e, options.dateFormat)
                    cd = f'{col} is {cd}'
            case ['LENGTH' | 'LC' | 'L', *e]:
                f, cd, _ = build_int_expression(pl.col('PUZZLE').str.extract_all('[A-Z]').list.len(), e)
                cd = f'length is {cd}'
            case ['LENGTH_UNIQUE' | 'LCU' | 'LU', *e]:
                f, cd, _ = build_int_expression(pl.col('PUZZLE').str.extract_all('[A-Z]').list.unique().list.len(), e)
                cd = f'total number of unique letters is {cd}'
            case ['COUNT' | 'C' | 'COUNT_UNIQUE' | 'CU' as col, letters, *e]:
                if letters in _letters_mapping:
                    letters = _letters_mapping[letters]
                elif not re.match('[A-Z]+', letters) or not len(set(letters)) == len(letters):
                    raise ValueError(f'Malformed letter string (must be all A-Z and all unique): {letters}')

                base_expr = pl.col('PUZZLE').str.extract_all('[A-Z]')
                if 'U' in col:
                    base_expr = base_expr.list.unique()
                    extra = ' unique'
                else:
                    extra = ''

                f, cd, _ = build_int_expression(
                    base_expr.list.eval(pl.element().is_in(list(letters)), parallel=True).list.sum(),
                    e,
                )
                cd = f'total{extra} number of {letters} is {cd}'
            case ['WORD_COUNT' | 'WC', *e]:
                f, cd, _ = build_int_expression(pl.col('PUZZLE').str.count_matches(_word_regex), e)
                cd = f'total word count is {cd}'
            case ['WORD' | 'W', regex]:
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
                f = (
                    pl.col('PUZZLE')
                    .str.extract_all(_word_regex)
                    .list.eval(pl.element().str.contains(regex), parallel=True)
                    .list.contains(True)
                )
                cd = f'any word matches "{regex}"'
            case ['WORD' | 'W', word, 'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as w_q]:
                if w_q.startswith('L'):
                    f = (
                        pl.col('PUZZLE')
                        .str.extract_all(_word_regex)
                        .list.eval(pl.element().str.contains(word, literal=True), parallel=True)
                        .list.contains(True)
                    )
                    cd = f'any word contains "{word}"'
                else:
                    f = pl.col('PUZZLE').str.extract_all(_word_regex).list.contains(word)
                    cd = f'any word is exactly "{word}"'
            case ['WORD' | 'W', regex, idx]:
                idx = int(idx)
                if idx > 0:
                    sub_cd = ordinal(idx)
                    idx -= 1
                elif idx == 0:
                    raise ValueError('Zeroth word is not defined.')
                else:
                    sub_cd = ordinal(-idx) + '-to-last' if idx < -1 else 'last'
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)

                f = pl.col('PUZZLE').str.extract_all(_word_regex).list.get(idx).str.contains(regex)
                cd = f'{sub_cd} word matches "{regex}"'
            case ['WORD' | 'W', word, 'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as w_q, idx]:
                idx, sub_cd = _ordinal_adjust(int(idx))

                base_expr = pl.col('PUZZLE').str.extract_all(_word_regex).list.get(idx)

                if w_q.startswith('L'):
                    f = base_expr.str.contains(word, literal=True)
                    cd = f'{sub_cd} word contains "{word}"'
                else:
                    f = base_expr == word
                    cd = f'{sub_cd} word is exactly "{word}"'
            case ['MULT' | 'M', letters, mults, *e]:
                if letters in _letters_mapping:
                    letters = _letters_mapping[letters]
                elif not re.match('[A-Z]+', letters) or not len(set(letters)) == len(letters):
                    raise ValueError(f'Malformed letter string (must be all A-Z and all unique): {letters}')

                try:
                    _, mult_cd, _ = build_int_expression(pl.col('DUMMY'), [mults], mult_hybrid=True)
                except KeyError:
                    raise ValueError('All multiples must be between 1 (single) and 12 (dodecuple).')

                if not e:
                    e = ['>=1']

                f, cd, _ = build_int_expression(
                    pl.col('_lc')
                    .list.eval(
                        (pl.element().struct.field('').is_in(list(letters)))
                        & (build_int_expression(pl.element().struct.field('count'), [mults], expr_only=True)),
                        parallel=True,
                    )
                    .list.sum(),
                    e,
                )
                cd = f'number of {mult_cd} of "{letters}" is {cd}'
            case ['PP' | 'PR' | 'RL' as col]:
                if options.time != 'syndicated' and not (
                    (options.time == 'primetime' and col == 'PP') or (options.time == 'gb' and col == 'PR')
                ):
                    raise ValueError(f'{col} is invalid in {options.time}.')
                f = pl.col(col)
                cd = f'is a {col} puzzle'
            case [
                'PP' | 'PR' | 'RL' as col,
                '1' | '0' | 'YES' | 'NO' | 'Y' | 'N' | 'T' | 'F' | 'TRUE' | 'FALSE' as b,
            ]:
                if options.time != 'syndicated' and not (
                    (options.time == 'primetime' and col == 'PP') or (options.time == 'gb' and col == 'PR')
                ):
                    raise ValueError(f'{col} is invalid in {options.time}.')

                b = re.match('[1YT]', b)

                f = pl.col(col) if b else pl.col(col).not_()
                cd = f'is a {col} puzzle' if b else f'is not a {col} puzzle'
            case ['UC' as col]:
                if options.time != 'syndicated':
                    raise ValueError(f'{col} is invalid in {options.time}.')

                f = pl.col(col) != ''
                cd = f'has some uncertainty in date and/or ep #'
            case [
                'UC' as col,
                '1' | '0' | 'YES' | 'NO' | 'Y' | 'N' | 'T' | 'F' | 'TRUE' | 'FALSE' as b,
            ]:
                if options.time != 'syndicated':
                    raise ValueError(f'{col} is invalid in {options.time}.')

                b = re.match('[1YT]', b)

                f = pl.col(col) != '' if b else pl.col(col) == ''
                cd = f'has some uncertainty in date and/or ep #' if b else f'has no uncertainty in date and/or ep #'
            case ['UC' as col, word]:
                if options.time != 'syndicated':
                    raise ValueError(f'{col} is invalid in {options.time}.')

                m = re.match('[D#B]+', word.upper())
                if not m:
                    raise ValueError(f'Invalid uncertainty string: {word}')

                s = set(word.upper())

                f = pl.col(col).is_in(s)
                cd = f'has uncertainty ' + ((' or '.join(word)) if len(s) > 1 else word[0])
            case _:
                f, cd = gen_sched_expr(words, options)
                if options.time in ('syndicated', 'primetime'):
                    join = 'sched'
                else:
                    raise ValueError(f'{col} is invalid in {options.time}.')

        f_exprs.append(f)
        cond_descriptions.append(cd)

    if options.logicExpr == 'all':
        total_expr = pl.all_horizontal(f_exprs)
        expr_str = ' all of'
    elif options.logicExpr == 'any':
        total_expr = pl.any_horizontal(f_exprs)
        expr_str = ' any of'
    else:
        l = locals()
        l |= {letter: fe for fe, letter in zip(f_exprs, string.ascii_uppercase)}
        total_expr = eval(re.sub('([A-Z])', r'(\1)', options.logicExpr))
        expr_str = f'\n{options.logicExpr}; where'

    return total_expr, expr_str, cond_descriptions, join


def build_choices_search_expr(options):
    f_exprs = []
    cond_descriptions = []

    if len(options.conditions) > 26:
        raise ValueError("Too many conditions given, max is 26. (You shouldn't need close to this many!)")

    for cond in options.conditions:
        words = [w.strip().upper() for w in cond.split(';')]

        match words:
            case [
                'PUZZLE' | 'CHOSEN' | 'CHOICE1' | 'C1' | 'CHOICE2' | 'C2' | 'CHOICE3' | 'C3' as col,
                lit,
                'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as p_q,
            ]:
                col = COL_NAME_REMAPPING.get(col, col)

                if p_q.startswith('L'):
                    f = pl.col(col).cast(str).str.contains(lit, literal=True)
                    verb = 'contains'
                else:
                    f = pl.col(col).cast(str).str == lit
                    verb = 'is exactly'
                if re.match('[123]', col[-1]):
                    col = ordinal(col[-1]) + ' CHOICE'
                cd = f'{col} {verb} "{lit}"'
            case ['PUZZLE' | 'P' as col, regex, *e]:
                col = COL_NAME_REMAPPING.get(col, col)

                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)

                if e:
                    f, cd, p = build_int_expression(pl.col(col).str.count_matches(regex), e)
                    cd = f'{col} matches "{regex}" {cd} time' + ('s' if p else '')
                else:
                    f = pl.col(col).str.contains(regex)
                    cd = f'{col} matches "{regex}"'
            case ['CHOSEN' | 'CHOICE1' | 'C1' | 'CHOICE2' | 'C2' | 'CHOICE3' | 'C3' as col, regex]:
                col = COL_NAME_REMAPPING.get(col, col)
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
                f = pl.col(col).cast(str).str.contains(regex)
                if col != 'CHOSEN':
                    col = ordinal(col[-1]) + ' CHOICE'
                cd = f'{col} matches "{regex}"'
            case ['C' | 'CHOICE', regex]:
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
                f = pl.any_horizontal(cs.matches('^CHOICE\d$').cast(str).str.contains(regex))
                cd = f'any CHOICE matches "{regex}"'
            case ['C' | 'CHOICE', lit, 'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as p_q]:
                if p_q.startswith('L'):
                    f = pl.any_horizontal(cs.matches('^CHOICE\d$').cast(str).str.contains(lit, literal=True))
                    verb = 'contains'
                else:
                    f = pl.any_horizontal(cs.matches('^CHOICE\d$') == lit)
                    verb = 'is exactly'
                if re.match('[123]', col[-1]):
                    col = ordinal(col[-1]) + ' CHOICE'
                cd = f'{col} {verb} "{lit}"'
            case ['UC' | 'UNCHOSEN' | 'NC' | 'NOTCHOSEN' | 'NOT_CHOSEN', regex]:
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
                f = pl.all_horizontal(
                    pl.any_horizontal(cs.matches('^CHOICE\d$').cast(str).str.contains(regex)),
                    (~pl.col('CHOSEN').cast(str).str.contains(regex)),
                )
                cd = f'any CHOICE matches "{regex}" but CHOSEN does not match "{regex}"'
            case [
                'UC' | 'UNCHOSEN' | 'NC' | 'NOTCHOSEN' | 'NOT_CHOSEN',
                lit,
                'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as p_q,
            ]:
                if p_q.startswith('L'):
                    f = pl.all_horizontal(
                        pl.any_horizontal(cs.matches('^CHOICE\d$').cast(str).str.contains(lit, literal=True)),
                        (~pl.col('CHOSEN').cast(str).str.contains(lit, literal=True)),
                    )
                    verb = 'contains'
                    verb2 = 'does not contain'
                else:
                    f = pl.all_horizontal(pl.any_horizontal(cs.matches('^CHOICE\d$') == lit), pl.col('CHOSEN') != lit)
                    verb = 'is exactly'
                    verb2 = 'is not exactly'
                if re.match('[123]', col[-1]):
                    col = ordinal(col[-1]) + ' CHOICE'
                cd = f'any CHOICE {verb} "{lit}" but CHOSEN {verb2} "{lit}"'
            case ['SEASON' | 'S' | 'EPISODE' | 'EP' | 'ES' | 'E/S' | 'POS' as col, *e]:
                col = COL_NAME_REMAPPING.get(col, col)
                if options.time == 'primetime' and col not in ('C', 'EP'):
                    raise ValueError(f'{col} is invalid in {options.time}.')

                f, cd, _ = build_int_expression(pl.col(col), e)
                cd = f'{col} is {cd}'
            case ['HH', 'A' | 'B' as hh]:
                if options.time != 'primetime':
                    raise ValueError('HH is only valid in primetime.')
                f = pl.col('HH') == hh
                cd = f'HH is {hh}'
            case [
                'DATE' | 'D' as col,
                'YEAR' | 'Y' | 'MONTH' | 'M' | 'DAY' | 'D' | 'DOW' | 'WKDAY' | 'WEEKDAY' as dt_q,
                *e,
            ]:
                if options.time in ('kids', 'daytime', 'au', 'gb'):
                    raise ValueError(f'DATE is too incomplete to search specifics on for {options.time}.')
                col = COL_NAME_REMAPPING.get(col, col)
                f, cd = build_dt_q_expression(col, dt_q, e)
            case ['DATE' | 'D' as col, *e]:
                col = COL_NAME_REMAPPING.get(col, col)
                e = transform_str_to_dts(e, options.dateFormat)
                f, cd = build_date_expression(pl.col(col), e, options.dateFormat)
                cd = f'{col} is {cd}'
            case ['LENGTH' | 'LC' | 'L', *e]:
                f, cd, _ = build_int_expression(pl.col('PUZZLE').str.extract_all('[A-Z]').list.len(), e)
                cd = f'length is {cd}'
            case ['LENGTH_UNIQUE' | 'LCU' | 'LU', *e]:
                f, cd, _ = build_int_expression(pl.col('PUZZLE').str.extract_all('[A-Z]').list.unique().list.len(), e)
                cd = f'total number of unique letters is {cd}'
            case ['COUNT' | 'C' | 'COUNT_UNIQUE' | 'CU' as col, letters, *e]:
                if letters in _letters_mapping:
                    letters = _letters_mapping[letters]
                elif not re.match('[A-Z]+', letters) or not len(set(letters)) == len(letters):
                    raise ValueError(f'Malformed letter string (must be all A-Z and all unique): {letters}')

                base_expr = pl.col('PUZZLE').str.extract_all('[A-Z]')
                if 'U' in col:
                    base_expr = base_expr.list.unique()
                    extra = ' unique'
                else:
                    extra = ''

                f, cd, _ = build_int_expression(
                    base_expr.list.eval(pl.element().is_in(list(letters)), parallel=True).list.sum(),
                    e,
                )
                cd = f'total{extra} number of {letters} is {cd}'
            case ['WORD_COUNT' | 'WC', *e]:
                f, cd, _ = build_int_expression(pl.col('PUZZLE').str.count_matches(_word_regex), e)
                cd = f'total word count is {cd}'
            case ['WORD' | 'W', regex]:
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
                f = (
                    pl.col('PUZZLE')
                    .str.extract_all(_word_regex)
                    .list.eval(pl.element().str.contains(regex), parallel=True)
                    .list.contains(True)
                )
                cd = f'any word matches "{regex}"'
            case ['WORD' | 'W', word, 'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as w_q]:
                if w_q.startswith('L'):
                    f = (
                        pl.col('PUZZLE')
                        .str.extract_all(_word_regex)
                        .list.eval(pl.element().str.contains(word, literal=True), parallel=True)
                        .list.contains(True)
                    )
                    cd = f'any word contains "{word}"'
                else:
                    f = pl.col('PUZZLE').str.extract_all(_word_regex).list.contains(word)
                    cd = f'any word is exactly "{word}"'
            case ['WORD' | 'W', regex, idx]:
                idx = int(idx)
                if idx > 0:
                    sub_cd = ordinal(idx)
                    idx -= 1
                elif idx == 0:
                    raise ValueError('Zeroth word is not defined.')
                else:
                    sub_cd = ordinal(-idx) + '-to-last' if idx < -1 else 'last'
                regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)

                f = pl.col('PUZZLE').str.extract_all(_word_regex).list.get(idx).str.contains(regex)
                cd = f'{sub_cd} word matches "{regex}"'
            case ['WORD' | 'W', word, 'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as w_q, idx]:
                idx, sub_cd = _ordinal_adjust(int(idx))

                base_expr = pl.col('PUZZLE').str.extract_all(_word_regex).list.get(idx)

                if w_q.startswith('L'):
                    f = base_expr.str.contains(word, literal=True)
                    cd = f'{sub_cd} word contains "{word}"'
                else:
                    f = base_expr == word
                    cd = f'{sub_cd} word is exactly "{word}"'
            case ['MULT' | 'M', letters, mults, *e]:
                if letters in _letters_mapping:
                    letters = _letters_mapping[letters]
                elif not re.match('[A-Z]+', letters) or not len(set(letters)) == len(letters):
                    raise ValueError(f'Malformed letter string (must be all A-Z and all unique): {letters}')

                try:
                    _, mult_cd, _ = build_int_expression(pl.col('DUMMY'), [mults], mult_hybrid=True)
                except KeyError:
                    raise ValueError('All multiples must be between 1 (single) and 12 (dodecuple).')

                if not e:
                    e = ['>=1']

                f, cd, _ = build_int_expression(
                    pl.col('_lc')
                    .list.eval(
                        (pl.element().struct.field('').is_in(list(letters)))
                        & (build_int_expression(pl.element().struct.field('count'), [mults], expr_only=True)),
                        parallel=True,
                    )
                    .list.sum(),
                    e,
                )
                cd = f'number of {mult_cd} of "{letters}" is {cd}'
            case _:
                raise ValueError(f'Malformed condition: {cond}')

        f_exprs.append(f)
        cond_descriptions.append(cd)

    if options.logicExpr == 'all':
        total_expr = pl.all_horizontal(f_exprs)
        expr_str = ' all of'
    elif options.logicExpr == 'any':
        total_expr = pl.any_horizontal(f_exprs)
        expr_str = ' any of'
    else:
        l = locals()
        l |= {letter: fe for fe, letter in zip(f_exprs, string.ascii_uppercase)}
        total_expr = eval(re.sub('([A-Z])', r'(\1)', options.logicExpr))
        expr_str = f'\n{options.logicExpr}; where'

    return total_expr, expr_str, cond_descriptions


def gen_sched_expr(words, options):
    match words:
        case [
            'RED' | 'R' | 'YELLOW' | 'Y' | 'YEL' | 'BLUE' | 'BL' | 'THEME' | 'T' as col,
            lit,
            'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as p_q,
        ]:
            col = COL_NAME_REMAPPING.get(col, col)
            if col == 'THEME' and options.time == 'primetime':
                raise ValueError('THEME is not a column in primetime.')

            if p_q.startswith('L'):
                f = pl.col(col).cast(str).str.contains(lit.title(), literal=True)
                verb = 'contains'
            else:
                f = pl.col(col).cast(str).str == lit.title()
                verb = 'is exactly'
            cd = f'{col} {verb} "{lit.title()}"'
        case [
            'CONTESTANT' | 'CON' | 'PODIUM' | 'P',
            lit,
            'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as p_q,
        ]:
            if p_q.startswith('L'):
                f = pl.any_horizontal(pl.col('RED', 'YELLOW', 'BLUE').str.contains(lit.title(), literal=True))
                verb = 'contains'
            else:
                f = pl.any_horizontal(pl.col('RED', 'YELLOW', 'BLUE').cast(str) == lit.title())
                verb = 'is exactly'
            cd = f'any PODIUM {verb} "{lit.title()}"'
        case ['RED' | 'R' | 'YELLOW' | 'Y' | 'YEL' | 'BLUE' | 'BL' | 'THEME' | 'T' as col, regex]:
            col = COL_NAME_REMAPPING.get(col, col)
            if col == 'THEME' and options.time == 'primetime':
                raise ValueError('THEME is not a column in primetime.')
            regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
            f = pl.col(col).cast(str).str.contains(f'(?i){regex}')
            cd = f'{col} matches "{regex}"  (case-insensitive)'
        case ['CONTESTANT' | 'CON' | 'PODIUM' | 'P', regex]:
            regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
            f = pl.any_horizontal(pl.col('RED', 'YELLOW', 'BLUE').str.contains(f'(?i){regex}'))
            cd = f'any PODIUM matches "{regex}" (case-insensitive)'
        case ['SEASON' | 'S' | 'EPISODE' | 'EP' | 'ES' | 'E/S' as col, *e]:
            col = COL_NAME_REMAPPING.get(col, col)
            if options.time == 'primetime' and col != 'EP':
                raise ValueError(f'{col} is invalid in {options.time}.')

            f, cd, _ = build_int_expression(pl.col(col), e)
            cd = f'EP is {cd}'
        case [
            'DATE' | 'D' as col,
            'YEAR' | 'Y' | 'MONTH' | 'M' | 'DAY' | 'D' | 'DOW' | 'WKDAY' | 'WEEKDAY' as dt_q,
            *e,
        ]:
            col = COL_NAME_REMAPPING.get(col, col)
            f, cd = build_dt_q_expression(col, dt_q, e)
        case ['DATE' | 'D' as col, *e]:
            col = COL_NAME_REMAPPING.get(col, col)
            e = transform_str_to_dts(e, options.dateFormat)
            f, cd = build_date_expression(pl.col(col), e, options.dateFormat)
            cd = f'{col} is {cd}'
        case ['DATE_STR' | 'DS', lit, 'LITERAL' | 'LIT' | 'L' | 'EXACT' | 'E' as p_q]:
            if options.time == 'primetime':
                raise ValueError('There are no uncertain dates in prinmetime.')
            if p_q.startswith('L'):
                f = pl.all_horizontal(pl.col('DATE').is_null(), pl.col('DATE_STR').str.contains(lit.title(), literal=True))
                verb = 'contains'
            else:
                f = pl.all_horizontal(pl.col('DATE').is_null(), pl.col('DATE_STR') == lit.title())
                verb = 'is exactly'
            cd = f'DATE is uncertain and {verb} "{lit.title()}"'
        case ['DATE_STR' | 'DS', regex]:
            if options.time == 'primetime':
                raise ValueError('There are no uncertain dates in prinmetime.')
            regex = re.sub(r'\\\w', lambda m: m.group().lower(), regex)
            f = pl.all_horizontal(pl.col('DATE').is_null(), pl.col('DATE_STR').str.contains(f'(?i){regex}'))
            cd = f'DATE is uncertain and matches "{regex}" (case-insensitive)'
        case _:
            raise ValueError(f'Malformed condition: {words}')

    return f, cd


def build_sched_search_expr(options):
    f_exprs = []
    cond_descriptions = []

    if len(options.conditions) > 26:
        raise ValueError("Too many conditions given, max is 26. (You shouldn't need close to this many!)")

    for cond in options.conditions:
        words = [w.strip().upper() for w in cond.split(';')]

        f, cd = gen_sched_expr(words, options)

        f_exprs.append(f)
        cond_descriptions.append(cd)

    if options.logicExpr == 'all':
        total_expr = pl.all_horizontal(f_exprs)
        expr_str = ' all of'
    elif options.logicExpr == 'any':
        total_expr = pl.any_horizontal(f_exprs)
        expr_str = ' any of'
    else:
        l = locals()
        l |= {letter: fe for fe, letter in zip(f_exprs, string.ascii_uppercase)}
        total_expr = eval(re.sub('([A-Z])', r'(\1)', options.logicExpr))
        expr_str = f'\n{options.logicExpr}; where'

    return total_expr, expr_str, cond_descriptions

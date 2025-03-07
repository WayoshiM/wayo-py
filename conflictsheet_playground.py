from conflictsheet import *
import io
import os
import datetime
from pg import PG

pl.toggle_string_cache(True)

(
    pl.cfg.Config.set_tbl_cols(-1)
    .set_tbl_rows(-1)
    .set_tbl_width_chars(1_000)
    .set_fmt_str_lengths(100)
    .set_tbl_formatting('NOTHING')
    .set_tbl_cell_alignment('RIGHT')
    .set_tbl_hide_dataframe_shape()
    .set_tbl_hide_column_data_types()
    .set_tbl_hide_dtype_separator()
)


# print(datetime.datetime.now())
def sf(iob, fn):
    with open(os.path.expanduser('~/Dropbox/heroku/wayo-py/' + fn), 'wb') as f:
        f.write(iob.read())


cs = ConflictSheet(lambda fn: open(os.path.expanduser('~/Dropbox/heroku/wayo-py/' + fn), 'rb'), sf, False)
# print(datetime.datetime.now())

x = (
    cs.slot_table('daytime', None)
    # .filter(pl.col('PG').is_in([str(pg) for pg in PG.partition_table['2 PRIZER']]))
    .collect()
)
# .filter(~has_any_flags('flag', UNCERTAIN_FLAGS))
v = (
    x.lazy()
    .filter(~has_any_flags('flag', frozenset({2**7, 2**8})))
    .groupby('PG')
    .agg(pl.exclude('flag').sum())
    .select(pl.col('^PG1?$').sort_by('PG1', True).head(3))
)
# print(v.collect())
vv = (
    x.lazy()
    .filter(~has_any_flags('flag', frozenset({2**7, 2**8})))
    .groupby('PG')
    .agg(pl.exclude('flag').sum())
    .select([pl.col('PG'), pl.fold(pl.lit(0), operator.add, pl.col(f'^PG[12]$')).alias('sum')])
    .select(pl.all().sort_by('sum', True).head(3))
)
print(vv.collect())

pgQ = PG.FivePriceTags
isPGGroup = False
fe = [pl.col('PG').is_in([str(pg) for pg in pgQ]) if isPGGroup else pl.col('PG') == str(pgQ)]
# fe.append(pl.col('flag').is_in(options.pgFlags))

xx = (
    x.lazy()
    .filter(pl.all(fe))
    .select(pl.exclude('PG'))
    .groupby('flag')
    .agg(pl.exclude('flag').sum())
    .with_columns(pl.concat_list(pl.exclude('flag')).arr.sum().alias('ALL'))
)

# print(xx.collect())
by = 17
xxx = (
    x.lazy()
    .filter(pl.all(fe))
    .select(pl.exclude('PG'))
    .groupby([(pl.col('S').rank('dense') - 1) // by, 'flag'])
    .agg(pl.exclude('S').sum())
)
xxxx = (
    xxx.select(
        [pl.col('S')]
        + [
            pl.when(pl.col('flag') == sc)
            .then(pl.concat_list(pl.col('^PG\d$')).arr.sum())
            .otherwise(pl.lit(0))
            .alias(f'PG{f}')
            for sc, f in zip((SlotCertainty.SLOT, SlotCertainty.GAME), '^?')
        ]
    )
    .groupby('S')
    .agg(pl.all().sum())
)
xxxxx = (
    xxx.filter(~pl.col('flag').is_between(SlotCertainty.SLOT, SlotCertainty.GAME, True))
    .join(xxxx, on='S')
    .groupby('S')
    .agg(pl.exclude('flag').sum())
    .sort('S')
    .with_columns([pl.Series('S', ['S1-17', 'S18-34', 'S35-51']), pl.concat_list(pl.exclude('S')).arr.sum().alias('ALL')])
)
x6 = pl.concat([xxxxx, xxxxx.select([pl.lit('ALL').alias('S'), pl.exclude('S').sum()])])
# print(x6.rename({'S': ''}).collect())
# quit()

y = cs.concurrence_query(P.closed(46, 50), 'daytime', (PG.Plinko, PG.RangeGame), (frozenset((8,)), frozenset((1, 16))))
print(y.collect())
# print(y.select(pl.concat_list('^PG\d_p$').alias('PG').explode().value_counts(True, True)).unnest('PG').collect())

w = cs.gen_sheet(
    [
        PGPlaying(pg.sheetName, 0, pg)
        for pg in (PG.GrandGame, PG.DoublePrices, PG.ThreeStrikes, PG.FlipFlop, PG.Cliffhangers, PG.Bonkers)
    ],
    P.closed(47, 51),
    'S47-51',
    'daytime',
)
# print(w)

# print(cs.get('daytime').select(pl.arg_where(pl.col('PROD') == '9905K')).to_series()[0])
(a,) = (
    cs.get('daytime')
    .filter(pl.col('S') > CURRENT_SEASON - 1)
    .select(pl.col('S').value_counts(True))
    .unnest('S')
    .select(pl.col('counts'))
)
(b,) = a
# print(a)
# print(b)
# b, c = a
# print(type(b))

#########################
#   DATASET BICICLETTE  #
#########################

# TOTALE DELLE CORSE PER GIORNO DELLA SETTIMANA

import preprocessing as pre
import dfplot
import polars as pl
import opendp.prelude as dp
import math

dp.enable_features("contrib", "floating-point")

schema_overrides = {
    "start_station_id": pl.Utf8,
    "end_station_id": pl.Utf8,
    "started_at": pl.Datetime(time_unit="ms"), 
    "ended_at": pl.Datetime(time_unit="ms")
}

#original dataset
bike = pl.read_csv(
    "bike/tripdata.csv",
    schema_overrides=schema_overrides
)
bike = pre.cast_enum(bike)

#sample dei dati
num = bike.height
#num = 1000
#bike = bike.sample(n = num, with_replacement = False, seed = 1234)

#DP dataset
df = bike.lazy()

eps = 0.01

trips_weekday = bike.group_by("weekday").len()
trips_weekday = trips_weekday.sort("weekday")
trips_weekday = trips_weekday.with_columns(
    pl.col("weekday").cast(pl.Utf8),
    pl.col("len").cast(pl.Int64))


#difference
week_diff = trips_weekday.join(trips_weekday, how = "cross")
week_diff = week_diff.filter(pl.col("weekday") < pl.col("weekday_right"))
week_diff = week_diff.with_columns((abs(pl.col("len") - pl.col("len_right"))).alias("diff"))
week_diff = week_diff.select(["weekday", "weekday_right", "len", "len_right", "diff"])
min_diff = week_diff.select(pl.col("diff").min())
max_diff = week_diff.select(pl.col("diff").max())
mean_diff = week_diff.select(pl.col("diff").mean())


#invariant keys - giorni della settimana sono pubblici
context = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=eps),
    split_evenly_over = 1,
    margins = [dp.polars.Margin(by=["weekday"], public_info="keys")]            
)

query_counts = (
    context.query()
    .group_by("weekday")
    .agg(dp.len())
)
result = query_counts.release().collect().sort("weekday")
result = result.with_columns(pl.col("len").cast(pl.Int64))


#difference DP
week_diffDP = result.join(result, how = "cross")
week_diffDP = week_diffDP.filter(pl.col("weekday") < pl.col("weekday_right"))
week_diffDP = week_diffDP.with_columns((abs(pl.col("len") - pl.col("len_right"))).alias("diff"))
week_diffDP = week_diffDP.select(["weekday", "weekday_right", "len", "len_right", "diff"])
min_diffDP = week_diffDP.select(pl.col("diff").min())
max_diffDP = week_diffDP.select(pl.col("diff").max())
mean_diffDP = week_diffDP.select(pl.col("diff").mean())


print(f"Totale corse per giorno della settimana: {trips_weekday}")
print(f"Totale corse per giorno della settimana con DP: {result}")
print(query_counts.summarize(alpha=0.05))

print(f"Differenza (MIN, MEAN e MAX) tra i conteggi per giorno della settimana: {[min_diff.item(), mean_diff.item(), max_diff.item()]}")
print(f"Differenza (MIN, MEAN e MAX) tra i conteggi per giorno della settimana con DP: {[min_diffDP.item(), mean_diffDP.item(), max_diffDP.item()]}")





#crea grafico
dfplot.plot_total_rides_comparison(trips_weekday, result, eps, num)



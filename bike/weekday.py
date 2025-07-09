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
num = 200000
bike = bike.sample(n = num, with_replacement = False, seed = 1234)

#DP dataset
df = bike.lazy()

trips_weekday = bike.group_by("weekday").len()
trips_weekday = trips_weekday.sort("weekday")
trips_weekday = trips_weekday.with_columns(pl.col("len").cast(pl.Int64))


#difference
week_diff = trips_weekday.join(trips_weekday, how = "cross")
week_diff = week_diff.filter(pl.col("weekday") < pl.col("weekday_right"))
week_diff = week_diff.with_columns((abs(pl.col("len") - pl.col("len_right"))).alias("diff"))
week_diff = week_diff.select(["weekday", "weekday_right", "len", "len_right", "diff"])
min_diff = week_diff.select(pl.col("diff").min())
max_diff = week_diff.select(pl.col("diff").max())
mean_diff = week_diff.select(pl.col("diff").mean())

print(f"Totale corse per giorno della settimana: {trips_weekday}")
print(f"Differenza (MIN, MEAN e MAX) tra i conteggi per giorno della settimana: {[min_diff.item(), mean_diff.item(), max_diff.item()]}")

'''filename = f"bike/samplecsv/diff_{num}.csv"
week_diff.write_csv(filename)'''

#invariant keys - giorni della settimana sono pubblici
eps = 0.1
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



print(f"Totale corse per giorno della settimana con DP, eps = {eps}: {result}")
print(query_counts.summarize(alpha=0.05))

#invariant keys - giorni della settimana sono pubblici
eps1 = 0.01
context1 = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=eps1),
    split_evenly_over = 1,
    margins = [dp.polars.Margin(by=["weekday"], public_info="keys")]            
)

query_counts1 = (
    context1.query()
    .group_by("weekday")
    .agg(dp.len())
)
result1 = query_counts1.release().collect().sort("weekday")
result1 = result1.with_columns(pl.col("len").cast(pl.Int64))



print(f"Totale corse per giorno della settimana con DP, eps = {eps1}: {result1}")
print(query_counts1.summarize(alpha=0.05))

#invariant keys - giorni della settimana sono pubblici
eps2 = 0.001
context2 = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=eps2),
    split_evenly_over = 1,
    margins = [dp.polars.Margin(by=["weekday"], public_info="keys")]            
)

query_counts2 = (
    context2.query()
    .group_by("weekday")
    .agg(dp.len())
)
result2 = query_counts2.release().collect().sort("weekday")
result2 = result2.with_columns(pl.col("len").cast(pl.Int64))



print(f"Totale corse per giorno della settimana con DP, eps = {eps2}: {result2}")
print(query_counts2.summarize(alpha=0.05))


#crea grafici
dfplot.plot_total_rides_comparison(trips_weekday, result, eps, num)
dfplot.plot_total_rides_comparison(trips_weekday, result1, eps1, num)
dfplot.plot_total_rides_comparison(trips_weekday, result2, eps2, num)
dfplot.plot_total_rides_comparison2(trips_weekday, result, result1, eps, eps1, num)
dfplot.plot_total_rides_comparison2(trips_weekday, result, result2, eps, eps2, num)
dfplot.plot_total_rides_comparison3(trips_weekday, result, result1, result2, eps, eps1, eps2, num)
#########################################################
#   DATASET BICICLETTE SAMPLE - GIORNI DELLA SETTIMANA  #
#########################################################

import preprocessing as pre
import dfplot
import polars as pl
import opendp.prelude as dp
import math
import numpy as np 

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
    schema_overrides = schema_overrides
)
bike = pre.cast_enum(bike)

#nums = np.arange(100, 1000, 100).tolist() + np.arange(1000, 10000, 1000).tolist() + np.arange(10000, 100000, 10000).tolist() + np.arange(100000, 1000001, 100000).tolist()
num = 500000

eps = 0.01
#leps = np.arange(0.001, 0.01, 0.001).tolist() + np.arange(0.01, 1.01, 0.01).tolist()

'''
maxs =[]
mins = []
means = []
for num in nums:
    #sample original dataset
    bikes = bike.sample(n = num, with_replacement = False, seed = 1234)

    #DP dataset
    dfs = bikes.lazy()

    trips_weekday = bikes.group_by("weekday").len()
    trips_weekday = trips_weekday.with_columns(
        pl.col("weekday").cast(pl.Utf8),
        pl.col("len").cast(pl.Int64)
    )

    #difference
    week_diff = trips_weekday.join(trips_weekday, how = "cross")
    week_diff = week_diff.filter(pl.col("weekday") < pl.col("weekday_right"))
    week_diff = week_diff.with_columns((abs(pl.col("len") - pl.col("len_right"))).alias("diff"))
    week_diff = week_diff.select(["weekday", "weekday_right", "len", "len_right", "diff"])
    min_diff = week_diff.select(pl.col("diff").min())
    mins.append(min_diff.item())
    max_diff = week_diff.select(pl.col("diff").max())
    maxs.append(max_diff.item())
    mean_diff = week_diff.select(pl.col("diff").mean())
    means.append(mean_diff.item())

    #print(f"check: {num}")

    #print(f"Totale corse per giorno della settimana: {trips_weekday}")
    #print(f"Minima differenza tra i conteggi per giorno della settimana: {min_diff.item()}")
    #print(f"Massima differenza tra i conteggi per giorno della settimana: {max_diff.item()}")
    #print(f"Differenza media tra i conteggi per giorno della settimana: {mean_diff.item()}")

#print(nums, means, mins, maxs)
range = pl.DataFrame({
    "num": nums,
    "min": mins,
    "mean": means,
    "max": maxs
})


filename = f"bike/samplecsv/range.csv"
range.write_csv(filename)



accuracies = []
for eps in leps:
    #invariant keys - giorni della settimana sono pubblici
    context = dp.Context.compositor(
        data = dfs,
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
    result = query_counts.release().collect()
    summary = query_counts.summarize(alpha=0.05)
    acc_value = summary.select("accuracy").to_series()[0]

    #print(f"Totale corse per giorno della settimana con DP: {result}")
    #print(summary)
    accuracies.append(acc_value)

acc_eps = pl.DataFrame({
    "epsilon": leps,
    "accuracy": accuracies
})

#print(acc_eps)


filename = f"bike/samplecsv/eps_{num}.csv"
acc_eps.write_csv(filename)


#crea grafici
dfplot.plot_acceps()
'''

#sample original dataset
bikes = bike.sample(n = num, with_replacement = False, seed = 1234)

#DP dataset
dfs = bikes.lazy()

trips_weekday = bikes.group_by("weekday").len()
trips_weekday = trips_weekday.with_columns(
    pl.col("weekday").cast(pl.Utf8),
    pl.col("len").cast(pl.Int64)
)

#difference
week_diff = trips_weekday.join(trips_weekday, how = "cross")
week_diff = week_diff.filter(pl.col("weekday") < pl.col("weekday_right"))
week_diff = week_diff.with_columns((abs(pl.col("len") - pl.col("len_right"))).alias("diff"))
week_diff = week_diff.select(["weekday", "weekday_right", "len", "len_right", "diff"])
min_diff = week_diff.select(pl.col("diff").min())
max_diff = week_diff.select(pl.col("diff").max())
mean_diff = week_diff.select(pl.col("diff").mean())

print(f"Totale corse per giorno della settimana: {trips_weekday}")
print(f"Differenze: {week_diff}")
print(f"Minima differenza tra i conteggi per giorno della settimana: {min_diff.item()}")
print(f"Massima differenza tra i conteggi per giorno della settimana: {max_diff.item()}")
print(f"Differenza media tra i conteggi per giorno della settimana: {mean_diff.item()}")

#filename = f"bike/samplecsv/diff_{num}.csv"
#week_diff.write_csv(filename)

#invariant keys - giorni della settimana sono pubblici
context = dp.Context.compositor(
    data = dfs,
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
result = query_counts.release().collect()
result = result.with_columns(pl.col("len").cast(pl.Int64))
summary = query_counts.summarize(alpha=0.05)
acc_value = summary.select("accuracy").to_series()[0]

print(f"Totale corse per giorno della settimana con DP: {result}")
print(summary)

dfplot.plot_total_rides_comparison(trips_weekday, result, eps, num)
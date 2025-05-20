#########################
#   DATASET BICICLETTE  #
#########################

import preprocessing as pre
import dfplot
import polars as pl
import opendp.prelude as dp

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

#DP dataset
df = pl.scan_csv(
    "bike/tripdata.csv",
    schema_overrides=schema_overrides
)

df = pre.cast_enum(df)



###
# ANALISI DEL REPORT
###

# https://mot-marketing-whitelabel-prod.s3.amazonaws.com/nyc/January-2024-Citi-Bike-Monthly-Report.pdf

eps = 0.01

# NUMERO TOTALE DELLE CORSE
print("NUMERO TOTALE DELLE CORSE\n")
total_trips = bike.height

print(f"Numero corse totale di gennaio 2024: {total_trips}")

context = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=eps),
    split_evenly_over = 1             
)

query_num_responses = context.query().select(dp.len())
result = query_num_responses.release().collect()

print(f"Numero corse totale di gennaio 2024 con DP: {result.item()}")
print(query_num_responses.summarize(alpha=0.05))
accuracy = query_num_responses.summarize(alpha=0.05)["accuracy"][0]
relative_error = abs(result.item() - total_trips) / total_trips * 100  
print(f"Errore relativo: {relative_error}%")


# CORSE PER GIORNO - media (sapendo che i giorni di gennaio sono 31 dal preprocessing)
print("\n\n\nMEDIA CORSE AL GIORNO\n")
mean_trips = total_trips / 31
print(f"Media corse al giorno: {mean_trips}")

context = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=eps),
    split_evenly_over = 1             
)

query_num_responses = context.query().select(dp.len())
result = query_num_responses.release().collect()

print(f"Numero corse totale di gennaio 2024 con DP: {result.item()/31}")

relative_error = abs((result.item()/31) - mean_trips) / mean_trips * 100  
print(f"Errore relativo: {relative_error}%")


# DURATA MEDIA DELLA CORSA
print("\n\nDURATA MEDIA DELLA CORSA\n")
mean_duration = bike.select(pl.col("duration_sec").mean())
print(f"Media durata di ogni corsa: {mean_duration.item()} secondi")

context = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=eps),
    split_evenly_over = 1,
    margins=[
        dp.polars.Margin(
            max_partition_length = 1
        ),
    ],        
)

query_duration = (
    context.query()
    .select(
        pl.col("duration_sec")
        .cast(int)
        .fill_null(540)
        .dp.mean(bounds=(0, 24*2*3600)) 
    )
)
result = query_duration.release().collect()

print(f"Media durata di ogni corsa con DP: {result.item()} secondi")
print(query_duration.summarize(alpha=0.05))

relative_error = abs(result.item() - mean_duration.item()) / mean_duration.item() * 100  
print(f"Errore relativo: {relative_error}%")


# MEDIA DELLE CORSE PER GIORNO DELLA SETTIMANA
print("\n\nMEDIA DELLE CORSE PER GIORNO DELLA SETTIMANA\n")
trips_weekday = bike.group_by("weekday").len()
freq_df = pl.DataFrame({
    "weekday": ["Monday", "Tuesday", "Wednesday", "Thursday","Friday","Saturday","Sunday"],
    "occurrences": [5] * 3 + [4] * 4, 
})
trips_weekday = (
    trips_weekday
    .with_columns(pl.col("weekday").cast(pl.Utf8) )
    .join(freq_df, on="weekday")
    .with_columns((pl.col("len") / pl.col("occurrences")).alias("average_rides"))
)
print(f"Totale corse per giorno della settimana: {trips_weekday}")


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
result = query_counts.release().collect()
result = (
    result
    .with_columns(pl.col("weekday").cast(pl.Utf8) )
    .join(freq_df, on="weekday")
    .with_columns((pl.col("len") / pl.col("occurrences")).alias("average_rides"))
)

print(f"Totale corse per giorno della settimana con DP: {result}")
print(query_counts.summarize(alpha=0.05))

#crea grafico
#dfplot.plot_average_rides_comparison(trips_weekday, result)


# STAZIONI PIU' POPOLARI
start_stations = bike.group_by("start_station_id").len().sort("len", descending=True).head(10)
print(f"Stazioni più popolari di partenza: {start_stations}")

context = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=eps),
    split_evenly_over = 1,
    margins = [dp.polars.Margin(by=["start_station_id"], public_info="keys")]            
)

query_start_counts = (
    context.query()
    .group_by("start_station_id")
    .agg(dp.len())
)
result = query_start_counts.release().collect()
result = result.with_columns(pl.col("start_station_id").cast(pl.Utf8))

print(f"Stazioni più popolari di partenza con DP: {result.sort("len", descending=True).head(10)}")
print(query_start_counts.summarize(alpha=0.05))


end_stations = bike.group_by("end_station_id").len().sort("len", descending=True).head(10)
print(f"Stazioni più popolari di arrivo: {end_stations}")

context = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=eps),
    split_evenly_over = 1,
    margins = [dp.polars.Margin(by=["end_station_id"], public_info="keys")]            
)

query_end_counts = (
    context.query()
    .group_by("end_station_id")
    .agg(dp.len())
)
result = query_end_counts.release().collect()
result = result.with_columns(pl.col("end_station_id").cast(pl.Utf8))

print(f"Stazioni più popolari di arrivo con DP: {result.sort("len", descending=True).head(10)}")
print(query_end_counts.summarize(alpha=0.05))
#########################
#   DATASET BICICLETTE  #
#########################

import preprocessing as pre
import polars as pl
import opendp.prelude as dp

dp.enable_features("contrib")

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

eps = 0.1

# NUMERO TOTALE DELLE CORSE
'''print("NUMERO TOTALE DELLE CORSE\n")
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
relative_error = (accuracy / result.item()) * 100  
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

query_duration = context.query().select(
    pl.col("duration_sec").cast(int).fill_null(0).dp.sum(bounds=(0, 31 * 24 * 3600)),
    dp.len()
)
result = query_duration.release().collect().with_columns(mean = pl.col("duration_sec") / pl.col.len)

print(f"Media durata di ogni corsa con DP: {result["mean"][0]} secondi")
print(query_duration.summarize(alpha=0.05))

relative_error = abs(result["mean"][0] - mean_duration.item()) / mean_duration.item() * 100  
print(f"Errore relativo: {relative_error}%")'''


# MEDIA DELLE CORSE PER GIORNO DELLA SETTIMANA
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


'''
# STAZIONI PIU' POPOLARI
start_stations = bike.group_by("start_station_name").len().sort("len", descending=True).head(10)
print(f"Stazioni più popolari di partenza: {start_stations}")
end_stations = bike.group_by("end_station_name").len().sort("len", descending=True).head(10)
print(f"Stazioni più popolari di arrivo: {end_stations}")


'''
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

df = df.with_columns(
    pl.col("rideable_type").cast(pl.Enum(["electric_bike", "classic_bike"])),
    pl.col("member_casual").cast(pl.Enum(["member", "casual"]))
)



###
# ANALISI DEL REPORT
###

# https://mot-marketing-whitelabel-prod.s3.amazonaws.com/nyc/January-2024-Citi-Bike-Monthly-Report.pdf


# NUMERO TOTALE DELLE CORSE
total_trips = bike.height
print(f"Numero corse totale di gennaio 2024: {total_trips}")

context = dp.Context.compositor(
    data = df,
    privacy_unit = dp.unit_of(contributions=1),
    privacy_loss = dp.loss_of(epsilon=1.0),
    split_evenly_over = 1             
)

query_num_responses = context.query().select(dp.len())
result = query_num_responses.release().collect()

# Stampa il risultato
print(f"Numero corse totale di gennaio 2024 con DP: {result.item()}")

'''
# CORSE PER GIORNO - media (sapendo che i giorni di gennaio sono 31 dal preprocessing)
mean_trips = total_trips / 31
print(f"Media corse al giorno: {mean_trips}")


# DURATA MEDIA DELLA CORSA
mean_duration = bike.select(pl.col("duration_sec").mean())
print(f"Media durata di ogni corsa: {mean_trips}")

# CORSE PER GIORNO DELLA SETTIMANA
trips_weekday = bike.group_by("weekday").len()
print(f"Totale corse per giorno della settimana: {trips_weekday}")

# STAZIONI PIU' POPOLARI
start_stations = bike.group_by("start_station_name").len().sort("len", descending=True).head(10)
print(f"Stazioni più popolari di partenza: {start_stations}")
end_stations = bike.group_by("end_station_name").len().sort("len", descending=True).head(10)
print(f"Stazioni più popolari di arrivo: {end_stations}")


'''
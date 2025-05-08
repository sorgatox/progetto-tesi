import polars as pl

#########################
#   DATASET BICICLETTE  #
#########################

schema_overrides = {
    "start_station_id": pl.Utf8,
    "end_station_id": pl.Utf8,
    "started_at": pl.Datetime(time_unit="ms"), 
    "ended_at": pl.Datetime(time_unit="ms")
}

bike = pl.read_csv(
    "data/202401-tripdata.csv",
    schema_overrides=schema_overrides
)

print(bike.height)
#print(bike.schema)

'''
#verifica - quali possibili valori abbiamo per ogni categoria
values = bike["rideable_type"].unique()
print("Valori unici in 'rideable_type':", values)

values = bike["member_casual"].unique()
print("Valori unici in 'member_casual':", values)
'''

#cast dei dati in Enumerici (Factor di R)
bike = bike.with_columns(
    pl.col("rideable_type").cast(pl.Enum(["electric_bike", "classic_bike"])),
    pl.col("member_casual").cast(pl.Enum(["member", "casual"]))
)

'''
#valori nulli?
NAdata = bike.select([
    pl.col(col).is_null().sum().alias(col) for col in bike.columns
])
print(NAdata)
'''

#filtra valori nulli
bike = bike.filter(
    pl.col("end_lat").is_not_null() & 
    pl.col("end_lng").is_not_null()
)

'''
#verifica valori nulli - deve dare zero
NAdata = bike.select([
    pl.col(col).is_null().sum().alias(col) for col in bike.columns
])
print(NAdata)
'''

#durata in secondi della corsa
bike = bike.with_columns(
    (pl.col("ended_at") - pl.col("started_at")).dt.total_seconds().alias("duration_sec")
).filter(
    pl.col("duration_sec") > 0
)


print("=== HEAD ===")
print(bike.head())

print(bike.height)

'''
print("\n=== DIMENSIONI FINALI ===")
print(bike.shape)

print("\n=== SCHEMA FINALE ===")
print(bike.schema)
'''


###
# ANALISI DEL REPORT
###

# https://mot-marketing-whitelabel-prod.s3.amazonaws.com/nyc/January-2024-Citi-Bike-Monthly-Report.pdf




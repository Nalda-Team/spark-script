from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract,
    explode, concat, lit, to_date,to_timestamp
)
from pyspark.sql.types import StringType, IntegerType, TimestampType
from pyspark.sql.functions import udf
from spark_json_parser.utils.common_utils import convert_to_utc_str

def process_domestic_flights_df(df: DataFrame, airport_map_bc):
    # 1) attach file_path and fetched_date
    df = df.withColumn("file_path", input_file_name()) \
           .withColumn("fetched_date", regexp_extract("file_path", r"/(\d{4}-\d{2}-\d{2})/", 1))

    # 2) explode departures and preserve fetched_date
    schedules = df.select(
        explode(col("data.domesticFlights.departures")).alias("sch"),
        col("file_path"),
        col("fetched_date")
    ).select("sch.*", "file_path", "fetched_date")

    # 3) UDF definitions
    get_option_type = udf(lambda s: {'Y':'일반석','D':'할인석','L':'특가석','C':'비즈니스석'}.get(s, '기타'), StringType())
    calc_journey = udf(lambda jt: 0 if jt is None or len(jt)<5 else int(jt[:2])*60 + int(jt[3:]), IntegerType())
    convert_dt = udf(lambda ts, city: convert_to_utc_str(ts, city, airport_map_bc.value), StringType())

    # 4) datetime source columns (already YYYYMMDDHHmm)
    schedules = schedules.withColumn("dep_dt_src", col("departureDate")) \
                         .withColumn("arr_dt_src", col("arrivalDate"))

    # 5) enrich base DataFrame
    base = schedules \
        .withColumn("air_id", concat(
            col("departureDate"), col("depCity"), col("arrCity"),
            col("airlineCode"), col("fitName"), col("seatClass")
        )) \
        .withColumn("depart_ts_str",  convert_dt(col("dep_dt_src"), col("depCity"))) \
        .withColumn("arrival_ts_str", convert_dt(col("arr_dt_src"), col("arrCity"))) \
        .withColumn("option_type",    get_option_type(col("seatClass"))) \
        .withColumn("journey_time",   calc_journey(col("journeyTime")))

    # 6) flight_info_df
    flight_info_df = base.select(
        "air_id",
        col("airlineName").alias("airline"),
        col("depCity").alias("depart_airport"),
        col("depart_ts_str").cast(TimestampType()).alias("depart_timestamp"),
        col("arrCity").alias("arrival_airport"),
        col("arrival_ts_str").cast(TimestampType()).alias("arrival_timestamp"),
        col("journey_time")
    ).withColumn("is_layover", lit(False))

    # 7) fare_info_df
    fare_exploded = base.select("air_id", "option_type", explode(col("fare")).alias("f"), "fetched_date")
    fare_info_df = fare_exploded.select(
        "air_id",
        col("option_type").alias("seat_class"),
        col("f.agtCode").alias("agt_code"),
        (col("f.adultFare") + col("f.aTax") + col("f.aFuel") + col("f.publishFee")).alias("adult_fare"),
        to_date(col("fetched_date"), "yyyy-MM-dd").alias("fetched_date"),
        lit("n").alias("fare_class")
    )

    # flight_info_df.show(truncate=False)
    # fare_info_df.show(truncate=False)

    return flight_info_df, fare_info_df

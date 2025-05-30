from pyspark.sql.functions import (
    input_file_name, regexp_extract, lit, explode, col, concat, 
    to_utc_timestamp, expr, to_timestamp,create_map, lit, to_date, udf
)
from pyspark.sql.types import TimestampType

def build_base_df(df, timezone_map_bc):
    @udf()
    def get_timezone(airport_code):
        return timezone_map_bc.value.get(airport_code, "UTC")
    
    seat_map = create_map(
        lit('Y'), lit('일반석'), lit('D'), lit('할인석'),
        lit('L'), lit('특가석'), lit('C'), lit('비즈니스석')
    )
    
    df = df.withColumn("file_path", input_file_name()) \
           .withColumn("fetched_date", regexp_extract(col("file_path"), r"/(\d{4}-\d{2}-\d{2})/", 1))
    
    schedules = df.select(
        explode(col("data.domesticFlights.departures")).alias("sch"),
        col("file_path"),
        col("fetched_date")
    ).select("sch.*", "file_path", "fetched_date")
    
    base = schedules \
        .withColumn("air_id", concat(
            col("departureDate"), col("depCity"), col("arrCity"),
            col("airlineCode"), col("fitName"), col("seatClass")
        )) \
        .withColumn("depart_local_ts", to_timestamp(col("departureDate"), "yyyyMMddHHmm")) \
        .withColumn("arrival_local_ts", to_timestamp(col("arrivalDate"), "yyyyMMddHHmm")) \
        .withColumn("dep_timezone", get_timezone(col("depCity"))) \
        .withColumn("arr_timezone", get_timezone(col("arrCity"))) \
        .withColumn("depart_timestamp", to_utc_timestamp(
            col("depart_local_ts"), col("dep_timezone")
        )) \
        .withColumn("arrival_timestamp", to_utc_timestamp(
            col("arrival_local_ts"), col("arr_timezone")
        )) \
        .withColumn("option_type", seat_map[col("seatClass")]) \
        .withColumn("journey_time", expr("cast(substring(journeyTime, 1, 2) as int) * 60 + cast(substring(journeyTime, 4, 2) as int)"))
    
    return base.select(
    "air_id", "airlineName", "depCity", "arrCity", 
    "depart_local_ts", "arrival_local_ts", "depart_timestamp", "arrival_timestamp",
    "journey_time", "option_type", "fare", "fetched_date"
    )

def build_flight_info_df(base_df):
    flight_info_df = base_df.select(
        "air_id",
        col("airlineName").alias("airline"),
        col("depCity").alias("depart_airport"),
        col("depart_timestamp").cast(TimestampType()),
        col("depart_local_ts").cast(TimestampType()),
        col("arrCity").alias("arrival_airport"),
        col("arrival_timestamp").cast(TimestampType()),
        col("arrival_local_ts").cast(TimestampType()),
        col("journey_time")
    ).withColumn("is_layover", lit(False))

    return flight_info_df

def build_fare_info_df(base_df):
    fare_exploded = base_df.select("air_id", "option_type", explode(col("fare")).alias("f"), "fetched_date")
    fare_info_df = fare_exploded.select(
        "air_id",
        col("option_type").alias("seat_class"),
        col("f.agtCode").alias("agt_code"),
        (col("f.adultFare") + col("f.aTax") + col("f.aFuel") + col("f.publishFee")).alias("adult_fare"),
        to_date(col("fetched_date"), "yyyy-MM-dd").alias("fetched_date"),
        lit("n").alias("fare_class")
    )

    return fare_info_df

from pyspark.sql.functions import concat, lit, coalesce, avg, expr, count, col
from pyspark.sql.types import IntegerType
def build_agg_df(flight_info_df, fare_info_df):
   
   joined_df = flight_info_df.join(fare_info_df, "air_id", "inner")
   
   agg_df = joined_df \
       .withColumn("depart_date", to_date(col("depart_local_ts"))) \
       .withColumn("airline", coalesce(col("airline"), lit("airline 여러개"))) \
       .groupBy(
           "depart_airport", "arrival_airport", "depart_date", "is_layover", 
           "airline", "seat_class", "fare_class", "fetched_date"
       ) \
       .agg(
            avg("adult_fare").cast(IntegerType()).alias("mean_price"),
            expr("cast(percentile_approx(adult_fare, 0.5) as int)").alias("median_price"),
            count("*").alias("ticket_count")
        )
   return agg_df
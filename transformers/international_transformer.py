from pyspark.sql.functions import (
    col, split, element_at, lit, when, size, explode, map_entries, 
    posexplode, to_timestamp, to_utc_timestamp, url_decode, 
    input_file_name, regexp_extract, create_map, to_date, substring,
    min as Fmin, max as Fmax, first as Ffirst, count as Fcount,
    collect_set, udf
)
from pyspark.sql.types import IntegerType
from maps.airport_map import airport_map

def build_base_df(raw_df):
    df = raw_df.select(
        "data.internationalList.results.airlines",
        "data.internationalList.results.fareTypes",
        "data.internationalList.results.fares",
        "data.internationalList.results.schedules"
        ).withColumn("file_path", lit("gs://flight-data-staging-bucket/2025-05-29/international/2025-05-30_CTS_SEL_C_international.json")) \
     .withColumn("fetched_date", regexp_extract("file_path", r"/(\d{4}-\d{2}-\d{2})/", 1)) \
     .withColumn("seat_class_code", regexp_extract("file_path", r"_([YPCF])_", 1))
    
    # seat_class 매핑
    seat_map = create_map(
        lit('Y'), lit('일반석'), lit('P'), lit('이코노미'),
        lit('C'), lit('비즈니스석'), lit('F'), lit('일등석')
    )
    df = df.withColumn("seat_class", seat_map[col("seat_class_code")])

    df = df.withColumn("airlines_map", df["airlines"]) \
           .withColumn("fare_types_map", df["fareTypes"]) \
           .withColumn("fares_map", df["fares"]) \
           .withColumn("schedules_arr", df["schedules"])
    
    return df.select("fetched_date", "seat_class", "airlines_map", 
                "fare_types_map", "fares_map", "schedules_arr")

from pyspark.sql.types import IntegerType, StringType

def build_valid_df(base_df, timezone_map_bc):
    # UDF 정의 시 returnType 명시
    @udf(returnType=StringType())
    def get_timezone(airport_code):
        return timezone_map_bc.value.get(airport_code, "UTC")
    
    # explode schedules
    base = base_df.select(
        "fetched_date", "seat_class", "airlines_map", "fare_types_map", "fares_map",
        explode(col("schedules_arr")).alias("sch_map")
    )
    
    sched = base.select(
        "fetched_date", "seat_class", "airlines_map", "fare_types_map", "fares_map",
        explode(map_entries(col("sch_map"))).alias("me")
    ).select(
        col("me.key").alias("full_air_id"), "fetched_date", "seat_class",
        col("airlines_map"), col("fare_types_map"), col("fares_map"),
        col("me.value.detail").alias("detail_arr"),
        col("me.value.journeyTime").alias("jt_arr")
    )

    # segment explode + journey_time
    seg = sched.select(
        "full_air_id", "fetched_date", "seat_class", "airlines_map", "fare_types_map", "fares_map",
        posexplode(col("detail_arr")).alias("ord", "seg"), "jt_arr"
    ).withColumn("journey_time", col("jt_arr")[0].cast(IntegerType())*60 + col("jt_arr")[1].cast(IntegerType()))

    # 공항 필터링 + utc 시간 통일
    airport_codes = list(airport_map.keys())
    
    valid = seg.filter(
        col("seg.sa").isin(airport_codes) & col("seg.ea").isin(airport_codes)
    ).withColumn("depart_local_ts", to_timestamp(col("seg.sdt"), "yyyyMMddHHmm")) \
     .withColumn("arrival_local_ts", to_timestamp(col("seg.edt"), "yyyyMMddHHmm")) \
     .withColumn("dep_timezone", get_timezone(col("seg.sa"))) \
     .withColumn("arr_timezone", get_timezone(col("seg.ea"))) \
     .withColumn("depart_ts", to_utc_timestamp(col("depart_local_ts"), col("dep_timezone"))) \
     .withColumn("arrival_ts", to_utc_timestamp(col("arrival_local_ts"), col("arr_timezone"))) \
     .withColumn("airline", url_decode(col("airlines_map")[col("seg.av")]))
    
    return valid.select(
            "full_air_id", "fetched_date", "seat_class", "ord", "journey_time",
            "depart_local_ts", "arrival_local_ts", "depart_ts", "arrival_ts",
            "airline", "seg")


def build_flight_info_df(valid_df):
    # 경유편 항공사 통합 로직
    airline_check = valid_df.groupBy("full_air_id").agg(
        collect_set("airline").alias("airline_set")
    ).withColumn("unified_airline", 
        when(size(col("airline_set")) == 1, col("airline_set")[0])
        .otherwise(lit(None))
    )
    
    # 경유편 전체 정보
    full_df = valid_df.groupBy("full_air_id").agg(
        Fmin("depart_ts").alias("depart_timestamp"),
        Fmax("arrival_ts").alias("arrival_timestamp"),
        Ffirst("depart_local_ts").alias("depart_local_ts"),
        Ffirst("arrival_local_ts").alias("arrival_local_ts"),
        Ffirst("journey_time").alias("journey_time"),
        Ffirst(col("seg.sa")).alias("depart_airport"),
        Ffirst(col("seg.ea")).alias("arrival_airport"),
        (Fcount("*") > 1).alias("is_layover")
    ).join(airline_check.select("full_air_id", "unified_airline"), "full_air_id") \
     .select(
         col("full_air_id").alias("air_id"),
         col("unified_airline").alias("airline"),  # 여기서 컬럼명 변경
         "depart_airport", "depart_timestamp", "depart_local_ts",
         "arrival_airport", "arrival_timestamp", "arrival_local_ts", 
         "journey_time", "is_layover"
     )

    # 경유 세그먼트별 독립 레코드
    seg_df = valid_df.filter(col("full_air_id").contains("+")) \
        .withColumn("air_id", element_at(split(col("full_air_id"), "\\+"), col("ord")+1)) \
        .withColumn("is_layover", lit(False)) \
        .select(
            "air_id", "airline",
            col("depart_ts").alias("depart_timestamp"),
            col("arrival_ts").alias("arrival_timestamp"),
            "depart_local_ts", "arrival_local_ts",
            "journey_time",
            col("seg.sa").alias("depart_airport"),
            col("seg.ea").alias("arrival_airport"),
            "is_layover"
        )

    # 최종 합치기
    flight_info_df = full_df.unionByName(seg_df)

    return flight_info_df

def build_layover_info_df(valid_df):
    # + 가 포함된 경유편만 필터링
    layover_info_df = valid_df.filter(col("full_air_id").contains("+")) \
        .withColumn("segment_id", element_at(split(col("full_air_id"), "\\+"), col("ord")+1)) \
        .withColumn("layover_order", col("ord")) \
        .withColumn("connect_time", substring(col("seg.ct"), 1, 2).cast(IntegerType())*60 + 
                   substring(col("seg.ct"), 3, 2).cast(IntegerType())) \
        .select(col("full_air_id").alias("air_id"), "segment_id", "layover_order", "connect_time")

    return layover_info_df


def build_fare_info_df(valid_df, base_df):
   # valid_df에서 유효한 air_id 추출 (full_air_id + 세그먼트별 air_id 모두 포함)
   full_air_ids = valid_df.select("full_air_id").distinct()
   segment_air_ids = valid_df.filter(col("full_air_id").contains("+")) \
       .withColumn("segment_air_id", element_at(split(col("full_air_id"), "\\+"), col("ord")+1)) \
       .select("segment_air_id").distinct() \
       .withColumnRenamed("segment_air_id", "full_air_id")
   
   valid_air_ids = full_air_ids.union(segment_air_ids).distinct().hint("broadcast")
   
   # Step 1: fares_map 분해 (air_id별)
   fare_base = base_df.select("fetched_date","seat_class","fare_types_map",
                             explode(map_entries(col("fares_map"))).alias("fe")) \
       .select(col("fe.key").alias("air_id"),col("fe.value.fare").alias("fare_map"),
              "fetched_date","seat_class","fare_types_map")
   
   # Step 2: fare_map 분해 (option별)
   options = fare_base.select("air_id","fetched_date","seat_class","fare_types_map",
                             explode(map_entries(col("fare_map"))).alias("ft")) \
       .select("air_id","fetched_date","seat_class","fare_types_map",
              col("ft.key").alias("option_key"),col("ft.value").alias("fare_list"))
   
   # Step 3: fare_list 분해 (각 fare 객체별)
   fares = options.select("air_id","fetched_date","seat_class","fare_types_map","option_key",
                         explode(col("fare_list")).alias("fare"))
   
   # Step 4: fare_url 컬럼 추가 후 fare_class 추출
   fares_with_url = fares.withColumn("fare_url", col("fare.priceTransparencyUrl.`#cdata-section`"))

   
   parts = split(regexp_extract(col("fare_url"),r"FareRuleItnInfo=([^&]+)",1),"/")
   fare_class_col = when((size(parts)<3)|(regexp_extract(col("fare_url"),r"FareRuleItnInfo=([^&]+)",1)==""),lit("n")).otherwise(element_at(parts,3))
   
   # Step 5: 최종 데이터 생성 (Python 로직과 동일)
   fare_info_df = fares_with_url \
       .withColumn("option_type", url_decode(col("fare_types_map")[col("option_key")])) \
       .filter(col("option_type") == lit("성인/모든 결제수단")) \
       .withColumn("agt_code", col("fare.AgtCode")) \
       .withColumn("adult_base_fare", col("fare.Adult.Fare").cast(IntegerType())) \
       .withColumn("adult_naver_fare", col("fare.Adult.NaverFare").cast(IntegerType())) \
       .withColumn("adult_tax", col("fare.Adult.Tax").cast(IntegerType())) \
       .withColumn("adult_qcharge", col("fare.Adult.QCharge").cast(IntegerType())) \
       .withColumn("adult_fare", 
           col("adult_base_fare") + col("adult_naver_fare") + col("adult_tax") + col("adult_qcharge")) \
       .withColumn("fare_class", fare_class_col) \
       .select("air_id","seat_class","agt_code","adult_fare","fetched_date","fare_class") \
       .withColumn("fetched_date", to_date(col("fetched_date"), "yyyy-MM-dd")) \
       .join(valid_air_ids.withColumnRenamed("full_air_id", "air_id"), on="air_id", how="left_semi")

   return fare_info_df

from pyspark.sql.functions import concat, lit, coalesce, avg, expr, count, col
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
from itertools import chain
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract,
    explode, map_entries, posexplode,
    to_date, split, lit, create_map, element_at,
    to_timestamp, to_utc_timestamp, broadcast,
    substring, min as Fmin, max as Fmax, first as Ffirst, count as Fcount,
    when, size, url_decode
)
from pyspark.sql.types import StringType, IntegerType


def process_international_flights_df(raw_df: DataFrame, airport_map_bc):
    # 1) 메타 정보 추출 + 필수 컬럼만 선별
    df = raw_df.select(
        "data.internationalList.results.airlines",
        "data.internationalList.results.fareTypes",
        "data.internationalList.results.fares",
        "data.internationalList.results.schedules"
    ).withColumn("file_path", input_file_name()) \
     .withColumn("fetched_date", regexp_extract("file_path", r"/(\d{4}-\d{2}-\d{2})/", 1)) \
     .withColumn("seat_class_code", regexp_extract("file_path", r"_([YPCF])_", 1))

    # seat_class 매핑
    seat_map = create_map(
        lit('Y'), lit('일반석'), lit('P'), lit('이코노미'),
        lit('C'), lit('비즈니스석'), lit('F'), lit('일등석')
    )
    df = df.withColumn("seat_class", seat_map[col("seat_class_code")])

    # UDF 제거: 내장 url_decode로 URL 디코딩 사용
    df = df.withColumn("airlines_map", df["airlines"]) \
           .withColumn("fare_types_map", df["fareTypes"]) \
           .withColumn("fares_map", df["fares"]) \
           .withColumn("schedules_arr", df["schedules"])

    # timezone map 생성
    tz_map = create_map(*[lit(k).alias(k) for k in []])  # dummy init
    tz_entries = []
    for code, info in airport_map_bc.value.items():
        tz_entries.extend([lit(code), lit(info["time_zone"])])
    tz_map = create_map(*tz_entries)

    # explode schedules
    base = df.select(
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

    # 공항 필터링 + 시간 변환 + cache + repartition
    airport_codes = list(airport_map_bc.value.keys())
    valid = seg.filter(
        col("seg.sa").isin(airport_codes) & col("seg.ea").isin(airport_codes)
    ).withColumn("depart_local_ts", to_timestamp(col("seg.sdt"), "yyyyMMddHHmm")) \
     .withColumn("arrival_local_ts", to_timestamp(col("seg.edt"), "yyyyMMddHHmm")) \
     .withColumn("depart_ts", to_utc_timestamp(col("depart_local_ts"), tz_map[col("seg.sa")])) \
     .withColumn("arrival_ts", to_utc_timestamp(col("arrival_local_ts"), tz_map[col("seg.ea")])) \
     .withColumn("airline", url_decode(col("airlines_map")[col("seg.av")]))
    # valid = valid.cache().repartition(col("full_air_id"))

        # FLIGHT_INFO 집계
    full_df = valid.groupBy("full_air_id").agg(
        Fmin("depart_ts").alias("depart_timestamp"),
        Fmax("arrival_ts").alias("arrival_timestamp"),
        Ffirst("journey_time").alias("journey_time"),
        Ffirst("airline").alias("airline"),
        Ffirst(col("seg.sa")).alias("depart_airport"),
        Ffirst(col("seg.ea")).alias("arrival_airport"),
        (Fcount("*")>1).alias("is_layover")
    ).withColumnRenamed("full_air_id", "air_id")

    # 경유 세그먼트 별 독립 레코드 생성 (is_layover=False)
    seg_df = valid.filter(col("full_air_id").contains("+")) \
        .withColumn("air_id", element_at(split(col("full_air_id"), "\+"), col("ord")+1)) \
        .withColumn("is_layover", lit(False)) \
        .select(
            col("air_id"),
            col("depart_ts").alias("depart_timestamp"),
            col("arrival_ts").alias("arrival_timestamp"),
            col("journey_time"),
            col("airline"),
            col("seg.sa").alias("depart_airport"),
            col("seg.ea").alias("arrival_airport"),
            col("is_layover")
        )

    # 최종 합치기
    flight_info_df = full_df.unionByName(seg_df)


    # LAYOVER_INFO
    layover_df = valid.withColumn("segment_id", element_at(split(col("full_air_id"),"\\+"),col("ord")+1)) \
        .withColumn("layover_order", col("ord")) \
        .withColumn("connect_time", substring(col("seg.ct"),1,2).cast(IntegerType())*60 + substring(col("seg.ct"),3,2).cast(IntegerType())) \
        .select(col("full_air_id").alias("air_id"),"segment_id","layover_order","connect_time")

    # FARE_INFO: left_semi + broadcast + early projection
    valid_flights = flight_info_df.select("air_id").hint("broadcast")
    fare_base = df.select("fetched_date","seat_class","fare_types_map",explode(map_entries(col("fares_map"))).alias("fe")) \
        .select(col("fe.key").alias("air_id"),col("fe.value.fare").alias("fare_map"),"fetched_date","seat_class","fare_types_map")
    fees = fare_base.select("air_id","fetched_date","seat_class","fare_types_map",explode(map_entries(col("fare_map"))).alias("ft"))
    fare_url = col("ft.value.priceTransparencyUrl.`#cdata-section`")[0]
    parts = split(regexp_extract(fare_url,r"FareRuleItnInfo=([^&]+)",1),"/")
    fare_class_col = when((size(parts)<3)|(regexp_extract(fare_url,r"FareRuleItnInfo=([^&]+)",1)==""),lit("n")).otherwise(element_at(parts,3))
    fare_info_df = fees \
        .withColumn("option_type", url_decode(col("fare_types_map")[col("ft.key")])) \
        .withColumn("adult_fare", col("ft.value.Adult.Fare")[0].cast(IntegerType())) \
        .withColumn("fare_class", fare_class_col) \
        .withColumn("agt_code", explode(col("ft.value.AgtCode"))) \
        .select(
            "air_id","seat_class","option_type","agt_code","adult_fare","fetched_date","fare_class") \
        .join(valid_flights, on="air_id", how="left_semi")

    # 결과 확인
    # flight_info_df.show(truncate=False)
    # layover_df.show(truncate=False)
    # fare_info_df.show(truncate=False)
    
    return flight_info_df, layover_df, fare_info_df

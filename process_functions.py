
from spark_json_parser.transformers.domestic_transform import (
   build_base_df as build_domestic_base_df, 
   build_flight_info_df as build_domestic_flight_info_df, 
   build_fare_info_df as build_domestic_fare_info_df, 
   build_agg_df as build_domestic_agg_df
)
from transformers.international_transformer import (
   build_base_df as build_international_base_df,
   build_valid_df as build_international_valid_df,
   build_flight_info_df as build_international_flight_info_df,
   build_fare_info_df as build_international_fare_info_df,
   build_layover_info_df as build_international_layover_info_df,
   build_agg_df as build_international_agg_df
)
from spark_json_parser.utils.db_utils import save_flight_data_sequentially
import time

def process_domestic_raw_df(raw_df, timezone_map_bc):
    """국내선 항공편 파일 배치 처리"""
    process_start=time.time()
    # 기본 df 캐싱
    base_df=build_domestic_base_df(raw_df, timezone_map_bc)
    base_df.cache()

    flight_info_df=build_domestic_flight_info_df(base_df)
    flight_info_df.cache()
    flight_info_for_save = flight_info_df.select(
        "air_id", "airline", "depart_airport", "depart_timestamp",
        "arrival_airport", "arrival_timestamp", "journey_time", "is_layover"
    )
    save_flight_data_sequentially(flight_info_for_save, "flight_info", ["air_id"])

    fare_info_df=build_domestic_fare_info_df(base_df)
    fare_info_df.cache()
    save_flight_data_sequentially(fare_info_df, "fare_info", ["air_id", "seat_class", "agt_code", "fetched_date", "fare_class"])

    base_df.unpersist() # flight_info, fare_info 두개 만들고 캐싱해제 (더이상 필요 X)

    # 날짜별 집계 결과 (출발공항, 도착공항, 출발 날짜, 경유 여부, 항공사, 좌석 등급, 운임 등급, 수집 날짜, 평균가, 중앙가, 티켓 수)
    agg_df = build_domestic_agg_df(flight_info_df, fare_info_df)
    save_flight_data_sequentially(agg_df, "daily_aggregated_flight_info", ["depart_airport", "arrival_airport", "depart_date", "is_layover", "fetched_date", "airline", "seat_class", "fare_class"])
    
    flight_info_df.unpersist() # 중간 집계 후 캐싱 해제
    fare_info_df.unpersist() # 중간 집계 후 캐싱 해제
    
    process_end=time.time()
    print(f"국내선 데이터 저장 완료 (소요시간 : {int(process_end-process_start)})")


def process_international_raw_df(raw_df, timezone_map_bc):
    # 타이머
    process_start=time.time()
    """국제선 항공편 파일 배치 처리"""
    # 기본 df 캐싱
    base_df = build_international_base_df(raw_df)
    base_df=base_df.cache()

    valid_df=build_international_valid_df(base_df, timezone_map_bc)
    valid_df.cache()

    flight_info_df = build_international_flight_info_df(valid_df)
    flight_info_df.cache()
    flight_info_for_save = flight_info_df.select(
        "air_id", "airline", "depart_airport", "depart_timestamp",
        "arrival_airport", "arrival_timestamp", "journey_time", "is_layover"
    )
    save_flight_data_sequentially(flight_info_for_save, "flight_info", ["air_id"])
    
    fare_info_df = build_international_fare_info_df(valid_df, base_df)
    fare_info_df.cache()
    save_flight_data_sequentially(fare_info_df, "fare_info", ["air_id", "seat_class", "agt_code", "fetched_date", "fare_class"])
    base_df.unpersist() # valid_df, fare_info 두개 만들고 캐싱해제 (더이상 필요 X)


    layover_info_df = build_international_layover_info_df(valid_df)
    save_flight_data_sequentially(layover_info_df, "layover_info", ["air_id", "segment_id", "layover_order"])
    
    valid_df.unpersist() # flight_info, layover_info, layover_info 세개 만들고 캐싱해제

    
    agg_df = build_international_agg_df(flight_info_df, fare_info_df)
    save_flight_data_sequentially(agg_df, "daily_aggregated_flight_info", ["depart_airport", "arrival_airport", "depart_date", "is_layover", "fetched_date", "airline", "seat_class", "fare_class"])
    
    flight_info_df.unpersist() # 집계 후 캐싱 해제
    fare_info_df.unpersist() 

    process_end=time.time()
    print(f"국제선 데이터 저장 완료 (소요시간 : {int(process_end-process_start)})")
import argparse
from spark_json_parser.maps.airport_map import airport_map
from spark_json_parser.utils.db_utils import save_flight_data_sequentially
from config.spark_session import get_spark_session
from spark_json_parser.utils.optimize_partition import optimize_partitions
from transformers.international_transformer import build_base_df, build_valid_df, build_flight_info_df, build_fare_info_df, build_layover_info_df
from config.schemas import get_international_schema
from spark_json_parser.utils.gcs_utils import list_gcs_files
from pyspark.sql.functions import input_file_name
import time
import os

def create_batches(file_list, batch_size=10):
    """파일 목록을 배치로 분할"""
    batches = []
    for i in range(0, len(file_list), batch_size):
        batches.append(file_list[i:i+batch_size])
    return batches

def process_international_batch(spark, file_paths):
    
    # 타이머
    process_start=time.time()
    """국제선 항공편 파일 배치 처리"""
    airport_map_bc = spark.sparkContext.broadcast(airport_map)
    schema = get_international_schema()
    df = (
        spark
        .read
        .schema(schema)
        .json(file_paths)
        .withColumn("file_path", input_file_name())
    )
    base_df = build_base_df(df)
    base_df=base_df.cache()

    valid_df=build_valid_df(base_df, airport_map_bc)
    # valid_df=valid_df.cache()

    flight_info_df = build_flight_info_df(valid_df)
    layover_info_df = build_layover_info_df(valid_df)
    fare_info_df = build_fare_info_df(flight_info_df, base_df)
    # flight_info_df.show(truncate=False)
    # layover_info_df.show(truncate=False)
    # fare_info_df.show(truncate=False)
    print("국제선 항공권 데이터 처리 완료")

    if flight_info_df is not None and fare_info_df is not None:
        # 중복 제거 및 파티션 최적화 + 캐시
        flight_info_df = optimize_partitions(flight_info_df, ["air_id"])
        fare_info_df   = optimize_partitions(
            fare_info_df,
            ["air_id", "seat_class", "agt_code", "fetched_date", "fare_class"]
        )

        # 경유 정보가 있는 경우 최적화 + 캐시
        if layover_info_df is not None and not layover_info_df.isEmpty():
            layover_info_df = optimize_partitions(
                layover_info_df,
                ["air_id", "segment_id", "layover_order"]
            )
        else:
            layover_info_df = None
        # 순차적으로 데이터 저장 (layover_info 포함)
        # 캐시된 DF의 storageLevel과 파티션 수 출력

        # start=time.time()
        # print(f"flight_info    — storage: {flight_info_df.storageLevel}, partitions: {flight_info_df.rdd.getNumPartitions()}")
        # print(f"fare_info      — storage: {fare_info_df.storageLevel}, partitions: {fare_info_df.rdd.getNumPartitions()}")
        # if layover_info_df is not None:
        #     print(f"layover_info   — storage: {layover_info_df.storageLevel}, partitions: {layover_info_df.rdd.getNumPartitions()}")
        
        # end=time.time()

        # print('파티션 수 카운팅 소요 시간', int(end-start))

        save_flight_data_sequentially(flight_info_df, fare_info_df, layover_info_df)
        base_df.unpersist()
        process_end=time.time()
        print(f"국제선 데이터 저장 완료 (소요시간 : {int(process_end-process_start)})")
    else:
        print("국제선 처리 실패")

def process_folder_in_batches(bucket_name, folder_path, batch_size=10):
    """폴더 내 파일들을 배치로 처리"""
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    file_list = list_gcs_files(spark, bucket_name, folder_path)
    file_list = [f"gs://{bucket_name}/{file}" for file in file_list if file.endswith('international.json')]
    batches = create_batches(file_list, batch_size)

    for i, batch in enumerate(batches, start=1):
        print(f"배치 {i}/{len(batches)} 처리 시작 ({len(batch)}개 파일)")
        process_international_batch(spark, batch)
        print(f"배치 {i}/{len(batches)} 처리 완료")

    print("모든 배치 처리 완료")
    spark.stop()

def main():
    parser = argparse.ArgumentParser(description='국제선 항공편 데이터 배치 처리 스크립트')
    parser.add_argument('--LOCAL_FLAG',
                        type=str,
                        default='N',
                        help='로컬 실행 여부')
    parser.add_argument('--DB_HOST',
                        type=str,
                        default=None,
                        help='DB 호스트 주소')
    
    parser.add_argument('--DB_USER',
                        type=str,
                        default=None,
                        help='DB 유저')
    
    parser.add_argument('--DB_PASSWORD',
                        type=str,
                        default=None,
                        help='DB 비밀번호')
    
    parser.add_argument('--DB_NAME',
                        type=str,
                        default=None,
                        help='DB 이름')
    
    parser.add_argument(
        '--bucket',
        type=str,
        default='origin_fetched_flight_data_bucket',
        help='GCS 버킷 이름'
    )
    parser.add_argument(
        '--folder',
        type=str,
        default=None,
        help='처리할 폴더 경로 (기본값: 2025-04-30/international)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=60,
        help='배치당 파일 수 (기본값: 60)'
    )
    args = parser.parse_args()
    if args.DB_HOST:
        os.environ['DB_HOST'] = args.DB_HOST
    if args.DB_USER:
        os.environ['DB_USER'] = args.DB_USER
    if args.DB_PASSWORD:
        os.environ['DB_PASSWORD'] = args.DB_PASSWORD
    if args.DB_NAME:
        os.environ['DB_NAME'] = args.DB_NAME
    if args.LOCAL_FLAG:
        os.environ['LOCAL_FLAG'] = args.LOCAL_FLAG
    print(vars(args))
    process_folder_in_batches(
        bucket_name=args.bucket,
        folder_path=args.folder,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()
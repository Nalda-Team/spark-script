import argparse
from datetime import datetime
from pyspark.sql.functions import input_file_name
from spark_json_parser.utils.common_utils import read_json_file
from spark_json_parser.utils.db_utils import save_flight_data_sequentially
from spark_json_parser.utils.optimize_partition import optimize_partitions
from spark_json_parser.transformers.international_transformer import process_international_flights_df
from spark_json_parser.config.schemas import get_international_schema
from config.spark_session import get_spark_session


def process_international_stream(batch_df, batch_id):
    """스트리밍 배치 단위로 국내선 항공편 처리"""
    print(f"배치 ID {batch_id} 처리 시작")

    if batch_df.isEmpty():
        print("빈 배치입니다. 처리하지 않음.")
        return

    # 브로드캐스트용 매핑 로드
    airport_map = read_json_file('maps/airport_map.json')
    airport_map_bc = batch_df.sparkSession.sparkContext.broadcast(airport_map)

    # 변환 처리
    flight_info_df, layover_info_df, fare_info_df = process_international_flights_df(batch_df,  airport_map_bc)

    if flight_info_df is not None and fare_info_df is not None:
        print(f"국제선 항공권 데이터 처리 완료 (배치 {batch_id})")
        
        # 중복 제거 및 최적화
        flight_info_df = optimize_partitions(flight_info_df, ["air_id"])
        fare_info_df = optimize_partitions(fare_info_df, 
                                    ["air_id", "seat_class", "agt_code", "fetched_date", "fare_class"])
        
        if layover_info_df is not None:
            layover_info_df = optimize_partitions(layover_info_df, 
                                                ["air_id", "segment_id", "layover_order"])
        
        # 순차적으로 데이터 저장 (layover_info 포함)
        save_flight_data_sequentially(flight_info_df, fare_info_df, layover_info_df)
        print(f"배치 ID {batch_id} 처리 완료")
    else:
        print(f"배치 ID {batch_id}: 처리 실패 또는 데이터 없음")


def process_folder_in_stream(bucket_name, folder_prefix, checkpoint_dir, max_files_per_trigger=10, processing_interval="1 minute"):
    """지정 GCS 폴더에서 파일을 스트리밍 처리"""
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    gcs_path = f"gs://{bucket_name}/{folder_prefix}"
    print(f"스트리밍 시작: {gcs_path}")

    # 스트리밍 DataFrame 생성
    streaming_df = spark.readStream \
        .schema(get_international_schema()) \
        .format("json") \
        .option("maxFilesPerTrigger", max_files_per_trigger) \
        .load(gcs_path) \
        .withColumn("file_path", input_file_name())

    # 스트리밍 쿼리 실행
    query = streaming_df.writeStream \
        .foreachBatch(process_international_stream) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=processing_interval) \
        .start()

    try:
        query.awaitTermination()
    finally:
        print("스트리밍 종료 중...")
        spark.stop()
        print("완료")


def main():
    # ArgumentParser 생성
    parser = argparse.ArgumentParser(description='국제선 항공편 스트리밍 데이터 처리 스크립트')
    
    # 인자 추가
    parser.add_argument('--bucket', 
                        type=str, 
                        default='origin_fetched_flight_data_bucket', 
                        help='GCS 버킷 이름 (기본값: origin_fetched_flight_data_bucket)')
    
    parser.add_argument('--folder', 
                        type=str, 
                        default='2025-04-30/international', 
                        help='처리할 폴더 경로 (기본값: 2025-04-30/international)')
    
    parser.add_argument('--checkpoint-dir', 
                        type=str, 
                        default=None, 
                        help='체크포인트 디렉토리 (기본값: gs://spark-checkpoint-bucket/flight-data-streaming/international/날짜)')
    
    parser.add_argument('--max-files', 
                        type=int, 
                        default=300, 
                        help='트리거당 최대 파일 수 (기본값: 300)')
    
    parser.add_argument('--processing-interval', 
                        type=str, 
                        default='1 minute', 
                        help='처리 간격 (기본값: 1 minute)')
    
    # 인자 파싱
    args = parser.parse_args()
    
    # 체크포인트 디렉토리 기본값 설정
    if args.checkpoint_dir is None:
        today = datetime.now().strftime("%Y-%m-%d")
        args.checkpoint_dir = f"gs://spark-checkpoint-bucket/flight-data-streaming/international/{today}"
    
    # 주 처리 함수 호출
    process_folder_in_stream(
        bucket_name=args.bucket, 
        folder_prefix=args.folder, 
        checkpoint_dir=args.checkpoint_dir,
        max_files_per_trigger=args.max_files,
        processing_interval=args.processing_interval
    )

if __name__ == "__main__":
    main()
import argparse
from spark_json_parser.utils.common_utils import read_json_file
from spark_json_parser.utils.db_utils import save_flight_data_sequentially, cleanup_database_connections
from config.spark_session import get_spark_session
from spark_json_parser.utils.optimize_partition import optimize_partitions
from transformers.international_transformer import process_international_flights_df
from config.schemas import get_international_schema
from spark_json_parser.utils.gcs_utils import list_gcs_files
from pyspark.sql.functions import input_file_name

def create_batches(file_list, batch_size=10):
    """파일 목록을 배치로 분할"""
    batches = []
    for i in range(0, len(file_list), batch_size):
        batches.append(file_list[i:i+batch_size])
    return batches

def process_international_batch(spark, file_paths):
    """국내선 항공편 파일 배치 처리"""
    # 매핑 정보 로드
    airport_map = read_json_file('maps/airport_map.json')
    airport_map_bc = spark.sparkContext.broadcast(airport_map)
    schema = get_international_schema()
    df = spark.read.schema(schema).json(file_paths).withColumn("file_path", input_file_name())
    
    # 국내선 항공편 처리
    flight_info_df, layover_info_df, fare_info_df = process_international_flights_df(df, airport_map_bc)
    print("국제선 항공권 데이터 처리 완료")
    if flight_info_df is not None and fare_info_df is not None:
        
        # 중복 제거 및 파티션 최적화
        flight_info_df = optimize_partitions(flight_info_df, ["air_id"])
        
        # fare_info는 복합 키로 중복 제거
        fare_info_df = optimize_partitions(fare_info_df, 
                                            ["air_id", "seat_class", "agt_code", "fetched_date", "fare_class"])
        
        # 경유 정보가 있는 경우 최적화
        if layover_info_df is not None and not layover_info_df.isEmpty():
            layover_info_df = optimize_partitions(layover_info_df, 
                                                ["air_id", "segment_id", "layover_order"])
        
        # 순차적으로 데이터 저장 (layover_info 포함)
        save_flight_data_sequentially(flight_info_df, fare_info_df, layover_info_df)
        
        print("국제선 데이터 저장 완료")
    else:
        print('국제선 처리 실패')
    

def process_folder_in_batches(bucket_name, folder_path, batch_size=10):
    """폴더 내 파일들을 배치로 처리"""
    # Spark 세션 생성
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 파일 목록 가져오기
    file_list = list_gcs_files(spark, bucket_name, folder_path)
    file_list = [f"gs://{bucket_name}/{file}" for file in file_list if file.endswith('international.json')]
    batches = create_batches(file_list, batch_size)    
    # 배치 처리
    for i, batch in enumerate(batches):
        print(f"배치 {i+1}/{len(batches)} 처리 시작 ({len(batch)}개 파일)")
        process_international_batch(spark,  batch,)
        print(f"배치 {i+1}/{len(batches)} 처리 완료")
        
    print("모든 배치 처리 완료")

    # 데이터베이스 연결 정리
    cleanup_database_connections()
    
    # Spark 세션 종료
    spark.stop()

def main():
    # ArgumentParser 생성
    parser = argparse.ArgumentParser(description='국제선 항공편 데이터 배치 처리 스크립트')
    
    # 인자 추가
    parser.add_argument('--bucket', 
                        type=str, 
                        default='origin_fetched_flight_data_bucket', 
                        help='GCS 버킷 이름 (기본값: origin_fetched_flight_data_bucket)')
    
    parser.add_argument('--folder', 
                        type=str, 
                        default='2025-04-22', 
                        help='처리할 폴더 경로 (기본값: 2025-04-22)')
    
    parser.add_argument('--batch-size', 
                        type=int, 
                        default=20, 
                        help='배치당 파일 수 (기본값: 10)')
    
    # 인자 파싱
    args = parser.parse_args()
    
    # 주 처리 함수 호출
    process_folder_in_batches(
        bucket_name=args.bucket, 
        folder_path=args.folder, 
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()
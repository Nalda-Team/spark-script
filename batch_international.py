import argparse
from spark_json_parser.maps.airport_map import airport_map
from config.spark_session import get_spark_session
from spark_json_parser.utils.batch_utils import create_batches
from config.schemas import get_international_schema
from spark_json_parser.utils.gcs_utils import list_gcs_files
from pyspark.sql.functions import input_file_name
import time
import os
from spark_json_parser.process_functions import process_international_raw_df    

def process_folder_in_batches(bucket_name, folder_path, batch_size=10):
    """폴더 내 파일들을 배치로 처리"""
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    # 공항 timezone map 브로드 캐스팅
    timezone_dict = {code: info["time_zone"] for code, info in airport_map.items()}
    timezone_map_bc=spark.sparkContext.broadcast(timezone_dict)
    
    file_list = list_gcs_files(spark, bucket_name, folder_path)
    file_list = [f"gs://{bucket_name}/{file}" for file in file_list if file.endswith('international.json')]
    batches = create_batches(file_list, batch_size)

    for i, batch in enumerate(batches, start=1):
        print(f"배치 {i}/{len(batches)} 처리 시작 ({len(batch)}개 파일)")
        raw_df = spark.read.schema(get_international_schema()).json(batch).withColumn("file_path", input_file_name())
        process_international_raw_df(raw_df, timezone_map_bc)
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
    process_folder_in_batches(
        bucket_name=args.bucket,
        folder_path=args.folder,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()
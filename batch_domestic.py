import argparse
from config.spark_session import get_spark_session
from spark_json_parser.utils.batch_utils import create_batches
from spark_json_parser.maps.airport_map import airport_map
from spark_json_parser.config.schemas import get_domestic_schema
from spark_json_parser.utils.gcs_utils import list_gcs_files
from pyspark.sql.functions import input_file_name
import os
from spark_json_parser.process_functions import process_domestic_raw_df

def process_folder_in_batches(bucket_name, folder_path, batch_size):
    """폴더 내 파일들을 배치로 처리"""
    # Spark 세션 생성
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    timezone_dict = {code: info["time_zone"] for code, info in airport_map.items()}
    # 공항 timezone map 브로드 캐스팅
    timezone_map_bc=spark.sparkContext.broadcast(timezone_dict)
    
    # 파일 목록 가져오기
    file_list = list_gcs_files(spark, bucket_name, folder_path)
    # 파일 타입에 따라 필터링
    file_list = [f"gs://{bucket_name}/{file}" for file in file_list if file.endswith(f'domestic.json')]
    print(f"총 {len(file_list)}개 파일 발견")
    
    # 배치 생성
    batches = create_batches(file_list, batch_size)
    print(f"{len(batches)}개 배치로 분할 (배치당 최대 {batch_size}개 파일)")
    
    # 배치 처리
    for i, batch in enumerate(batches):
        print(f"배치 {i+1}/{len(batches)} 처리 시작 ({len(batch)}개 파일)")
        raw_df = spark.read.schema(get_domestic_schema()).json(batch).withColumn("file_path", input_file_name())
        process_domestic_raw_df(raw_df,  timezone_map_bc)
        print(f"배치 {i+1}/{len(batches)} 처리 완료")
    
    print("모든 배치 처리 완료")
    
    # Spark 세션 종료
    spark.stop()

def main():
    # ArgumentParser 생성
    parser = argparse.ArgumentParser(description='국내선 항공편 데이터 배치 처리 스크립트')
    
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
    
    parser.add_argument('--bucket', 
                        type=str, 
                        default=None, 
                        help='GCS 버킷 이름')
    
    parser.add_argument('--folder', 
                        type=str, 
                        default=None, 
                        help='처리할 폴더 경로 (기본값: 2025-04-22/domestic)')
    
    parser.add_argument('--batch-size', 
                        type=int, 
                        default=60, 
                        help='배치당 파일 수 (기본값: 60)')
    
    # 인자 파싱
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

    # 주 처리 함수 호출
    process_folder_in_batches(
        bucket_name=args.bucket, 
        folder_path=args.folder, 
        batch_size=args.batch_size,
    )

if __name__ == "__main__":
    main()
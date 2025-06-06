from pyspark.sql.functions import input_file_name
from spark_json_parser.config.schemas import get_domestic_schema
from config.spark_session import get_spark_session
from spark_json_parser.maps.airport_map import airport_map
from pyspark.sql.streaming import StreamingQueryException
from spark_json_parser.process_functions import process_domestic_raw_df
import argparse
import time
import os


def process_folder_in_stream(bucket_name, folder_prefix, checkpoint_dir, max_files_per_trigger=10, processing_interval="1 minute", timeout_minutes=10):
    """지정 GCS 폴더에서 파일을 스트리밍 처리"""
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 공항 timezone map 브로드 캐스팅
    timezone_dict = {code: info["time_zone"] for code, info in airport_map.items()}
    timezone_map_bc=spark.sparkContext.broadcast(timezone_dict)
    
    gcs_path = f"gs://{bucket_name}/{folder_prefix}"
    print(f"스트리밍 시작: {gcs_path}")

    # 마지막 배치 처리 시간을 저장할 변수
    last_batch_time = time.time()

    # 배치 처리 및 타임아웃 체크 함수
    def process_batch_with_timeout(batch_df, batch_id):
        nonlocal last_batch_time
        
        # 데이터가 있을 때만 시간 업데이트
        if not batch_df.isEmpty():
            print('바로 다음 배치를 처리합니다.')
            process_domestic_raw_df(batch_df, timezone_map_bc)
            last_batch_time = time.time()  # 데이터 처리 후 시간 갱신
        else:
            print(f"배치 ID {batch_id}: 빈 배치")
            # 마지막 배치 처리 후 경과 시간 확인
            elapsed_minutes = (time.time() - last_batch_time) / 60
            print(f"마지막 배치 이후 경과 시간: {elapsed_minutes:.2f}분")
            
            # 타임아웃 체크
            if elapsed_minutes >= timeout_minutes:
                print(f"{timeout_minutes}분 동안 새 데이터가 없어 쿼리를 종료합니다.")
                # 현재 실행 중인 쿼리를 가져와 종료
                for query in spark.streams.active:
                    query.stop()

    # 스트리밍 DataFrame 생성
    streaming_df = spark.readStream \
        .schema(get_domestic_schema()) \
        .format("json") \
        .option("maxFilesPerTrigger", max_files_per_trigger) \
        .load(gcs_path) \
        .withColumn("file_path", input_file_name())

    # 스트리밍 쿼리 실행
    query = streaming_df.writeStream \
        .foreachBatch(process_batch_with_timeout) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=processing_interval) \
        .start()

    try:
        query.awaitTermination()
    except StreamingQueryException as e:
        print(f"스트리밍 쿼리 예외 발생: {e}")
    finally:
        print("스트리밍 종료 중...")
        spark.stop()
        print("완료")


def main():
    parser = argparse.ArgumentParser(description='국제선 항공편 스트리밍 데이터 처리 스크립트')
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
                        help='처리할 폴더 경로 (형식: 2025-04-30/domestic)')
    
    parser.add_argument('--checkpoint-dir', 
                        type=str, 
                        default=None, 
                        help='체크포인트 디렉토리 (형식: gs://{버킷 이름}/{폴더이름}/domestic/날짜)')
    
    parser.add_argument('--max-files', 
                        type=int, 
                        default=60, 
                        help='트리거당 최대 파일 수 (형식: 10)')
    
    parser.add_argument('--processing-interval', 
                        type=str, 
                        default='1 minute', 
                        help='처리 간격 (형식: 1 minute)')
    
    parser.add_argument('--timeout', 
                        type=int, 
                        default=15, 
                        help='데이터 없을 경우 타임아웃 시간(분) (형식: 15분)')
    
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

    process_folder_in_stream(
        bucket_name=args.bucket, 
        folder_prefix=args.folder, 
        checkpoint_dir=args.checkpoint_dir,
        max_files_per_trigger=args.max_files,
        processing_interval=args.processing_interval,
        timeout_minutes=args.timeout
    )

if __name__ == "__main__":
    main()
from spark_json_parser.maps.airport_map import airport_map
from config.spark_session import get_spark_session
from config.schemas import get_international_schema
from pyspark.sql.functions import input_file_name
from pyspark.sql.streaming import StreamingQueryException
import argparse
import time
import os
from spark_json_parser.process_functions import process_international_raw_df

def process_folder_in_stream(bucket_name, folder_prefix, checkpoint_dir,
                             max_files_per_trigger=10,
                             processing_interval="1 minute",
                             timeout_minutes=10):
    """지정 GCS 폴더에서 파일을 스트리밍 처리하고,
       10분간 새 파일 미수신 시 쿼리 자동 종료"""
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 공항 timezone map 브로드 캐스팅
    timezone_dict = {code: info["time_zone"] for code, info in airport_map.items()}
    timezone_map_bc=spark.sparkContext.broadcast(timezone_dict)
    
    gcs_path = f"gs://{bucket_name}/{folder_prefix}"
    print(f"스트리밍 시작: {gcs_path}")

    # 마지막 배치 처리 시각 초기화
    last_batch_time = time.time()

    def process_batch_with_timeout(batch_df, batch_id):
        nonlocal last_batch_time
        if not batch_df.rdd.isEmpty():
            last_batch_time = time.time()
            print()
            print(f"배치 ID {batch_id} 수신, 즉시 처리")
            process_international_raw_df(batch_df, timezone_map_bc)
        else:
            print(f"배치 ID {batch_id}: 빈 배치")

    streaming_df = spark.readStream \
        .schema(get_international_schema()) \
        .format("json") \
        .option("maxFilesPerTrigger", max_files_per_trigger) \
        .load(gcs_path) \
        .withColumn("file_path", input_file_name())

    query = streaming_df.writeStream \
        .foreachBatch(process_batch_with_timeout) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=processing_interval) \
        .start()

    # 모니터링 루프: 60초마다 last_batch_time 체크
    timeout_secs = timeout_minutes * 60
    try:
        while True:
            # 60초 동안 대기. 쿼리가 종료되면 True 반환
            if query.awaitTermination(60):
                print("스트리밍 쿼리 자체 종료 감지")
                break

            idle = time.time() - last_batch_time
            minutes = int(idle // 60)
            seconds = int(idle % 60)
            print(f"마지막 수신 후 경과: {minutes}분 {seconds}초")
            if idle > timeout_secs:
                print(f"{timeout_minutes}분간 새 파일 미수신, 쿼리 중지")
                query.stop()
                break

        # 쿼리 완전 종료 대기
        query.awaitTermination()
    except StreamingQueryException as e:
        print(f"스트리밍 예외 발생: {e}")
    finally:
        print("스트리밍 종료 중...")
        spark.stop()
        print("완료")

def main():
    # ArgumentParser 생성
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
    # 인자 추가
    parser.add_argument('--bucket', 
                        type=str, 
                        default=None, 
                        help='GCS 버킷 이름')
    
    parser.add_argument('--folder', 
                        type=str, 
                        default=None, 
                        help='처리할 폴더 경로 (기본값: 2025-04-30/international)')
    
    parser.add_argument('--checkpoint-dir', 
                        type=str, 
                        default=None, 
                        help='체크포인트 디렉토리 (기본값: gs://{버킷 이름}/{폴더이름}/international/날짜)')
    
    parser.add_argument('--max-files', 
                        type=int, 
                        default=60, 
                        help='트리거당 최대 파일 수 (기본값: 60)')
    
    parser.add_argument('--processing-interval', 
                        type=str, 
                        default='1 minute', 
                        help='처리 간격 (기본값: 1 minute)')
    
    parser.add_argument('--timeout', 
                        type=int, 
                        default=15, 
                        help='데이터 없을 경우 타임아웃 시간(분) (기본값: 15 분)')
    
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
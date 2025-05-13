from spark_json_parser.config.db_config import SQL_QUERIES, QUEUE_MAPPING, COLUMN_ORDER, DatabaseConnectionPool

def save_partition_with_database(partition_iterator, table_name):
    """각 파티션의 데이터를 DataBase 클래스를 사용하여 데이터베이스에 저장"""
    from pyspark import TaskContext
    print(f"{table_name} 파티션 저장 시작 - Partition: {TaskContext.get().partitionId()}")
    # 파티션 데이터를 리스트로 변환
    rows = list(partition_iterator)
    if not rows:
        return  # 빈 파티션이면 처리하지 않음
    
    # 테이블에 맞는 SQL 쿼리 가져오기
    sql_query = SQL_QUERIES.get(table_name)
    if not sql_query:
        print(f"테이블 {table_name}에 대한 SQL 쿼리가 정의되지 않았습니다.")
        return
    
    # 큐 이름 가져오기
    queue_name = QUEUE_MAPPING.get(table_name)
    
    # 컬럼 순서 가져오기
    cols = COLUMN_ORDER.get(table_name, [])
    if not cols:
        print(f"테이블 {table_name}에 대한 컬럼 순서가 정의되지 않았습니다.")
        return
    
    # 각 행을 딕셔너리에서 튜플로 변환
    params_list = []
    for row in rows:
        # row는 Row 객체이므로 dict로 변환 후 처리
        row_dict = row.asDict()
        params = tuple(row_dict.get(col) for col in cols)
        params_list.append(params)
    
    # 연결 풀에서 데이터베이스 연결 가져오기
    db = DatabaseConnectionPool().get_connection(table_name)
    
    if not db.connect():
        print(f"{queue_name}: 데이터베이스 연결 실패")
        return
    
    success, error = db.execute_values_query(queue_name, sql_query, params_list)
    if success:
        print(f"{queue_name}: 파티션 데이터 {len(params_list)}개 행 저장 완료")
    else:
        print(f"{queue_name}: 데이터 저장 실패 - {error}")

def save_flight_data_sequentially(flight_info_df, fare_info_df, layover_info_df=None):
    """항공편 데이터를 순차적으로 저장하는 함수"""
    print('순차저장을 시작합니다!!')
    # 1. 항공편 정보 저장 및 캐싱
    flight_info_df.foreachPartition(lambda partition: save_partition_with_database(partition, "flight_info"))
    # 2. 요금 정보 저장
    fare_info_df.foreachPartition(lambda partition: save_partition_with_database(partition, "fare_info"))
    # 3. 경유 정보 저장
    if layover_info_df is not None:
        layover_info_df.foreachPartition(lambda partition: save_partition_with_database(partition, "layover_info"))
    
# 데이터베이스 연결 정리 함수
def cleanup_database_connections():
    """모든 데이터베이스 연결 정리"""
    print("데이터베이스 연결 정리 중...")
    DatabaseConnectionPool().close_all()
    print("데이터베이스 연결 정리 완료")
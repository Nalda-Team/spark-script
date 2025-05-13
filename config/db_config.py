import psycopg2
from psycopg2.extras import execute_values
import traceback
import time
from spark_json_parser.utils.common_utils import read_json_file
import os

# SQL 쿼리 템플릿
SQL_QUERIES = read_json_file('maps/query_map.json')

# 테이블별 컬럼 순서 정의
COLUMN_ORDER = {
    "flight_info": ["air_id", "airline", "depart_airport", "depart_timestamp", 
                  "arrival_airport", "arrival_timestamp", "journey_time", "is_layover"],
    "fare_info": ["air_id", "seat_class", "agt_code", "adult_fare", "fetched_date", "fare_class"],
    "temp_fare_info": ["air_id", "seat_class", "agt_code", "adult_fare", "fetched_date", "fare_class"],
    "layover_info": ["air_id", "segment_id", "layover_order", "connect_time"],
    "airport_info": ["airport_code", "name", "country", "time_zone"]
}

# 테이블별 큐 이름 매핑
QUEUE_MAPPING = {
    "flight_info": "Flight_Info_Queue",
    "fare_info": "Fare_Info_Queue",
    "layover_info": "Layover_Info_Queue",
    "temp_fare_info": "Fare_Info_Queue"
}

PAGE_SIZE_MAPPING = {
    "Flight_Info_Queue": 3000,
    "Fare_Info_Queue": 5000,
    "Layover_Info_Queue": 3000,
    "Fare_Info_Queue": 3000
}

# 데이터베이스 연결 정보
DB_CONFIG = {
    'host': os.environ.get("DB_HOST"),
    'database': os.environ.get("DB_NAME"),
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD")
}

# 데이터베이스 클래스 (DB 연결, insert, 연결 종료 메소드)
class DataBase:
    def __init__(self, host, database_name, user, password, max_retries=100, retry_delay=5):
        self.host = host
        self.database = database_name
        self.user = user
        self.password = password
        self.conn = None
        self.cur = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def connect(self):
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                self.cur = self.conn.cursor()
                return True
            except Exception as e:
                retry_count += 1
                if retry_count >= self.max_retries:
                    print(f"데이터베이스 연결 최대 재시도 횟수 초과: {e}")
                    return False
                print(f"데이터베이스 연결 오류, 재시도 중 ({retry_count}/{self.max_retries}): {e}")
                time.sleep(self.retry_delay)

    def execute_values_query(self, queue_name, query, params_list):
        if not self.conn or not self.cur:
            if not self.connect():
                return False, "데이터베이스에 연결할 수 없습니다."
        
        # 큐 이름에 따라 page_size 동적 설정 (16GB RAM 환경 최적화)
        page_size = PAGE_SIZE_MAPPING[queue_name]
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                execute_values(self.cur, query, params_list, page_size=page_size)
                self.conn.commit()
                return True, None
            
            except (psycopg2.errors.DeadlockDetected, psycopg2.errors.SerializationFailure) as e:
                # 데드락 또는 직렬화 오류 처리 - 재시도하는 것이 좋음
                retry_count += 1
                error_traceback = traceback.format_exc()
                self.conn.rollback()
                
                if retry_count >= self.max_retries:
                    print(f"{queue_name} 쿼리 실행 중 데드락 발생, 최대 재시도 횟수 초과")
                    return False, error_traceback
                    
                print(f"{queue_name} 쿼리 실행 중 데드락 발생, 재시도 중 ({retry_count}/{self.max_retries})")
                time.sleep(self.retry_delay * retry_count)  # 지수 백오프
        
    def close(self):
        if self.cur:
            self.cur.close()

        if self.conn:
            self.conn.close()
        
        self.cur = None
        self.conn = None

# 연결 풀 관리를 위한 싱글톤 클래스
class DatabaseConnectionPool:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnectionPool, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance
    
    def initialize(self):
        self.connections = {}
    
    def get_connection(self, table_name):
        queue_name = QUEUE_MAPPING.get(table_name, "Default_Queue")
        
        # 테이블별로 연결 재사용 (연결 풀링 효과)
        if queue_name not in self.connections:
            self.connections[queue_name] = DataBase(
                host=DB_CONFIG['host'], 
                database_name=DB_CONFIG['database'], 
                user=DB_CONFIG['user'], 
                password=DB_CONFIG['password']
            )
        
        return self.connections[queue_name]
    
    def close_all(self):
        for conn in self.connections.values():
            conn.close()
        self.connections = {}
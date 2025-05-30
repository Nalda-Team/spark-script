from spark_json_parser.config.db_config import get_db_config, get_jdbc_url, get_jdbc_props, QUERY_MAP_MIGRATION_STAGING
import psycopg2
import time
def write_and_migrate(df, staging_table, dedupe_cols, migration_sql, num_partitions=10, batch_size=30000):
    """
    1) 중복 제거 + coalesce
    2) staging_table에 bulk insert
    3) psycog2로 migrate+truncate 쿼리
    """
    JDBC_URL = get_jdbc_url()
    DB_CONFIG = get_db_config()
    JDBC_PROPS = get_jdbc_props()
    start=time.time()
    df.dropDuplicates(dedupe_cols) \
      .write \
      .mode("append") \
      .format("jdbc") \
      .option("url", JDBC_URL) \
      .option("dbtable", staging_table) \
      .option("batchsize", str(batch_size)) \
      .option("rewriteBatchedStatements", "true") \
      .options(**JDBC_PROPS) \
      .save()
    
    if migration_sql:    
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        cur.execute(migration_sql)
        conn.commit()
        cur.close()
        conn.close()
    end=time.time()
    print(f'{staging_table} insert 및 마이그레이션 완료\n소요시간({int(end-start)}초)')

def save_flight_data_sequentially(df, table_name, duplicate_cols):
    write_and_migrate(
        df,
        staging_table=f"{table_name}_staging",
        dedupe_cols=duplicate_cols,
        migration_sql=QUERY_MAP_MIGRATION_STAGING.get(table_name, None)
    )
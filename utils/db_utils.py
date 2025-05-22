from spark_json_parser.config.db_config import  QUERY_MAP_MIGRATION_STAGING, JDBC_PROPS, JDBC_URL, DB_CONFIG
import psycopg2
import time
def write_and_migrate(df, staging_table, dedupe_cols, migration_sql, num_partitions=10, batch_size=20000):
    """
    1) 중복 제거 + coalesce
    2) staging_table에 bulk insert
    3) psycog2로 migrate+truncate 쿼리
    """

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
    mid_end=time.time()
    print('DB 쓰기 소요시간', int(mid_end-start))
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute(migration_sql)  # INSERT…; TRUNCATE…;
    conn.commit()
    cur.close()
    conn.close()
    end=time.time()
    print('insert 및 migrate 소요시간', int(end-start))
    print(f'{staging_table} insert 및 마이그레이션 완료')

def save_flight_data_sequentially(flight_info_df,fare_info_df,layover_info_df=None):
    # 1) flight_info
    write_and_migrate(
        flight_info_df,
        staging_table="flight_info_staging",
        dedupe_cols=["air_id"],
        migration_sql=QUERY_MAP_MIGRATION_STAGING["flight_info"]
    )
    
    # 2) fare_info
    write_and_migrate(
        fare_info_df,
        staging_table="fare_info_staging",
        dedupe_cols=["air_id", "seat_class", "agt_code", "fetched_date", "fare_class"],
        migration_sql=QUERY_MAP_MIGRATION_STAGING["fare_info"]
    )

    # 3) layover_info (존재할 때만)
    if layover_info_df is not None:
        write_and_migrate(
            layover_info_df,
            staging_table="layover_info_staging",
            dedupe_cols=["air_id", "segment_id", "layover_order"],
            migration_sql=QUERY_MAP_MIGRATION_STAGING["layover_info"]
        )
        
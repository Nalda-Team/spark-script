import os

def get_db_config():
    return {
        'host': os.environ.get("DB_HOST"),
        'database': os.environ.get("DB_NAME"),
        'user': os.environ.get("DB_USER"),
        'password': os.environ.get("DB_PASSWORD"),
        'port': '5432'
    }

def get_jdbc_url():
    cfg = get_db_config()
    return f"jdbc:postgresql://{cfg['host']}:{cfg['port']}/{cfg['database']}"

def get_jdbc_props():
    return {
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "driver": "org.postgresql.Driver",
        "rewriteBatchedStatements": "true"
    }

QUERY_MAP_MIGRATION_STAGING = {
  "flight_info": """
    INSERT INTO flight_info
      (air_id, airline, depart_airport, depart_timestamp,
       arrival_airport, arrival_timestamp, journey_time, is_layover)
    SELECT air_id, airline, depart_airport, depart_timestamp,
           arrival_airport, arrival_timestamp, journey_time, is_layover
      FROM flight_info_staging
    ON CONFLICT (air_id) DO UPDATE
      SET airline            = EXCLUDED.airline,
          depart_airport     = EXCLUDED.depart_airport,
          depart_timestamp   = EXCLUDED.depart_timestamp,
          arrival_airport    = EXCLUDED.arrival_airport,
          arrival_timestamp  = EXCLUDED.arrival_timestamp,
          journey_time       = EXCLUDED.journey_time,
          is_layover         = EXCLUDED.is_layover;
    TRUNCATE flight_info_staging;
  """,

  "fare_info": """
    INSERT INTO fare_info
      (air_id, seat_class, agt_code, adult_fare, fetched_date, fare_class)
    SELECT air_id, seat_class, agt_code, adult_fare, fetched_date, fare_class
      FROM fare_info_staging
    ON CONFLICT (air_id, seat_class, agt_code, fetched_date, fare_class)
      DO UPDATE SET adult_fare = EXCLUDED.adult_fare;
    TRUNCATE fare_info_staging;
  """,

  "layover_info": """
    INSERT INTO layover_info
      (air_id, segment_id, layover_order, connect_time)
    SELECT air_id, segment_id, layover_order, connect_time
      FROM layover_info_staging
    ON CONFLICT (air_id, segment_id, layover_order)
      DO UPDATE SET connect_time = EXCLUDED.connect_time;
    TRUNCATE layover_info_staging;
  """
}
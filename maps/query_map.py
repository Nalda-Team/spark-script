query_map={
    "flight_info": "INSERT INTO flight_info\n        (air_id, airline, depart_airport, depart_timestamp, arrival_airport, arrival_timestamp, journey_time, is_layover)\n        VALUES %s\n        ON CONFLICT (air_id) DO UPDATE\n        SET airline = EXCLUDED.airline,\n            depart_airport = EXCLUDED.depart_airport,\n            depart_timestamp = EXCLUDED.depart_timestamp,\n            arrival_airport = EXCLUDED.arrival_airport,\n            arrival_timestamp = EXCLUDED.arrival_timestamp,\n            journey_time = EXCLUDED.journey_time,\n            is_layover = EXCLUDED.is_layover",
    
    "fare_info": "INSERT INTO fare_info \n        (air_id, seat_class, agt_code, adult_fare, fetched_date, fare_class)\n        VALUES %s\n        ON CONFLICT (air_id, seat_class, agt_code, fetched_date, fare_class) DO UPDATE\n        SET adult_fare = EXCLUDED.adult_fare",    
    
    "temp_fare_info": "INSERT INTO temp_fare_info (air_id, seat_class, agt_code, adult_fare, fetched_date, fare_class) VALUES %s\n        ON CONFLICT (air_id, seat_class, agt_code, fetched_date, fare_class) DO UPDATE\n        SET adult_fare = EXCLUDED.adult_fare",
    
    "layover_info": "INSERT INTO layover_info (air_id, segment_id, layover_order, connect_time)\n        VALUES %s\n        ON CONFLICT (air_id, segment_id, layover_order) DO UPDATE\n        SET            connect_time = EXCLUDED.connect_time",
    
    "airport_info": "INSERT INTO airport_info\n        (airport_code, name, country, time_zone) \n        VALUES %s\n        ON CONFLICT (airport_code) DO UPDATE\n        SET airport_code = EXCLUDED.airport_code,\n            name = EXCLUDED.name,\n            country = EXCLUDED.country,\n            time_zone = EXCLUDED.time_zone"
}
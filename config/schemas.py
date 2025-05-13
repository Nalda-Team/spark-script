from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType, BooleanType, MapType, DoubleType
def get_domestic_schema():
   """국내선 항공편 데이터를 위한 명시적 스키마 정의"""
   # 항공편 세부 정보 스키마
   departure_schema = StructType([
       StructField("airlineCode", StringType()),
       StructField("airlineName", StringType()),
       StructField("codeshare", StringType(), True),
       StructField("fitName", StringType()),
       StructField("depCity", StringType()),
       StructField("arrCity", StringType()),
       StructField("departureCityName", StringType()),
       StructField("arrivalCityName", StringType()),
       StructField("departureDate", StringType()),
       StructField("arrivalDate", StringType()),
       StructField("dayDiff", IntegerType()),
       StructField("departureTime", StringType()),
       StructField("arrivalTime", StringType()),
       StructField("detailedClass", StringType()),
       StructField("seatClass", StringType()),
       StructField("seatCnt", IntegerType()),
       StructField("journeyTime", StringType()),
       StructField("minFare", IntegerType()),
       StructField("supportNPay", BooleanType()),
       StructField("fare", ArrayType(
           StructType([
               StructField("agtCode", StringType()),
               StructField("bookingClass", StringType()),
               StructField("adultFare", IntegerType()),
               StructField("childFare", IntegerType()),
               StructField("aFuel", IntegerType()),
               StructField("cFuel", IntegerType()),
               StructField("aTax", IntegerType()),
               StructField("cTax", IntegerType()),
               StructField("publishFee", IntegerType()),
               StructField("etc", StringType()),
               StructField("supportNPay", BooleanType()),
               StructField("discountFare", StringType(), True)
           ])
       ))
   ])
   
   # 전체 데이터 스키마
   return StructType([
       StructField("data", StructType([
           StructField("domesticFlights", StructType([
               StructField("departures", ArrayType(departure_schema)),
               StructField("arrivals", ArrayType(departure_schema))
           ])),
           StructField("promotions", ArrayType(StringType()))
       ]))
   ])

# 국제선 항공편 JSON 스키마 정의
def get_international_schema():
    """국제선 항공편 데이터를 위한 명시적 스키마 정의"""
    # 항공사 정보 스키마
    airlines_schema = MapType(StringType(), StringType())
    
    # 공항 정보 스키마
    airports_schema = MapType(StringType(), StringType())
    
    # 요금 유형 스키마
    fare_types_schema = MapType(StringType(), StringType())
    
    # 세부 항공편 정보 스키마
    detail_schema = ArrayType(
        StructType([
            StructField("sa", StringType()),
            StructField("ea", StringType()),
            StructField("av", StringType()),
            StructField("fn", StringType()),
            StructField("sdt", StringType()),
            StructField("edt", StringType()),
            StructField("oav", StringType(), True),
            StructField("jt", StringType()),
            StructField("ft", StringType()),
            StructField("ct", StringType()),
            StructField("et", StringType(), True),
            StructField("im", BooleanType(), True),
            StructField("carbonEmission", DoubleType(), True)
        ])
    )
    
    # 각 항공편 스키마
    flight_schema = StructType([
        StructField("id", StringType()),
        StructField("detail", detail_schema),
        StructField("journeyTime", ArrayType(StringType()))
    ])
    
    # 스케줄 스키마
    schedules_schema = ArrayType(MapType(StringType(), flight_schema))
    
    # 성인/어린이/유아 요금 스키마
    person_fare_schema = StructType([
        StructField("Fare", StringType()),
        StructField("NaverFare", StringType()),
        StructField("Tax", StringType()),
        StructField("QCharge", StringType())
    ])
    
    # 예약 파라미터 스키마
    reserve_param_schema = StructType([
        StructField("#cdata-section", StringType())
    ])
    
    # 각 요금 정보 스키마
    fare_item_schema = StructType([
        StructField("Adult", person_fare_schema),
        StructField("Child", person_fare_schema),
        StructField("Infant", person_fare_schema),
        StructField("ReserveParameter", reserve_param_schema, True),
        StructField("PromotionParameter", reserve_param_schema, True),
        StructField("FareType", StringType(), True),
        StructField("AgtCode", StringType(), True),
        StructField("ConfirmType", StringType(), True),
        StructField("BaggageType", StringType(), True),
        StructField("priceTransparencyUrl", reserve_param_schema, True)
    ])
    
    # 요금 옵션 스키마
    fare_option_schema = ArrayType(fare_item_schema)
    
    # 각 항공편 요금 정보 스키마
    fare_info_schema = StructType([
        StructField("sch", ArrayType(StringType())),
        StructField("fare", MapType(StringType(), fare_option_schema))
    ])
    
    # 전체 데이터 스키마
    return StructType([
        StructField("data", StructType([
            StructField("internationalList", StructType([
                StructField("galileoKey", StringType()),
                StructField("galileoFlag", BooleanType()),
                StructField("travelBizKey", StringType()),
                StructField("travelBizFlag", BooleanType()),
                StructField("totalResCnt", IntegerType()),
                StructField("resCnt", IntegerType()),
                StructField("results", StructType([
                    StructField("airlines", airlines_schema),
                    StructField("airports", airports_schema),
                    StructField("fareTypes", fare_types_schema),
                    StructField("schedules", schedules_schema),
                    StructField("fares", MapType(StringType(), fare_info_schema)),
                    StructField("errors", ArrayType(
                        StructType([
                            StructField("agtCode", StringType()),
                            StructField("errCode", StringType()),
                            StructField("errMsg", StringType())
                        ])
                    ), True),
                    StructField("carbonEmissionAverage", StructType([
                        StructField("directFlightCarbonEmissionItineraryAverage", MapType(StringType(), DoubleType())),
                        StructField("directFlightCarbonEmissionAverage", DoubleType())
                    ]), True)
                ]))
            ]))
        ]))
    ])
def optimize_partitions(df, partition_key, num_partitions=None):
    """DataFrame 파티션 최적화 및 중복 제거 - count() 호출 없이 효율적으로 구현"""
    print('먼저 중복 제거')
    df = df.dropDuplicates(partition_key)
    return df
    # # 파티션 수 자동 결정 (count() 호출 없이)
    # if num_partitions is None:
    #     # 1. 현재 파티션 수 확인
    #     current_partitions = df.rdd.getNumPartitions()
    #     print('현재 파티션 수:', current_partitions)
    #     # 2. DataFrame 크기 추정 (전체 count 대신 샘플링 활용)
    #     # 데이터 크기 추정을 위해 Spark의 메타데이터 활용
    #     # size_estimate = df.rdd.countApprox(timeout=5, confidence=0.8)
    #     print('추정 완료')
        
    #     # 3. 적절한 파티션 수 결정 (파티션당 약 10,000행 목표)
    #     if size_estimate > 1000000:  # 대용량 데이터
    #         num_partitions = min(200, max(current_partitions, int(size_estimate / 10000)))
    #     elif size_estimate > 100000:  # 중간 크기 데이터
    #         num_partitions = min(100, max(current_partitions, int(size_estimate / 5000)))
    #     elif size_estimate > 10000:   # 소용량 데이터
    #         num_partitions = min(50, max(current_partitions, int(size_estimate / 2000)))
    #     else:  # 아주 작은 데이터셋
    #         num_partitions = max(1, min(current_partitions, 10))
        
    #     print('파티션수:', num_partitions)
    #     return df.repartition(num_partitions, partition_key)

    # else:
    #     print('정해진 파티션 수만큼 분할합니다:', num_partitions)
    #     df.repartition(num_partitions, partition_key)
    #     return df
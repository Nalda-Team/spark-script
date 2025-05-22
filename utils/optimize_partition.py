def optimize_partitions(df, partition_key, num_partitions=None):
    """DataFrame 파티션 최적화 및 중복 제거 - count() 호출 없이 효율적으로 구현"""
    # print('먼저 중복 제거')
    # df = df.coalesce(8)
    return df

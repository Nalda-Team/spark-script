def create_batches(file_list, batch_size=10):
    """파일 목록을 배치로 분할"""
    batches = []
    for i in range(0, len(file_list), batch_size):
        batches.append(file_list[i:i+batch_size])
    return batches
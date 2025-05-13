def list_gcs_files(spark, bucket_name, folder_path):
    """GCS 버킷의 특정 폴더에 있는 파일 목록 가져오기"""
    gcs_path = f"gs://{bucket_name}/{folder_path}"
    file_list = []
    
    # Hadoop 파일 시스템 API를 사용하여 파일 목록 가져오기
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI(gcs_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(gcs_path))
    
    for file_status in status:
        file_path = file_status.getPath().toString()
        if not file_status.isDirectory():
            # gs://bucket/folder/file.json 형태에서 folder/file.json 추출
            relative_path = file_path.split(f"gs://{bucket_name}/")[1]
            file_list.append(relative_path)
    
    return file_list
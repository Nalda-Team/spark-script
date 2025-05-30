from pyspark.sql import SparkSession
import os

def get_spark_session(local_flag='Y'):
    local_flag=os.environ.get("LOCAL_FLAG", 'Y')
    print(local_flag)
    if local_flag == 'Y':
        MASTER = "local[*]"
        SERVICE_ACCOUNT_KEY_PATH = '/Users/gunu/Desktop/custom_scripts/spark_json_parser/gcp-key.json'
        GCS_CONNECTOR_JAR = "/Users/gunu/Desktop/jars/gcs-connector-hadoop3-latest.jar"
        POSTGRESQL_JAR = "/Users/gunu/Desktop/jars/postgresql-42.5.4.jar"
        jar_config = f"{GCS_CONNECTOR_JAR},{POSTGRESQL_JAR}"
    else:
        MASTER = "spark://spark-master:7077"
        SERVICE_ACCOUNT_KEY_PATH = '/opt/spark/keys/gcp-key.json'
        jar_config = None

    builder = SparkSession.builder.appName("Flight Data Processing").master(MASTER)
    
    if jar_config:
        builder = builder.config("spark.jars", jar_config)

    spark = builder \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.path.abspath(SERVICE_ACCOUNT_KEY_PATH)) \
        .config("spark.driver.maxResultSize", "2560m") \
        .config("spark.driver.memory", "2560m") \
        .config("spark.executor.memory", "2560m") \
        .config("spark.sql.shuffle.partitions", '4') \
        .getOrCreate()

    # Hadoop 설정 추가
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("google.cloud.auth.service.account.enable", "true")
    hadoop_conf.set("google.cloud.auth.service.account.json.keyfile", os.path.abspath(SERVICE_ACCOUNT_KEY_PATH))
    
    return spark
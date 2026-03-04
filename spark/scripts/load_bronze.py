
# load_bronze.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

def main(file_name:str, target_table:str):
    spark = SparkSession.builder.getOrCreate()
    
    local_file = f"/tmp/{file_name}"
    
    # 1. 讀取原始數據
    df = spark.read.parquet(local_file)
    
    # 2. 注入 Bronze 層必備的追蹤元數據
    df_to_load = df.withColumn("load_time", current_timestamp()) \
        .withColumn("source_file", lit(local_file))
    
    # 3. 執行寫入 (Iceberg 會根據 PARTITIONED BY 自動處理)
    df_to_load.writeTo(target_table).append()


if __name__ == "__main__":
    # 接收 Airflow 傳入的參數:  
    if len(sys.argv) < 3:
        print("Usage: load_bronze.py")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
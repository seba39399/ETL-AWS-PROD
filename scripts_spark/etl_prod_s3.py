import os
import sys
import boto3
from pyspark.sql import SparkSession

#---- Set up environment variables for PySpark and Hadoop ----#

PYTHON_EXE = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE
os.environ['HADOOP_HOME'] = r'C:\\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\\hadoop\\bin'

#----AWS S3 Configuration----#
S3_BUCKET_NAME = "etlprodtest" 
DB_FILE = "empresa_oltp.db"

def upload_to_s3(local_folder, s3_prefix):

    """Uploads a file to an S3 bucket."""

    s3_client = boto3.client('s3')

    print(f"Uploading {local_folder} to s3://{S3_BUCKET_NAME}/{s3_prefix}")

    for root, _, files in os.walk(local_folder):
        for file in files:
            if file.endswith(".crc"):
                continue
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_folder)
            s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")

            s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_path)
            print(f"Uploaded {local_path} to s3://{S3_BUCKET_NAME}/{s3_path}")

#---Main function to perform ETL from SQLite to S3 using Spark----#

def main():
    spark = SparkSession.builder \
        .appName("ETL Prod S3") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.xerial:sqlite-jdbc:3.45.1.0") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    #----Bronze Layer: Read data from SQLite database----#
    print("Reading data from SQLite database (Bronze Layer).")
    jdbc_url = f"jdbc:sqlite:{DB_FILE}"

    df_bronze = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "orders") \
        .option("driver", "org.sqlite.JDBC") \
        .load()

    df_bronze.createOrReplaceTempView("bronze_orders")

    path_bronze_local = "output/bronze/orders"
    df_bronze.write.mode("overwrite").parquet(path_bronze_local)
    print(f"Bronze Layer data written to local path: {path_bronze_local}")

    #----Silver Layer: Data Cleaning and Transformation----#
    print("Starting data cleaning and transformation (Silver Layer).")

    df_silver = spark.sql("""
        WITH base_cleaned AS(
            SELECT
                TRIM(order_id) AS order_id,
                INITCAP(TRIM(customer_name)) AS customer_name,
                LOWER(TRIM(customer_email)) AS customer_email,
                CAST(amount AS DOUBLE) AS amount,
                UPPER(TRIM(country)) AS country,
                COALESCE(
                    TRY_TO_TIMESTAMP(order_date, 'yyyy-MM-dd HH:mm:ss'),
                    TRY_TO_TIMESTAMP(order_date, 'yyyy/MM/dd')
                ) AS order_timestamp
            FROM bronze_orders
            WHERE order_id IS NOT NULL
                AND customer_email LIKE '%@%'
                AND CAST(amount AS DOUBLE) > 0
        ),
        deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY order_timestamp DESC
                ) AS row_num
            FROM base_cleaned
        )
        SELECT
            order_id,
            customer_name,
            customer_email,
            amount,
            country,
            order_timestamp
        FROM deduplicated
        WHERE row_num = 1
    """)

    df_silver.createOrReplaceTempView("silver_orders")

    print("Silver Layer DataFrame:")
    df_silver.show(truncate=False)

    path_silver_local = "output/silver/orders"
    df_silver.write.mode("overwrite").parquet(path_silver_local)

    #---Gold Layer: Aggregation and Final Output---#
    print("Starting aggregation for Gold Layer.")

    df_gold = spark.sql("""
        SELECT
            country,
            COUNT(order_id) AS total_orders,
            COUNT(DISTINCT customer_email) AS total_unique_customers,
            ROUND(SUM(amount), 2) AS total_revenue,
            ROUND(AVG(amount), 2) AS average_order_value
        FROM silver_orders
        GROUP BY country
        ORDER BY total_revenue DESC
    """)

    print("Gold Layer DataFrame:")
    df_gold.show(truncate=False)

    path_gold_local = "output/gold/country_metrics" 
    df_gold.write.mode("overwrite").parquet(path_gold_local)

    spark.stop()

    #---Upload to S3---#
    upload_to_s3(path_bronze_local, "bronze/orders")
    upload_to_s3(path_silver_local, "silver/orders")
    upload_to_s3(path_gold_local, "gold/country_metrics")

    print("ETL process completed successfully. Data uploaded to S3.")

if __name__ == "__main__":
    main()

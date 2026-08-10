import pytest 
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, count, when
from scripts_spark.silver_transform import transform_silver

# --- DUMMY WINUTILS FOR WINDOWS PYTEST ---
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    hadoop_dir = os.path.join(tempfile.gettempdir(), "hadoop_dummy")
    bin_dir = os.path.join(hadoop_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    
    winutils_path = os.path.join(bin_dir, "winutils.exe")
    if not os.path.exists(winutils_path):
        with open(winutils_path, "w") as f:
            pass

    os.environ["HADOOP_HOME"] = hadoop_dir

DB_PATH = "empresa_oltp.db"

@pytest.fixture(scope="module")

def spark_session():
    spark = SparkSession.builder \
        .appName("Test-Silver") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.xerial:sqlite-jdbc:3.45.1.0") \
        .getOrCreate()

    yield spark
    spark.stop()

def test_silver_pipeline(spark_session: SparkSession):

    assert os.path.exists(DB_PATH), f"Data base not found in {DB_PATH}"

    df_bronze = spark_session.read \
        .format("jdbc") \
        .option("url",f"jdbc:sqlite:{DB_PATH}") \
        .option("dbtable", "orders") \
        .option("driver", "org.sqlite.JDBC") \
        .load()

    total_bronze_count = df_bronze.count()
    assert total_bronze_count > 0, "Orders table is empty"

    df_silver = transform_silver(spark_session, df_bronze)
    df_silver.cache()

    total_silver_count = df_silver.count()

    duplicate_orders = df_silver.groupBy("order_id").count().filter(col("count") > 1).count()
    assert duplicate_orders == 0, f"Se encontraron {duplicate_orders} order_id duplicados en Silver."

    null_counts = df_silver.select(
        count(when(col("order_id").isNull(), 1)).alias("null_orders"),
        count(when(col("customer_email").isNull(), 1)).alias("null_emails"),
        count(when(col("amount").isNull(), 1)).alias("null_amounts")
    ).collect()[0]

    assert null_counts["null_orders"] == 0, "Existen order_id nulos en Silver."
    assert null_counts["null_emails"] == 0, "Existen correos nulos en Silver."
    assert null_counts["null_amounts"] == 0, "Existen montos nulos en Silver."

    invalid_emails = df_silver.filter(~col("customer_email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")).count()
    assert invalid_emails == 0, f"Se encontraron {invalid_emails} correos con formato inválido."

    invalid_amounts = df_silver.filter(col("amount") <= 0).count()
    assert invalid_amounts == 0, f"Se encontraron {invalid_amounts} registros con monto <= 0."

    data_retention_ratio = total_silver_count / total_bronze_count
    assert data_retention_ratio >= 0.60, (
        f"Massive Data Loss Alert: Only {data_retention_ratio:.2%} was retained "
        f"from the original Bronze records."
    )
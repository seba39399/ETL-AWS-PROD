from pyspark.sql import SparkSession, DataFrame

def transform_silver(spark: SparkSession, df_bronze: DataFrame) -> DataFrame:

    df_bronze.createOrReplaceTempView("bronze_orders")

    sql_query = """
        WITH base_cleaned AS(
        SELECT
            TRIM(order_id) AS order_id,
            INITCAP(TRIM(customer_name)) AS customer_name,
            LOWER(TRIM(customer_email)) AS customer_email,
            CAST(amount AS DOUBLE) AS amount,
            UPPER(TRIM(country)) AS country,
            COALESCE(
                TRY_TO_TIMESTAMP(order_date, 'yyyy-MM-dd HH:mm:ss'),
                TRY_TO_TIMESTAMP(order_date, 'yyyy-MM-dd HH:mm:ss')
            ) AS order_timestamp
        FROM bronze_orders
        WHERE order_id IS NOT NULL
            AND customer_email LIKE '%@%.%'
            AND CAST(amount AS DOUBLE) > 0
        ),
        deduplicated AS(
            SELECT
                *,
                ROW_NUMBER() OVER(
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
    """

    return spark.sql(sql_query)
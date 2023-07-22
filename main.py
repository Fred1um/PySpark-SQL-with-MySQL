import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# MySQL credentials
MYSQL_HOST = 'host'
MYSQL_PORT = 0000
MYSQL_DATABASE = 'database'
MYSQL_TABLE = 'table'
MYSQL_USER = 'user'
MYSQL_PASSWORD = 'password'


dim_columns = ['id', 'name']
vendor = [
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc'),
]
rates = [
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
]
payment = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
]

trips_schema = StructType([
    StructField('vendor_id', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('ratecode_id', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pulocation_id', IntegerType(), True),
    StructField('dolocation_id', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType()),
])


def agg_calc(spark: SparkSession) -> DataFrame:
    data_path = os.path.join(Path(__name__).parent, r"path\to\your\data", '*.csv')

    trip_df = spark.read \
        .option("header", "true") \
        .schema(trips_schema) \
        .csv(data_path)

    datamart = trip_df \
        .where(trip_df['vendor_id'].isNotNull()) \
        .groupBy(trip_df['vendor_id'],
                 trip_df['payment_type'],
                 trip_df['ratecode_id'],
                 f.to_date(trip_df['tpep_pickup_datetime']).alias('dt')
                 ) \
        .agg(f.sum(trip_df['total_amount']).alias('sum_amount'), f.avg(trip_df['tip_amount']).alias("avg_tips")) \
        .select(f.col('dt'),
                f.col('vendor_id'),
                f.col('payment_type'),
                f.col('ratecode_id'),
                f.col('sum_amount'),
                f.col('avg_tips')) \
        .orderBy(f.col('dt').desc(), f.col('vendor_id'))

    return datamart


def create_dict(spark: SparkSession, header: list[str], data: list):
    df = spark.createDataFrame(data=data, schema=header)
    return df


def save_to_mysql(host: str, port: int, db_name: str, username: str, password: str, df: DataFrame, table_name: str):
    props = {
        'user': f'{username}',
        'password': f'{password}',
        'driver': 'com.mysql.cj.jdbc.Driver',
        "ssl": "true",
        "sslmode": "none",
    }

    df.write.mode("append").jdbc(
        url=f'jdbc:mysql://{host}:{port}/{db_name}',
        table=table_name,
        properties=props)


def main(spark: SparkSession):
    vendor_dim = create_dict(spark, dim_columns, vendor)
    payment_dim = create_dict(spark, dim_columns, payment)
    rates_dim = create_dict(spark, dim_columns, rates)

    datamart = agg_calc(spark).cache()
    datamart.show(truncate=False, n=100)

    joined_datamart = datamart \
        .join(other=vendor_dim, on=vendor_dim['id'] == f.col('vendor_id'), how='inner') \
        .join(other=payment_dim, on=payment_dim['id'] == f.col('payment_type'), how='inner') \
        .join(other=rates_dim, on=rates_dim['id'] == f.col('ratecode_id'), how='inner') \
        .select(f.col('dt'),
                f.col('vendor_id'), f.col('payment_type'), f.col('ratecode_id'), f.col('sum_amount'),
                f.col('avg_tips'),
                rates_dim['name'].alias('rate_name'), vendor_dim['name'].alias('vendor_name'),
                payment_dim['name'].alias('payment_name'),
                )

    joined_datamart.show(truncate=False, n=100)

    joined_datamart.write.mode('overwrite').csv('output')

    save_to_mysql(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=MYSQL_DATABASE,
        username=MYSQL_USER,
        password=MYSQL_PASSWORD,
        df=joined_datamart,
        table_name=f'{MYSQL_DATABASE}.{MYSQL_TABLE}'
    )


if __name__ == '__main__':
    main(SparkSession
         .builder
         .config("spark.jars", r".\jars\mysql-connector-java-8.0.25.jar")
         .appName('Spark Job')
         .getOrCreate())


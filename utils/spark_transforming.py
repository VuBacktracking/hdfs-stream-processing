import findspark
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.utils import AnalysisException
from dotenv import load_dotenv
import logging
import os

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
INPUT_DATA_PATH = 'hdfs:///user/stream_data/KETI/'
FINAL_OUTPUT_PATH = 'hdfs:///user/stream_data/output/sensors.parquet'

# Initialize Spark
findspark.init("/home/vuphan/spark-3.5.1")
spark = SparkSession.builder \
    .appName("Spark Read and Write") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

def get_hdfs_file_system():
    try:
        uri = spark.sparkContext._gateway.jvm.java.net.URI(INPUT_DATA_PATH)
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        FileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
        return FileSystem.get(uri, conf)
    except Exception as e:
        logger.error(f"Error getting HDFS FileSystem: {e}")
        return None

def list_hdfs_directories(fs, path):
    try:
        status = fs.listStatus(spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(path))
        return [file.getPath().getName() for file in status if file.isDirectory()]
    except Exception as e:
        logger.error(f"Error listing HDFS directories: {e}")
        return []

def list_hdfs_files(fs, path):
    try:
        status = fs.listStatus(spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(path))
        return [file.getPath().getName() for file in status if file.isFile()]
    except Exception as e:
        logger.error(f"Error listing HDFS files: {e}")
        return []

def create_separate_dataframes(fs) -> dict:
    """
    Creates a dictionary that includes room numbers as keys and dataframes per room as values
    """
    dataframes = {}
    columns = ['co2', 'humidity', 'light', 'pir', 'temperature']

    dirs = list_hdfs_directories(fs, INPUT_DATA_PATH)
    
    for dirname in dirs:
        directory = f"{INPUT_DATA_PATH}/{dirname}"
        files = list_hdfs_files(fs, directory)
        
        count = 0
        for filename in files:
            filepath = f"{directory}/{filename}"
            my_path = f"{dirname}_{filename.split('.')[0]}"
            df = spark.read.csv(filepath, header=True, inferSchema=True)
            df = df.toDF('ts_min_bignt', columns[count])
            dataframes[my_path] = df
            count += 1

        try:
            dataframes[f'{dirname}_co2'].createOrReplaceTempView('df_co2')
            dataframes[f'{dirname}_humidity'].createOrReplaceTempView('df_humidity')
            dataframes[f'{dirname}_light'].createOrReplaceTempView('df_light')
            dataframes[f'{dirname}_pir'].createOrReplaceTempView('df_pir')
            dataframes[f'{dirname}_temperature'].createOrReplaceTempView('df_temperature')

            dataframes[dirname] = spark.sql('''
                SELECT
                  df_co2.*,
                  df_humidity.humidity,
                  df_light.light,
                  df_pir.pir,
                  df_temperature.temperature        
                FROM df_co2
                INNER JOIN df_humidity
                  ON df_co2.ts_min_bignt = df_humidity.ts_min_bignt
                INNER JOIN df_light
                  ON df_humidity.ts_min_bignt = df_light.ts_min_bignt
                INNER JOIN df_pir
                  ON df_light.ts_min_bignt = df_pir.ts_min_bignt
                INNER JOIN df_temperature
                  ON df_pir.ts_min_bignt = df_temperature.ts_min_bignt      
            ''').withColumn("room", F.lit(dirname))
        except AnalysisException as e:
            logger.error(f"Error creating dataframe for room {dirname}: {e}")
    
    return dataframes

def merge_dataframes(dfs):
    """
    Merges multiple dataframes vertically.
    """
    try:
        return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
    except Exception as e:
        logger.error(f"Error in merge_dataframes: {e}")
        return None

def standardize_schema(df_list):
    """
    Standardizes the schema of all DataFrames in the list to ensure they have the same columns.
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    # Define a common schema that includes all possible columns
    common_schema = StructType([
        StructField("ts_min_bignt", StringType(), True),
        StructField("co2", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("light", DoubleType(), True),
        StructField("pir", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("room", StringType(), True),
    ])

    standardized_dfs = []
    for df in df_list:
        for field in common_schema.fields:
            if field.name not in df.columns:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        standardized_dfs.append(df.select([field.name for field in common_schema.fields]))

    return standardized_dfs

def create_main_dataframe(separate_dataframes: dict):
    """
    Merges all per-room dataframes vertically. Creates final dataframe.
    """
    try:
        # Collect all DataFrames
        dataframes_to_concat = list(separate_dataframes.values())
        
        # Standardize schema for all DataFrames
        standardized_dfs = standardize_schema(dataframes_to_concat)
        
        # Merge DataFrames
        df = reduce(DataFrame.unionAll, standardized_dfs)
        df = df.sort(F.col("ts_min_bignt"))

        df = df.dropna()

        df_main = df.withColumn("event_ts_min", F.from_unixtime(F.col("ts_min_bignt")).cast(DateType()))
        df_main = df_main.withColumn("event_ts_min", F.date_format(F.col("event_ts_min"), "yyyy-MM-dd HH:mm:ss"))

        logger.info("Successfully created main dataframe.")
        return df_main
    except Exception as e:
        logger.error(f"Error in create_main_dataframe: {e}")
        return None


def write_dataframe(df):
    """
    Writes the final dataframe to HDFS in Parquet format.
    """
    try:
        df.write.parquet(FINAL_OUTPUT_PATH, mode='overwrite')
        logger.info("Successfully written dataframe to HDFS!")
    except Exception as e:
        logger.error(f"Error in write_dataframe: {e}")

if __name__ == "__main__":
    fs = get_hdfs_file_system()
    if fs:
        room_dataframes = create_separate_dataframes(fs)
        if room_dataframes:
            main_df = create_main_dataframe(room_dataframes)
            if main_df:
                write_dataframe(main_df)
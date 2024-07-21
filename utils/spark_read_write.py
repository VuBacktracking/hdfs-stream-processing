import findspark
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.utils import AnalysisException
from dotenv import load_dotenv

load_dotenv()

INPUT_DATA_PATH = 'hdfs:///tmp/stream_data/KETI/'
FINAL_OUTPUT_PATH = 'hdfs:///tmp/output/sensors.parquet'  # Final Parquet file path

findspark.init("/home/vuphan/spark-3.5.1")  # This is where local spark is installed
spark = SparkSession.builder \
    .appName("Spark Read and Write") \
    .master("local[2]") \
    .getOrCreate()

def create_separate_dataframes() -> dict:
    """
    Creates a dictionary that includes room numbers as keys and dataframes per room as values
    """
    dataframes = {}  # This dict saves property dataframe per room as values.
    columns = ['co2', 'humidity', 'light', 'pir', 'temperature']
    
    # List directories in HDFS
    dirs = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem \
        .get(spark.sparkContext._gateway.jvm.java.net.URI(INPUT_DATA_PATH), spark.sparkContext._jsc.hadoopConfiguration()) \
        .listStatus(spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(INPUT_DATA_PATH))
    
    dirs = [file.getPath().getName() for file in dirs if file.isDirectory()]
    
    for dirname in dirs:  # loop through the folders under KETI
        directory = f"{INPUT_DATA_PATH}/{dirname}"
        dataframes_room = {}  # This dict saves dataframes per room.
        files = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem \
            .get(spark.sparkContext._gateway.jvm.java.net.URI(directory), spark.sparkContext._jsc.hadoopConfiguration()) \
            .listStatus(spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(directory))
        
        files = [file.getPath().getName() for file in files if file.isFile()]
        
        count = 0
        for filename in files:  # loop through the files under each room folder
            filepath = f"{directory}/{filename}"
            my_path = f"{dirname}_{filename.split('.')[0]}"  # e.g. 656_co2
            dataframes[my_path] = spark.read.csv(filepath, header=True, inferSchema=True)
            dataframes[my_path] = dataframes[my_path].toDF('ts_min_bignt', columns[count])  # Sample key: 656_co2. Dataframe columns: ts_min_bignt, co2
            count += 1

        dataframes[f'{dirname}_co2'].createOrReplaceTempView('df_co2')
        dataframes[f'{dirname}_humidity'].createOrReplaceTempView('df_humidity')
        dataframes[f'{dirname}_light'].createOrReplaceTempView('df_light')
        dataframes[f'{dirname}_pir'].createOrReplaceTempView('df_pir')
        dataframes[f'{dirname}_temperature'].createOrReplaceTempView('df_temperature')

        # Below SQL joins on ts_min_bignt and creates the dataframe per room
        dataframes_room[dirname] = spark.sql('''
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
        ''')

        dataframes_room[dirname] = dataframes_room[dirname].withColumn("room", F.lit(dirname))
    
    return dataframes_room

def mergeDataframes(dfs):
    """
    Merges multiple dataframes vertically.
    """
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

def create_main_dataframe(separate_dataframes: dict):
    """
    Merges all per-room dataframes vertically. Creates final dataframe.
    """
    dataframes_to_concat = []
    for i in separate_dataframes.values():
        dataframes_to_concat.append(i)

    df = reduce(DataFrame.unionAll, dataframes_to_concat)
    df = df.sort(F.col("ts_min_bignt"))  # All data is sorted according to ts_min_bignt. We want it to stream according to timestamp.

    df = df.dropna()

    df_main = df.withColumn("event_ts_min", F.from_unixtime(F.col("ts_min_bignt")).cast(DateType()))
    df_main = df_main.withColumn("event_ts_min", F.date_format(F.col("event_ts_min"), "yyyy-MM-dd HH:mm:ss"))  # Create datetime column

    return df_main

def write_dataframe(df):
    """
    Writes the final dataframe to HDFS in Parquet format.
    """
    df.write.parquet(FINAL_OUTPUT_PATH, mode='overwrite')

if __name__ == "__main__":
    room_dataframes = create_separate_dataframes()
    main_df = create_main_dataframe(room_dataframes)
    write_dataframe(main_df)
    print("Successfully!!")
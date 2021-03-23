import argparse
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, DoubleType, TimestampType

parser = argparse.ArgumentParser(description='Put in your output bucket.')
parser.add_argument('--bucket', required=True)
args = parser.parse_args()

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """This function is to create a Spark session"""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Transform and load raw song and artists data from S3 into analytics tables on another S3
    
    This function reads in raw song and artists data in JSON format from S3; 
    Defines the schema of songs and artists analytics tables; 
    Processes the raw data into analtics tables; 
    Writes the tables into partitioned parquet files on new S3 bucket.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read song data in from
        output_data: an S3 bucket to write analytics tables to
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*", "*.json")

    # Define type of data
    songdata_schema = StructType([
    StructField("artist_id", StringType(), True),
    StructField("artist_latitude", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_longitude", DoubleType(), True),
    StructField("artist_name", StringType(), True), 
    StructField("duration", DoubleType(), True),
    StructField("num_songs", IntegerType(), True),
    StructField("song_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("year", IntegerType(), True)
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema = songdata_schema)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + "/songs_table.parquet",
        mode="overwrite",
        partitionBy=["year", "artist_id"]
    )

    # extract columns to create artists table
    artists_table = df.selectExpr(
    "artist_id",
    col("artist_name").alias("name"),
    col("artist_location").alias("location"),
    col("artist_latitude").alias("latitude"),
    col("artist_longitude").alias("longitude")).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + "/artists_table.parquet",
        mode="overwrite"
)


def process_log_data(spark, input_data, output_data):
    """
    Transform and load raw user log data from S3 into analytics tables on another S3
    
    This function reads in raw user logs data in JSON format from S3; 
    Defines the schema of songplays, users, and time analytics tables; 
    Processes the raw data into analtics tables; 
    Writes the tables into partitioned parquet files on new S3 bucket.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read song data in from
        output_data: an S3 bucket to write analytics tables to
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*", "*.json")
                
    logdata_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), False),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), False),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), False),
        StructField("location", StringType(), True),
        StructField("method", StringType(), False),
        StructField("page", StringType(), False),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("ts", DoubleType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    
    # read log data file
    df = spark.read.json(log_data, schema = logdata_schema)
    
    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr(
        "userId as user_id", 
        "firstName as first_name", 
        "lastName as last_name", 
        "gender", 
        "level"
    ).distinct()

    # write users table to parquet files
    users_table.write.parquet(
        output_data + "/users_table.parquet",
        mode="overwrite"
    )

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
    TimestampType())
    df = df.withColumn("start_time", get_datetime("ts"))

    # extract columns to create time table
    time_table = (
        df
        .withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("month", month("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", dayofweek("start_time"))
        .select("start_time", "hour", "day", "week", "month", "year", "weekday")
        .distinct()
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        output_data + "/time_table.parquet", 
        mode="overwrite", 
        partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")
    
    # read artists data for songsplay table 
    artists_table = spark.read.parquet(output_data + "artists_table.parquet")
    
    # extract columns from joined song and log datasets to create songplays table 
    song_artist = song_df.join(artists_table, "artist_id", "full")\
        .selectExpr("artist_id", "name as artist", "title as song", "duration as length", "song_id")
    
    songplays_table = df.join(
        song_artist,
        [
            df.song == song_artist.song,
            df.artist == song_artist.artist,
            df.length == song_artist.length
        ],
        "left"
    ).join(time_table, "start_time", "left").selectExpr(
            "start_time",
            "userId as user_id",
            "level",
            "song_id",
            "artist_id", 
            "sessionId as session_id",
            "location", 
            "userAgent as user_agent",
            "year",
            "month"
        ).withColumn("songplay_id", monotonically_increasing_id())
    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
    output_data + "/songplays_table.parquet",
    mode="overwrite", 
    partitionBy=["year", "month"])


def main(bucket = args.bucket):
    """
    This Function is to run the ETL pipeline
    
    """
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend"
    output_data = f"s3a://{bucket}"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
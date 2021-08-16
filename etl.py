import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """create a spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
            

def process_song_data(spark, input_song_data, output_data):
    """create songs table and artists table as parquet files from song data"""

    song_data = input_song_data
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data,"songs"), 'overwrite', ["year"])
    
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data,"artists"))

def process_log_data(spark, input_log_data, input_song_data, output_data):
    
    """create users, time and songplay table as parquet files from both song data and log data"""
    
    log_data = input_log_data
    song_data = input_song_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page == 'NextSong'")
    
    
    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, "users"))

    
    # get_timestamp = udf(lambda x: int(int(x)/1000))
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                          hour('datetime').alias('hour'),
                          dayofmonth('datetime').alias('day'),
                          weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year')
                          ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"time"), 'overwrite')
    
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, [df.song == song_df.title, df.artist == song_df.artist_name], 'left_outer').dropDuplicates() \
    .select( col('timestamp').alias('start_time'),
            df.userId.alias("userId"), 
            df.level.alias("level"),
            song_df.song_id.alias("song_id"),
            song_df.artist_id.alias("artist_id"),
            df.sessionId.alias("sessionId"),
            df.location.alias("location"),
            df.userAgent.alias("userAgent"),
            year(df.datetime).alias("year"),
            month(df.datetime).alias("month")
           ).withColumn("songplay_id", monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data,"songplays"),'overwrite', ["year", "month"])


def main():
    """ETL pipeline using PySpark will process the original data and create dimentional table as parquet files """
    
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"

    song_data = f'{input_data}song_data/*/*/*/*.json'
    log_data = f'{input_data}log_data/*.json'

    output_data = input_data + "output/"
    
    process_song_data(spark, song_data, output_data)    
    process_log_data(spark, log_data, song_data, output_data)


if __name__ == "__main__":
    main()

import configparser
from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.functions import to_timestamp, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

# Create logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s  [%(name)s] %(message)s')
logger = logging.getLogger('etl_spark')


def create_spark_session():
    
    """
    Create a spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
   def process_song_data(spark, input_data, output_data):
    """
    The S3 JSON files are read and processed with Spark using this function. These files are partitioned into data frames that represent the tables planned in the modeling. These tables are created in the output using the output_data as a parameter.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
        input_data (:obj:`str`): Directory that stores the input files.
        output_data (:obj:`str`): Directory where to save the output files.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    logger.info('loading song json files')
    df = spark.read.json(song_data)
    
    logger.info('Creating song_data_view from song data json')
    df.createOrReplaceTempView("song_data_view")

    # extract columns to create songs table
      logger.info('Creating the songs_table')
    songs_table = spark.sql("""
    select song_id, title, artist_id, case when year != 0 then year else null end as year, duration from song_data_view
    """)
    
    # write songs table to parquet files partitioned by year and artist
     logger.info('Creating songs table parquet')
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table.parquet")

    # extract columns to create artists table
       logger.info('Creating the artists_table')
    artists_table = spark.sql("""
    select artist_id, name, location, latitude, longitude from (
        select artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude,
            row_number() over (partition by artist_id order by year desc) as row_number
        from song_data_view
    ) as artist
    where row_number = 1
    """)
    
    # write artists table to parquet files
    logger.info('Creating artists table parquet')
    artists_table.write.parquet(output_data + "artists_table.parquet")


def process_log_data(spark, input_data, output_data):
        """
    The S3 JSON files are read and processed with Spark using this function. These files are partitioned into data frames that represent the tables planned in the modeling. These tables are created in the output using the output_data as a parameter.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
        input_data (:obj:`str`): Directory that stores the input files.
        output_data (:obj:`str`): Directory where to save the output files.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*/*.json"

    # read log data file
    logger.info('loading log json files')
    df = spark.read.json(log_data)
    
    logger.info('Creating log_data_view from log data json')
    df.createOrReplaceTempView("log_data_view")
    
    # filter by actions for song plays
    logger.info('read result from json file with filter by actions song plays')
    df = df.filter(df.page == 'NextSong')
    
    logger.info('Creating log_data_view from log json filtering by actions for song plays')
    df.createOrReplaceTempView("log_data_view")
    
    # extract columns for users table    
    logger.info('Creating the users_table')
    users_table = spark.sql("""
    select user_id, first_name, last_name, gender, level
    from (
        select userId as user_id, firstName as first_name, lastName as last_name, gender, level,
            row_number() over (partition by userId order by ts desc) as row_number
        from log_data_view
    ) as user
    where row_number = 1
    """)
    
    # write users table to parquet files
    logger.info('creating users_table parquet')
    users_table.write.parquet(output_data + 'users_table.parquet')
    
  

    # create timestamp column from original timestamp column    
    logger.info('Creating timestamp and songplay_id columns')
    df = df \
        .withColumn("start_time", to_timestamp(df.ts/1000)) \
        .withColumn('songplay_id', monotonically_increasing_id())
    
    logger.info('Recreating log_data_view with timestamp and songplay_id columns')
    df.createOrReplaceTempView("log_data_view")
    
    
    # extract columns to create time table
        logger.info('Creating the time_table')
    time_table = time_table = df.select('start_time') \
        .dropDuplicates() \
        .withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    logger.info('Creating time_table parquet')
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time_table.parquet')

    # read in time data to use for songplays table
    logger.info('Creating the event_time_view from the time table')
    time_table.createOrReplaceTempView("time_data_view")

    # extract columns from joined song and log datasets to create songplays table 
    logger.info('Creating the songplays_table')
    songplays_table = spark.sql("""
    select ldv.songplay_id, ldv.start_time, ldv.userId as user_id, ldv.level, sdv.song_id, sdv.artist_id, ldv.sessionId as session_id, ldv.location, ldv.userAgent, tdv.year,                tdv.month
    from log_data_view ldv
    inner join song_data_view sdv on ldv.song = sdv.title and ldv.artist = sdv.artist_name and ldv.length = sdv.duration
    inner time_data_view tdv on ldv.start_time = tdv.start_time
    """)

    # write songplays table to parquet files partitioned by year and month
    logger.info('Creating songplays_table parquet')
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://" + output_bucket + "/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

import os
import zipfile
import configparser
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from column_names import users, artists, songs

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session() -> SparkSession:
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """
    Function to read all song data stored in song-data/*/*/*/*.json directory and
    partitions them into parquet (columnar) format.
    The songs parquet is partitioned by year and artist_id.
    """
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"

    # read song data file
    raw_song_df = spark.read.json(song_data)
    print('Printing Raw Song Schema ~')
    raw_song_df.printSchema()
    raw_song_df.show(5, truncate=False)

    # extract columns to create songs table - PySpark DF
    print('Printing Song Schema ~')
    print(songs)
    songs_pyspark_df = raw_song_df.select(
        [col for col in songs.values()]
    )

    # drop duplicates
    songs_pyspark_df = songs_pyspark_df.dropDuplicates(['song_id'])
    print('Songs - PySpark DF')
    songs_pyspark_df.show()

    # write songs table to parquet files partitioned by year and artist
    songs_pyspark_df.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs')

    # extract columns to create artists table - PySpark DF
    print('Printing Artist Schema ~')
    print(artists)
    artists_pyspark_df = raw_song_df.select(
        [col for col in artists.values()]
    )

    # drop duplicates
    artists_pyspark_df = artists_pyspark_df.dropDuplicates(['artist_id'])
    print('Artists - PySpark DF')
    artists_pyspark_df.show()

    # write artists table to parquet files
    artists_pyspark_df.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    sqlContext = SQLContext(spark)
    """
    Function to read all song data stored in log-data directory and partitions
    them into parquet (columnar) format. Events are retrieved by filtering for 
    `NextSong` labels under `page` column.
    """
    # get filepath to log data file
    log_data = input_data + 'log-data'

    # read log data file
    raw_log_df = spark.read.json(log_data)
    print('Logging Raw Log Schema ~')
    raw_log_df.printSchema()
    raw_log_df.show(5, truncate=False)

    # filter by actions for song plays
    songplay_log_pyspark_df = raw_log_df.filter(F.col('page') == 'NextSong')
    songplay_log_pyspark_df.show()

    # extract columns for users table
    users_pyspark_df = raw_log_df.select(
        [col for col in users.values()]
    )

    # drop duplicates
    users_pyspark_df = users_pyspark_df.dropDuplicates(['user_id'])
    print('Logging users table - PySpark')
    users_pyspark_df.show(5, truncate=False)

    # write users table to parquet files
    users_pyspark_df.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    tsFormat = "yyyy-MM-dd HH:MM:ss z"
    # converting ts to timestamp format
    time_table = songplay_log_pyspark_df.withColumn(
        'timestamp', F.to_timestamp(
            F.date_format(
                (songplay_log_pyspark_df.ts/1000).cast(dataType=T.TimestampType()), tsFormat), tsFormat))
    print('Logging Songplay Log Schema ~ After creating timestamp column')
    time_table.printSchema()

    # extract columns to create time table
    time_table = time_table.select(
        F.col('timestamp').alias('start_time'),
        F.year(F.col('timestamp')).alias('year'),
        F.month(F.col('timestamp')).alias('month'),
        F.weekofyear(F.col('timestamp')).alias('weekofyear'),
        F.dayofmonth(F.col('timestamp')).alias('dayofmonth'),
        F.hour(F.col('timestamp')).alias('hour')
    )

    # drop duplicates
    time_table = time_table.dropDuplicates(['start_time'])
    print('Logging Songplay Log Schema ~ After creating time table')
    time_table.printSchema()
    time_table.show(5)

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time')

    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    song_pyspark_df = spark.read.json(song_data)
    print('Logging Song Log Schema ~')
    song_pyspark_df.printSchema()

    # read artists and songs parquet
    songs_pyspark_df = sqlContext.read.parquet('./songs')
    artists_pyspark_df = sqlContext.read.parquet('./artists')

    # add start_time column
    songplay_log_pyspark_df = songplay_log_pyspark_df.withColumn(
        "start_time",
        F.to_timestamp(F.date_format(
                (F.col('ts')/1000).cast(dataType=T.TimestampType()), tsFormat), tsFormat)
    )

    # create lazily evaluated view
    songplay_log_pyspark_df.createOrReplaceTempView("events_table")
    songs_pyspark_df.createOrReplaceTempView("songs_table")
    artists_pyspark_df.createOrReplaceTempView("artists_table")
    time_table.createOrReplaceTempView("time_table")

    # read in song data to use for songplays table
    songplays_table = spark.sql(
        """
        SELECT 
            e.start_time,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.userAgent,
            t.year,
            t.month
        FROM events_table e 
        JOIN songs_table s ON e.song = s.title AND e.length = s.duration 
        JOIN artists_table a ON e.artist = a.artist_name AND a.artist_id = s.artist_id
        JOIN time_table t ON e.start_time = t.start_time
        """
    )
    # conditions = [
    #     song_pyspark_df['artist_name'] == songplay_log_pyspark_df['artist'],
    #     song_pyspark_df['title'] == songplay_log_pyspark_df['song'],
    #     song_pyspark_df['duration'] == songplay_log_pyspark_df['length']
    #     ]
    # songplays_table = song_pyspark_df.join(
    #     songplay_log_pyspark_df,
    #     conditions
    # ).withColumn(
    #     'songplay_id', F.monotonically_increasing_id()
    # ).withColumn(
    #     'start_time', F.to_timestamp(F.date_format(
    #             (F.col('ts')/1000).cast(dataType=T.TimestampType()), tsFormat), tsFormat)
    # ).select(
    #     'songplay_id',
    #     'start_time',
    #     F.col('userId').alias('user_id'),
    #     'level',
    #     'song_id',
    #     'artist_id',
    #     F.col('sessionId').alias('session_id'),
    #     'location',
    #     F.col('userAgent').alias('user_agent'),
    #     F.year(F.col('start_time')).alias('year'),
    #     F.month(F.col('start_time')).alias('month'))
    # print('Logging Songplays Schema ~')
    # songplays_table.printSchema()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays')


def main() -> None:
    spark = create_spark_session()

    # Unzip song & log data
    zip_ref = zipfile.ZipFile('data/song-data.zip', 'r')
    zip_ref.extractall('data/song-data/')
    zip_ref = zipfile.ZipFile('data/log-data.zip', 'r')
    zip_ref.extractall('data/log-data/')
    zip_ref.close()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    # For local run
    # output_data = f'{os.getcwd()}/'
    # input_data_song = "data/song-data/song_data/*/*/*/*.json"
    # input_data_log = "data/log-data/"
    # process_song_data(spark, input_data_song, output_data)
    # process_log_data(spark, input_data_log, output_data)


if __name__ == "__main__":
    main()

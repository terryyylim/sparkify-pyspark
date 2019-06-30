import os
import zipfile
import configparser
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
    print('Songs - PySpark DF')
    songs_pyspark_df.show()

    # write songs table to parquet files partitioned by year and artist
    songs_pyspark_df.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs')

    # extract columns to create artists table - PySpark DF
    print('Printing Artist Schema ~')
    print(artists)
    artists_pyspark_df = raw_song_df.select(
        [col for col in artists.values()]
    )
    print('Artists - PySpark DF')
    artists_pyspark_df.show()

    # write artists table to parquet files
    artists_pyspark_df.write.parquet(output_data + 'artists')


def process_log_data(spark: SparkSession, input_data: str, output_data: str) -> None:
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
    print('Logging users table - PySpark')
    users_pyspark_df.show(5, truncate=False)

    # write users table to parquet files
    users_pyspark_df.write.parquet(input_data + 'users')

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
    print('Logging Songplay Log Schema ~ After creating time table')
    time_table.printSchema()
    time_table.show(5)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(input_data+'time')

    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    song_pyspark_df = spark.read.json(song_data)
    print('Logging Song Log Schema ~')
    song_pyspark_df.printSchema()

    # read in song data to use for songplays table
    songplays_table = song_pyspark_df.join(
        songplay_log_pyspark_df,
        song_pyspark_df['artist_name'] == songplay_log_pyspark_df['artist']
    ).withColumn(
        'songplay_id', F.monotonically_increasing_id()
    ).withColumn(
        'start_time', F.to_timestamp(F.date_format(
                (F.col('ts')/1000).cast(dataType=T.TimestampType()), tsFormat), tsFormat)
    ).select(
        'songplay_id',
        'start_time',
        F.col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        F.col('sessionId').alias('session_id'),
        'location',
        F.col('userAgent').alias('user_agent'),
        F.year(F.col('start_time')).alias('year'),
        F.month(F.col('start_time')).alias('month'))
    print('Logging Songplays Schema ~')
    songplays_table.printSchema()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(input_data+'songplays')


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

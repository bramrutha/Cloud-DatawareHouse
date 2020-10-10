import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

iam_role = config['IAM_ROLE']['ARN']
logs_data = config['S3']['LOG_DATA']
logs_jsonpath = config['S3']['LOG_JSONPATH']
songs_data = config['S3']['SONG_DATA']


#DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS songs;"
artist_table_drop         = "DROP TABLE IF EXISTS artists;"
time_table_drop           = "DROP TABLE IF EXISTS time;"


#CREATE TABLES

staging_events_table_create = """CREATE TABLE IF NOT EXISTS staging_events (
                                  artist VARCHAR,
                                  auth VARCHAR,
                                  firstName VARCHAR,
                                  gender CHAR(1),
                                  itemInSession INTEGER,
                                  lastName VARCHAR,
                                  length  FLOAT,
                                  level VARCHAR,
                                  location VARCHAR,
                                  method VARCHAR,
                                  page VARCHAR,
                                  registration VARCHAR,
                                  sessionId VARCHAR,  
                                  song VARCHAR,
                                  status INTEGER,
                                  ts BIGINT,
                                  userAgent TEXT,
                                  userId INTEGER);"""


staging_songs_table_create = """CREATE TABLE IF NOT EXISTS staging_songs (
                                  num_songs INTEGER,
                                  artist_id VARCHAR,
                                  artist_latitude DECIMAL(9,6),
                                  artist_longitude DECIMAL(9,6),
                                  artist_location VARCHAR,
                                  artist_name VARCHAR,
                                  song_id  VARCHAR,
                                  title VARCHAR,
                                  duration FLOAT,
                                  year INTEGER);"""

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays (
                                            songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
                                            start_time TIMESTAMP REFERENCES time (start_time),
                                            user_id INTEGER REFERENCES users (user_id),
                                            level VARCHAR NOT NULL,
                                            song_id VARCHAR REFERENCES songs (song_id),
                                            artist_id VARCHAR REFERENCES artists (artist_id),
                                            session_id  VARCHAR NOT NULL,
                                            location VARCHAR,
                                            user_agent TEXT)
                                            DISTSTYLE KEY
                                            DISTKEY (start_time)
                                            SORTKEY (start_time);
                                            """

user_table_create = """ CREATE TABLE IF NOT EXISTS users (
                                        user_id INTEGER PRIMARY KEY,
                                        first_name VARCHAR,
                                        last_name VARCHAR,
                                        gender CHAR(1),
                                        LEVEL VARCHAR NOT NULL)
                                        SORTKEY (user_id);"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs (
                                        song_id VARCHAR PRIMARY KEY,
                                        title VARCHAR,
                                        artist_id VARCHAR ,
                                        year INTEGER,
                                        duration FLOAT)
                                        SORTKEY (song_id);"""


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                        artist_id VARCHAR PRIMARY KEY,
                                        name VARCHAR,
                                        location VARCHAR,
                                        latitude DECIMAL(9,6),
                                        longitude DECIMAL(9,6))
                                        SORTKEY (artist_id);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                        start_time TIMESTAMP PRIMARY KEY,
                                        hour INTEGER ,
                                        day INTEGER ,
                                        week INTEGER ,
                                        month INTEGER,
                                        year INTEGER,
                                        weekday VARCHAR NOT NULL)
                                        DISTSTYLE KEY
                                        DISTKEY (start_time)
                                        SORTKEY (start_time);""")


#STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {} 
                          region 'us-west-2'
                          json {};""").format(logs_data,iam_role,logs_jsonpath)


staging_songs_copy = ("""copy staging_songs 
                          from {}
                          iam_role {} 
                          region 'us-west-2'
                          json 'auto';""").format(songs_data,iam_role)


#FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
                              SELECT DISTINCT 
                                     TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,  
                                     e.userId AS user_id,
                                     e.level AS level,
                                     s.song_id AS song_id,
                                     s.artist_id AS artist_id,
                                     e.sessionId AS session_id,
                                     e.location AS location,
                                     e.userAgent AS user_agent
                              FROM  staging_events e 
                              JOIN  staging_songs s ON e.song = s.title
                                                   AND e.artist = s.artist_name
                              WHERE e.page = 'NextSong';""")



user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name,gender,level)
                              SELECT DISTINCT  
                                     e.userId AS user_id,
                                     e.firstName as first_name,
                                     e.lastName as last_name,
                                     e.gender as gender,
                                     e.level AS level
                              FROM  staging_events e 
                              WHERE e.page = 'NextSong' and e.userId is NOT NULL;""")

song_table_insert = ("""INSERT INTO songs ( song_id,title,artist_id ,year, duration)
                              SELECT DISTINCT  
                                     s.song_id AS song_id,
                                     s.title as title,
                                     s.artist_id as artist_id,
                                     s.year as year,
                                     s.duration AS duration
                              FROM  staging_songs s
                              WHERE s.song_id IS NOT NULL;""")

artist_table_insert = ("""INSERT INTO artists ( artist_id,name,location,latitude,longitude)
                              SELECT DISTINCT  
                                     s.artist_id as artist_id,
                                     s.artist_name as name,
                                     s.artist_location as duration,
                                     s.artist_latitude as latitude,
                                     s.artist_longitude as longitude
                              FROM  staging_songs s
                              WHERE s.artist_id IS NOT NULL;""")

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                              SELECT DISTINCT 
                                     TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as start_time, 
                                     EXTRACT(HOUR FROM start_time) as hour,
                                     EXTRACT(DAY FROM start_time) as day,
                                     EXTRACT(WEEK FROM start_time) as week,
                                     EXTRACT(MONTH FROM start_time) as month,
                                     EXTRACT(YEAR FROM start_time) as year,
                                     to_char(start_time,'Day') as weekday
                              FROM  staging_events;""")


create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,  artist_table_create, song_table_create,time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


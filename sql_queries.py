import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREATE SCHEMAS

stage_schema = "CREATE SCHEMA IF NOT EXISTS stage;"
star_schema = "CREATE SCHEMA IF NOT EXISTS star;"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage.stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage.stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS star.fct_songplays;"
user_table_drop = "DROP TABLE IF EXISTS star.dim_users;"
song_table_drop = "DROP TABLE IF EXISTS star.dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS star.dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS star.dim_time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stage.stg_events(artist VARCHAR,
                                                                             auth VARCHAR,
                                                                             firstName VARCHAR,
                                                                             gender VARCHAR,
                                                                             itemInSession INT,
                                                                             lastName VARCHAR,
                                                                             length DECIMAL,
                                                                             level VARCHAR,
                                                                             location VARCHAR,
                                                                             method VARCHAR,
                                                                             page VARCHAR,
                                                                             registration VARCHAR,
                                                                             sessionId INT,
                                                                             song VARCHAR,
                                                                             status VARCHAR,
                                                                             ts BIGINT,
                                                                             userAgent VARCHAR,
                                                                             userId INT);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stage.stg_songs(num_songs INT,
                                                                            artist_id VARCHAR,
                                                                            artist_latitude DECIMAL,
                                                                            artist_longitude DECIMAL,
                                                                            artist_location VARCHAR,
                                                                            artist_name VARCHAR,
                                                                            song_id VARCHAR,
                                                                            title VARCHAR,
                                                                            duration DECIMAL,
                                                                            YEAR INT);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS star.fct_songplays(songplay_id INT IDENTITY(0,1) PRIMARY KEY DISTKEY,
                                                                          start_time TIMESTAMP NOT NULL SORTKEY,
                                                                          user_id INT NOT NULL,
                                                                          level VARCHAR,
                                                                          song_id VARCHAR,
                                                                          artist_id VARCHAR,
                                                                          session_id INT,
                                                                          location VARCHAR,
                                                                          user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS star.dim_users(user_id INT PRIMARY KEY SORTKEY,
                                                                  first_name VARCHAR,
                                                                  last_name VARCHAR,
                                                                  gender VARCHAR,
                                                                  level VARCHAR NOT NULL);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS star.dim_songs(song_id VARCHAR PRIMARY KEY SORTKEY,
                                                                  title VARCHAR,
                                                                  artist_id VARCHAR,
                                                                  year INT,
                                                                  duration DECIMAL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS star.dim_artists(artist_id VARCHAR PRIMARY KEY SORTKEY,
                                                                      name VARCHAR,
                                                                      location VARCHAR,
                                                                      latitude DECIMAL,
                                                                      longitude DECIMAL);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS star.dim_time(start_time TIMESTAMP PRIMARY KEY SORTKEY,
                                                                 hour INT NOT NULL,
                                                                 day INT NOT NULL,
                                                                 week INT NOT NULL,
                                                                 month INT NOT NULL,
                                                                 year INT NOT NULL,
                                                                 weekday INT NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
copy stage.stg_events from {}
credentials 'aws_iam_role={}'
json {} compupdate off region 'us-west-2';
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE', 'ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy stage.stg_songs from {}
credentials 'aws_iam_role={}'
json 'auto' compupdate off region 'us-west-2';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO star.fct_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second' AS start_date,
       e.userid,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionid,
       e.location,
       e.useragent
  FROM stage.stg_events e
LEFT JOIN stage.stg_songs  s ON (e.song = s.title AND e.artist = s.artist_name)
 WHERE page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO star.dim_users (user_id, first_name, last_name, gender, level)
SELECT userid,
       firstname,
       lastname,
       gender,
       level
  FROM (
        SELECT timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS start_date,
               userid,
               firstname,
               lastname,
               gender,
               level,
               RANK() OVER (PARTITION BY userid ORDER BY start_date DESC) AS rank
          FROM stage.stg_events
         WHERE page = 'NextSong'
       ) qry
WHERE qry.rank = 1;
""")

song_table_insert = ("""
INSERT INTO star.dim_songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
       song_id,
       title,
       artist_id,
       DECODE(year,0,null,year) AS year,
       duration
FROM stage.stg_songs
""")

artist_table_insert = ("""
INSERT INTO star.dim_artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
       artist_id,
       artist_name,
       DECODE(artist_location,'',null,artist_location) AS artist_location,
       artist_latitude,
       artist_longitude
FROM stage.stg_songs;
""")

time_table_insert = ("""
INSERT INTO star.dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
       timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS start_time,
       EXTRACT(hour FROM start_time) AS hour,
       EXTRACT(day FROM start_time) AS day,
       EXTRACT(week FROM start_time) AS week,
       EXTRACT(month FROM start_time) AS month,
       EXTRACT(year FROM start_time) AS year,
       EXTRACT(weekday FROM start_time) AS weekday
FROM stage.stg_events;
""")

# QUERY LISTS

create_schema_queries = [stage_schema, star_schema]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
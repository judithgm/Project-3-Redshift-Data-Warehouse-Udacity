import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
iteminSession integer,
lastName varchar,
length numeric,
level varchar,
location varchar,
method varchar,
page varchar,
registration numeric,
sessionid integer,
song varchar,
status int,
ts bigint,
userAgent varchar,
userId int
) 
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int,
artist_id varchar,
artist_latitude numeric,
artist_longitude numeric,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration numeric,
year int
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
songplay_id int IDENTITY(0,1) PRIMARY KEY ,
start_time  timestamp NOT NULL,
user_id int NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (
song_id varchar PRIMARY KEY,
title varchar,
artist_id varchar,
year int,
duration numeric
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude numeric,
longitude numeric
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY,
 hour int,
 day int,
 week int,
 month int,
 year int,
 weekday int
 )
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}' 
COMPUPDATE OFF STATUPDATE OFF 
JSON {}""".format(config['S3']['LOG_DATA'],
                   config['IAM_ROLE']['ARN'],
                   config['S3']['LOG_JSONPATH'] )
                      )
                       
staging_songs_copy = ("""COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF STATUPDATE OFF
JSON 'auto'""".format(config['S3']['SONG_DATA'],
                   config['IAM_ROLE']['ARN']
                     )
                     )
                       
# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level,
song_id, artist_id, session_id, location, user_agent)
(SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
e.userId,
e.level,
s.song_id,
s.artist_id,
e.sessionid,
e.location,
e.userAgent 
FROM  staging_events e
JOIN staging_songs s 
ON ((e.artist = s.artist_name) AND (e.song = s.title))
WHERE e.page = 'NextSong')""")



song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration)
(SELECT DISTINCT song_id,
title,
artist_id,
year,
duration 
FROM staging_songs)""")



user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender,level)
(SELECT DISTINCT userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
AND userId NOT IN (SELECT DISTINCT user_id FROM users))""")




artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude)
(SELECT DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs)""")



time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
(SELECT  
     DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
   extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(weekday from start_time) as weekday
FROM staging_events
WHERE page = 'NextSong')""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

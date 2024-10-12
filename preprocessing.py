import os
import glob
import sqlite3
import pandas as pd
from table_creation import create_database, create_tables
from commands import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.DataFrame([pd.read_json(filepath, typ='series', convert_dates=False)])

    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artist_data)

        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)

    print(f"Records inserted for file {filepath}")


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    df = df[df['page'] == "NextSong"].astype({'ts': 'datetime64[ms]'})
    t = pd.Series(df['ts'], index=df.index)
    
    # insert time data records
    column_labels = ["timestamp", "hour", "day", "week", "month", "year", "weekday"]
    time_data = []
    for data in t:
        time_data.append([
            data, 
            data.hour, 
            data.day, 
            data.weekofyear, 
            data.month, 
            data.year, 
            data.strftime('%A')  # Convert to string for the weekday
        ])

    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        row = list(row)
        row[0] = row[0].strftime('%Y-%m-%d %H:%M:%S')  # Convert datetime to string
        cur.execute(time_table_insert, row)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():     
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (
            row.ts.strftime('%Y-%m-%d %H:%M:%S'),  # Convert datetime to string
            row.userId, 
            row.level, 
            songid, 
            artistid, 
            row.sessionId, 
            row.location, 
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)



def process_data(cur, conn, filepath, func):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    cur, conn = create_database()
    
    # Create tables
    create_tables(cur, conn)

    # Process song data
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    # Process log data
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()



if __name__ == "__main__":
    main()
    print("\n\nFinished processing!!!\n\n")

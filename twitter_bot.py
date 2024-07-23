# twitter_bot.py

import tweepy
import mysql.connector
import os
import time
from datetime import datetime, timedelta, timezone

# Twitter API credentials
API_KEY = os.getenv('API_KEY')
API_SECRET_KEY = os.getenv('API_SECRET_KEY')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')
BEARER_TOKEN = os.getenv('BEARER_TOKEN')  # Add the bearer token for API v2

# Authenticate to Twitter using OAuth1 for posting tweets
auth = tweepy.OAuth1UserHandler(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api_v1 = tweepy.API(auth)

# Authenticate to Twitter using Bearer Token for API v2
client = tweepy.Client(bearer_token=BEARER_TOKEN)

# MySQL database connection
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_DATABASE'),
    'charset': 'latin1'  # Set charset to latin1
}

# Connect to the MySQL database
def get_db_connection():
    return mysql.connector.connect(**db_config)

# File to store the last fetched entry ID
LAST_ENTRY_FILE = 'last_entry_fetched_ID.txt'

def fetch_next_database_entry_to_tweet(cursor):
    last_entry_fetched = 0
    if os.path.exists(LAST_ENTRY_FILE):
        with open(LAST_ENTRY_FILE, 'r') as file:
            last_entry_fetched = int(file.read().strip())
    else:
        with open(LAST_ENTRY_FILE, 'w') as file:
            file.write('0')

    query = f"SELECT id, comment FROM comments WHERE id > {last_entry_fetched} ORDER BY id ASC LIMIT 1"
    cursor.execute(query)
    entry = cursor.fetchone()

    if entry:
        entry_id, entry_text = entry
        with open(LAST_ENTRY_FILE, 'w') as file:
            file.write(str(entry_id))
        return entry_text
    return None

def split_data_into_chunks(data, chunk_size=280):
    words = data.split()
    chunks = []
    chunk = ""
    for word in words:
        if len(chunk) + len(word) + 1 > chunk_size:
            chunks.append(chunk)
            chunk = word
        else:
            if chunk:
                chunk += " "
            chunk += word
    if chunk:
        chunks.append(chunk)
    return chunks

def calculate_tweets_made_in_last_24_hours():
    now = datetime.now(timezone.utc)
    since_time = now - timedelta(days=1)
    user_id = client.get_me().data.id

    tweets = client.get_users_tweets(id=user_id, max_results=100, start_time=since_time.isoformat())
    recent_tweets = tweets.data if tweets.data else []
    
    return len(recent_tweets)

def calculate_time_until_post_limit_reset():
    now = datetime.now(timezone.utc)
    since_time = now - timedelta(days=1)
    user_id = client.get_me().data.id

    tweets = client.get_users_tweets(id=user_id, max_results=100, start_time=since_time.isoformat())
    recent_tweets = tweets.data if tweets.data else []

    if recent_tweets:
        oldest_tweet_time = min(tweet.created_at for tweet in recent_tweets)
        reset_time = oldest_tweet_time + timedelta(days=1)
        return (reset_time - now).total_seconds()
    return 0

def tweet_chunks(chunks):
    tweet_count = calculate_tweets_made_in_last_24_hours()
    if tweet_count < 50:
        for chunk in chunks:
            api_v1.update_status(chunk)
            time.sleep(2)  # To avoid hitting the rate limit
    else:
        print("Reached tweet limit for the last 24 hours.")

def list_tables(cursor):
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print("Tables in the database:")
    for table in tables:
        print(table)

def list_columns(cursor, table_name):
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = cursor.fetchall()
    print(f"Columns in the {table_name} table:")
    for column in columns:
        print(column)

def main():
    db = get_db_connection()
    cursor = db.cursor()

    # List all tables for debugging purposes
    list_tables(cursor)

    # List all columns for debugging purposes
    list_columns(cursor, 'comments')

    data = fetch_next_database_entry_to_tweet(cursor)
    if data:
        chunks = split_data_into_chunks(data)
        tweet_chunks(chunks)
    else:
        print("No new entries to tweet.")
    cursor.close()
    db.close()

if __name__ == '__main__':
    main()

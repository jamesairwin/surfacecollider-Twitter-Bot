# twitter_bot.py

import tweepy
import mysql.connector
import os
import time
from datetime import datetime, timedelta

# Twitter API credentials
API_KEY = os.getenv('API_KEY')
API_SECRET_KEY = os.getenv('API_SECRET_KEY')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')

# Authenticate to Twitter
auth = tweepy.OAuth1UserHandler(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

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

    query = f"SELECT id, text FROM comments WHERE id > {last_entry_fetched} ORDER BY id ASC LIMIT 1"
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
    now = datetime.utcnow()
    since_time = now - timedelta(days=1)
    tweets = api.user_timeline(count=100)
    recent_tweets = [tweet for tweet in tweets if tweet.created_at > since_time]
    return len(recent_tweets)

def calculate_time_until_post_limit_reset():
    now = datetime.utcnow()
    since_time = now - timedelta(days=1)
    tweets = api.user_timeline(count=100)
    recent_tweets = [tweet for tweet in tweets if tweet.created_at > since_time]
    if recent_tweets:
        oldest_tweet_time = min(tweet.created_at for tweet in recent_tweets)
        reset_time = oldest_tweet_time + timedelta(days=1)
        return (reset_time - now).total_seconds()
    return 0

def tweet_chunks(chunks):
    tweet_count = calculate_tweets_made_in_last_24_hours()
    if tweet_count < 50:
        for chunk in chunks:
            api.update_status(chunk)
            time.sleep(2)  # To avoid hitting the rate limit
    else:
        print("Reached tweet limit for the last 24 hours.")

def main():
    db = get_db_connection()
    cursor = db.cursor()
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

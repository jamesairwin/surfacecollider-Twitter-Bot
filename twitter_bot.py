import time
import os
import mysql.connector
import tweepy
import unicodedata
import re
from tweepy.errors import TweepyException
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()  

# Fetch credentials from environment variables
API_KEY = os.getenv('API_KEY')
API_SECRET_KEY = os.getenv('API_SECRET_KEY')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')

# MySQL database connection
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_DATABASE')
}

# Connect to the MySQL database
def get_db_connection():
    return mysql.connector.connect(**db_config)

# Fetch the latest entry from the database
def fetch_latest_entry(cursor):
    query = "SELECT * FROM comments ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    return cursor.fetchone()

# Fetch all new entries from the database since the last processed ID
def fetch_new_entries(cursor, last_entry_id):
    query = "SELECT * FROM comments WHERE id > %s ORDER BY id ASC"
    cursor.execute(query, (last_entry_id,))
    return cursor.fetchall()

# Clean the text to convert to regular characters, numbers or punctuation
def clean_text(text):
    normalized_text = unicodedata.normalize('NFKD', text)
    ascii_text = normalized_text.encode('ascii', 'ignore').decode('ascii')
    cleaned_text = re.sub(r'[^a-zA-Z0-9\s\.,!?\'\"-]', '', ascii_text)
    return cleaned_text

# Split the text into chunks of 140 characters, at natural whitespace intervals
def split_text_into_chunks(text, chunk_size=140):
    words = text.split()
    chunks = []
    chunk = words.pop(0)

    for word in words:
        if len(chunk) + len(word) + 1 > chunk_size:
            chunks.append(chunk)
            chunk = word
        else:
            chunk += ' ' + word

    chunks.append(chunk)
    return chunks

# Post a tweet
def post_tweet(client, chunk):
    while True:
        try:
            client.create_tweet(text=chunk)
            print(f"Posted tweet: {chunk[:30]}...")  # Print the beginning of the tweet for confirmation
            return True
        except TweepyException as e:
            if '429' in str(e):
                print("Rate limit exceeded. Waiting for 15 minutes...")
                time.sleep(15 * 60)  # Wait for 15 minutes
            else:
                print(f"An error occurred: {e}")
                return False

# Main function to run the bot
def run_bot():
    tweet_limit = 50
    tweet_count = 0

    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        latest_entry = fetch_latest_entry(cursor)
        last_entry_id = latest_entry['id'] if latest_entry else None
        cursor.close()
        db_conn.close()
    except Exception as e:
        print(f"Error initializing the bot: {e}")
        return

    client = tweepy.Client(
        bearer_token=API_KEY, 
        consumer_key=API_KEY, 
        consumer_secret=API_SECRET_KEY, 
        access_token=ACCESS_TOKEN, 
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    last_tweet_time = datetime.now()  # Initial last tweet time
    reset_interval = timedelta(days=1)  # 24-hour reset interval

    while True:
        try:
            db_conn = get_db_connection()
            cursor = db_conn.cursor(dictionary=True)
            new_entries = fetch_new_entries(cursor, last_entry_id)
            
            for entry in new_entries:
                cleaned_comment = clean_text(entry['comment'])
                tweet_content = f"New entry added: {cleaned_comment}"
                chunks = split_text_into_chunks(tweet_content)

                for chunk in chunks:
                    now = datetime.now()
                    elapsed_since_last_tweet = now - last_tweet_time
                    remaining_time = reset_interval - elapsed_since_last_tweet

                    if tweet_count >= tweet_limit:
                        if remaining_time > timedelta(0):
                            print(f"Tweet limit reached. Waiting for {remaining_time.total_seconds() / 60:.2f} minutes.")
                            time.sleep(remaining_time.total_seconds())
                        
                        tweet_count = 0
                        last_tweet_time = datetime.now()
                        print(f"Tweet limit reset. New last tweet time: {last_tweet_time}")

                    if post_tweet(client, chunk):
                        tweet_count += 1
                        print(f"Tweet count: {tweet_count}")  # Print tweet count
                        last_tweet_time = datetime.now()  # Update last tweet time
                        time.sleep(1)  # To avoid hitting rate limits

                last_entry_id = entry['id']
            
            cursor.close()
            db_conn.close()
        
        except Exception as e:
            print(f"An error occurred: {e}")

        time.sleep(300)  # Check for new entries every 5 minutes

if __name__ == "__main__":
    run_bot()

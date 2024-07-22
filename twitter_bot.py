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

def get_db_connection():
    return mysql.connector.connect(**db_config)

def fetch_latest_entry(cursor):
    query = "SELECT * FROM comments ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    return cursor.fetchone()

def fetch_new_entries(cursor, last_entry_id):
    query = "SELECT * FROM comments WHERE id > %s ORDER BY id ASC"
    cursor.execute(query, (last_entry_id,))
    return cursor.fetchall()

def clean_text(text):
    normalized_text = unicodedata.normalize('NFKD', text)
    ascii_text = normalized_text.encode('ascii', 'ignore').decode('ascii')
    cleaned_text = re.sub(r'[^a-zA-Z0-9\s\.,!?\'\"-]', '', ascii_text)
    return cleaned_text

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

def post_tweet(client, chunk):
    try:
        client.create_tweet(text=chunk)
        print(f"Posted tweet: {chunk[:30]}...")  # Print the beginning of the tweet for confirmation
        return True
    except TweepyException as e:
        if '429' in str(e):
            print("Rate limit exceeded. Sleeping for 15 minutes.")
            time.sleep(15 * 60)  # Sleep for 15 minutes before retrying
            return post_tweet(client, chunk)  # Retry posting the tweet
        else:
            print(f"An error occurred: {e}")
            return False

def read_last_entry_id(file_path):
    try:
        with open(file_path, 'r') as f:
            content = f.read().strip()
            return int(content)
    except ValueError:
        print("File content is invalid; defaulting to ID 0.")
        return 0
    except FileNotFoundError:
        print("File not found; defaulting to ID 0.")
        return 0

def write_last_entry_id(file_path, last_entry_id):
    with open(file_path, 'w') as f:
        f.write(str(last_entry_id))

def run_bot():
    last_entry_id_file = 'last_entry_id.txt'
    tweet_limit = 50
    tweet_count = 0

    print(f"Starting bot. Tweet limit is {tweet_limit} per 24 hours.")

    # Read the last processed entry ID
    last_entry_id = read_last_entry_id(last_entry_id_file)
    print(f"Last processed entry ID: {last_entry_id}")

    try:
        # Connect to the database and fetch new entries
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        new_entries = fetch_new_entries(cursor, last_entry_id)

        if not new_entries:
            print("No new entries found.")
            return

        client = tweepy.Client(
            bearer_token=API_KEY, 
            consumer_key=API_KEY, 
            consumer_secret=API_SECRET_KEY, 
            access_token=ACCESS_TOKEN, 
            access_token_secret=ACCESS_TOKEN_SECRET
        )

        for entry in new_entries:
            cleaned_comment = clean_text(entry['comment'])
            tweet_content = f"New entry added: {cleaned_comment}"
            chunks = split_text_into_chunks(tweet_content)

            for chunk in chunks:
                if tweet_count >= tweet_limit:
                    print(f"Tweet limit reached. Exiting script.")
                    return  # Exit the script

                success = post_tweet(client, chunk)
                if not success:
                    print("Failed to post tweet. Stopping script.")
                    return  # Exit the script

                tweet_count += 1
                print(f"Tweet count: {tweet_count}")  # Print tweet count
                time.sleep(1)  # To avoid hitting rate limits

            last_entry_id = entry['id']
            write_last_entry_id(last_entry_id_file, last_entry_id)

        cursor.close()
        db_conn.close()
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_bot()

import time
import os
import mysql.connector
import tweepy
import unicodedata
import re
from tweepy.errors import TweepyException
from datetime import datetime
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
    try:
        print("Connecting to the database...")
        return mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        raise

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

def post_tweet(client, chunk, retry_count=0):
    try:
        client.create_tweet(text=chunk)
        print(f"Posted tweet: {chunk[:30]}...")  # Print the beginning of the tweet for confirmation
        return True
    except TweepyException as e:
        if '429' in str(e):
            if retry_count < 5:  # Limit the number of retries
                reset_time = int(e.response.headers.get('x-rate-limit-reset', time.time() + 15 * 60))
                wait_time = max(reset_time - time.time(), 15 * 60)
                print(f"Rate limit exceeded. Waiting for {wait_time / 60:.2f} minutes before retrying.")
                time.sleep(wait_time)
                return post_tweet(client, chunk, retry_count + 1)
            else:
                print("Rate limit exceeded too many times. Stopping the script.")
                return False
        else:
            print(f"An error occurred: {e}")
            return False

def read_last_entry_id(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            content = file.read().strip()
            if content:
                return int(content)
            else:
                print("The file is empty.")
                return None
    else:
        print("File does not exist.")
        return None

def write_last_entry_id(file_path, last_entry_id):
    try:
        with open(file_path, 'w') as file:
            file.write(str(last_entry_id))
        print(f"Updated last processed entry ID to: {last_entry_id}")
    except IOError as e:
        print(f"Error writing to file: {e}")

def run_bot():
    tweet_limit = 50
    tweet_count = 0
    last_entry_id_file = "last_entry_id.txt"

    # Initialize the Tweepy client
    client = tweepy.Client(
        bearer_token=API_KEY, 
        consumer_key=API_KEY, 
        consumer_secret=API_SECRET_KEY, 
        access_token=ACCESS_TOKEN, 
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    # Read the last processed entry ID from file
    last_entry_id = read_last_entry_id(last_entry_id_file)
    print(f"Starting bot. Last processed entry ID: {last_entry_id}")

    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        new_entries = fetch_new_entries(cursor, last_entry_id)
        
        if not new_entries:
            print("No new entries found.")
        
        for entry in new_entries:
            cleaned_comment = clean_text(entry['comment'])
            tweet_content = f"New entry added: {cleaned_comment}"
            chunks = split_text_into_chunks(tweet_content)

            for chunk in chunks:
                if tweet_count >= tweet_limit:
                    print("Tweet limit reached. Stopping the script.")
                    return

                if post_tweet(client, chunk):
                    tweet_count += 1
                    print(f"Tweet count: {tweet_count}")  # Print tweet count
                    time.sleep(1)  # To avoid hitting rate limits

            last_entry_id = entry['id']
            write_last_entry_id(last_entry_id_file, last_entry_id)
        
        cursor.close()
        db_conn.close()
    
    except Exception as e:
        print(f"An error occurred: {e}")
        if 'db_conn' in locals():
            db_conn.close()

if __name__ == "__main__":
    run_bot()

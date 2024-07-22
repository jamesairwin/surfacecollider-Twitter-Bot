import time
import os
import mysql.connector
import tweepy
import unicodedata
import re
from tweepy.errors import TweepyException
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
from html import unescape

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

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

# File to store the last processed entry ID
LAST_ENTRY_ID_FILE = 'last_entry_id.txt'

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
    # Decode from Latin-1 to Unicode
    if isinstance(text, bytes):
        text = text.decode('latin1')
    
    # Decode HTML entities
    text = unescape(text)
    
    # Normalize to NFKC form
    normalized_text = unicodedata.normalize('NFKC', text)
    
    # Replace fancy quotes and apostrophes with standard ones
    cleaned_text = re.sub(r'[“”]', '"', normalized_text)  # Replace fancy quotes
    cleaned_text = re.sub(r"[‘’]", "'", cleaned_text)  # Replace fancy apostrophes
    
    # Optionally remove non-ASCII characters if required
    # cleaned_text = re.sub(r'[^\x00-\x7F]', '', cleaned_text)
    
    # Replace multiple spaces with a single space and strip leading/trailing spaces
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    
    # Debug logging
    logging.debug(f"Cleaned text: {cleaned_text}")
    
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
    try:
        response = client.create_tweet(text=chunk)
        logging.info(f"Posted tweet: {chunk[:30]}...")  # Log the beginning of the tweet for confirmation
        return True
    except TweepyException as e:
        # Check if the error is due to rate limit
        if '429' in str(e):
            logging.warning("Rate limit exceeded. Stopping the script.")
            return False
        else:
            logging.error(f"An error occurred: {e}")
            return False

# Load the last processed entry ID from the file
def load_last_entry_id():
    if os.path.exists(LAST_ENTRY_ID_FILE):
        with open(LAST_ENTRY_ID_FILE, 'r') as file:
            return int(file.read().strip())
    return None

# Save the last processed entry ID to the file
def save_last_entry_id(entry_id):
    with open(LAST_ENTRY_ID_FILE, 'w') as file:
        file.write(str(entry_id))

# Main function to run the bot
def run_bot():
    tweet_limit = 50
    tweet_count = 0
    reset_time = datetime.now() + timedelta(days=1)  # Reset in 24 hours

    logging.info(f"Starting bot. Tweet limit is {tweet_limit} per 24 hours.")
    logging.info(f"Current reset time: {reset_time}")

    client = None

    try:
        # Initialize the Tweepy client
        client = tweepy.Client(
            bearer_token=API_KEY,
            consumer_key=API_KEY,
            consumer_secret=API_SECRET_KEY,
            access_token=ACCESS_TOKEN,
            access_token_secret=ACCESS_TOKEN_SECRET
        )

        # Connect to the database and get the latest entry
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        
        # Load the last processed entry ID
        last_entry_id = load_last_entry_id()
        logging.debug(f"Loaded last entry ID: {last_entry_id}")
        
        # Fetch the latest entry from the database
        latest_entry = fetch_latest_entry(cursor)
        logging.debug(f"Fetched latest entry: {latest_entry}")

        # If no entries in the database or the latest entry is the same as the last processed one, exit
        if not latest_entry or (last_entry_id and latest_entry['id'] == last_entry_id):
            logging.info("No new entries to process.")
            cursor.close()
            db_conn.close()
            return

        # Clean the comment and debug the cleaned text
        cleaned_comment = clean_text(latest_entry['comment'])
        logging.debug(f"Cleaned comment: {cleaned_comment}")

        tweet_content = f"New entry added: {cleaned_comment}"
        chunks = split_text_into_chunks(tweet_content)

        for chunk in chunks:
            if tweet_count >= tweet_limit:
                now = datetime.now()
                if now >= reset_time:
                    tweet_count = 0
                    reset_time = now + timedelta(days=1)
                    logging.info(f"Tweet limit reset. New reset time: {reset_time}")
                else:
                    wait_time = (reset_time - now).total_seconds()
                    logging.info(f"Tweet limit reached. Waiting for {wait_time / 60:.2f} minutes.")
                    time.sleep(wait_time)

            if not post_tweet(client, chunk):
                logging.info("Stopping the bot due to rate limit.")
                break

            tweet_count += 1
            logging.info(f"Tweet count: {tweet_count}")  # Log tweet count
            time.sleep(1)  # To avoid hitting rate limits

        # Update the last processed entry ID
        save_last_entry_id(latest_entry['id'])
        logging.info(f"Updated last entry ID to {latest_entry['id']}")

        cursor.close()
        db_conn.close()

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    run_bot()

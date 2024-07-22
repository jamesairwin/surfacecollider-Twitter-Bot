import unicodedata
import re
import mysql.connector
import tweepy
import os
import logging
import time
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
    'database': os.getenv('DB_DATABASE'),
    'charset': 'latin1'  # Ensure this is set to latin1
}

# Connect to the MySQL database
def get_db_connection():
    return mysql.connector.connect(**db_config)

# Fetch the latest entry from the database
def fetch_latest_entry(cursor):
    query = "SELECT * FROM comments ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    logging.debug(f"Raw data from database: {result}")
    return result

# Convert Latin-1 bytes to UTF-8 string
def convert_latin1_to_unicode(text):
    if isinstance(text, bytes):
        return text.decode('latin1')
    return text

# Convert Unicode text to ASCII, ignoring non-ASCII characters
def convert_unicode_to_ascii(text):
    # Normalize Unicode text to NFC form and encode to ASCII
    normalized_text = unicodedata.normalize('NFC', text)
    ascii_text = normalized_text.encode('ascii', 'ignore').decode('ascii')
    
    # Replace multiple spaces with a single space and strip leading/trailing spaces
    cleaned_text = re.sub(r'\s+', ' ', ascii_text).strip()
    
    return cleaned_text

# Clean and process the text
def clean_text(text):
    # Convert from Latin-1 to Unicode
    text = convert_latin1_to_unicode(text)
    
    # Convert from Unicode to ASCII
    ascii_text = convert_unicode_to_ascii(text)
    
    # Debug logging
    logging.debug(f"Cleaned text: {ascii_text}")
    
    return ascii_text

# Post a tweet
def post_tweet(client, chunk):
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            client.create_tweet(text=chunk)
            logging.info(f"Posted tweet: {chunk[:30]}...")
            return True
        except TweepyException as e:
            if '429' in str(e):
                logging.warning("Rate limit exceeded. Waiting for 15 minutes...")
                time.sleep(15 * 60)  # Wait for 15 minutes
                retries += 1
            else:
                logging.error(f"An error occurred: {e}")
                return False
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return False

    logging.error("Max retries reached. Unable to post tweet.")
    return False

# Main function to run the bot
def run_bot():
    tweet_limit = 50
    tweet_count = 0
    reset_time = datetime.now() + timedelta(days=1)  # Reset in 24 hours

    logging.info(f"Starting bot. Tweet limit is {tweet_limit} per 24 hours.")
    logging.info(f"Current reset time: {reset_time}")

    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        latest_entry = fetch_latest_entry(cursor)
        last_entry_id = latest_entry['id'] if latest_entry else None
        cursor.close()
        db_conn.close()
    except Exception as e:
        logging.error(f"Error initializing the bot: {e}")
        return

    client = tweepy.Client(
        bearer_token=API_KEY, 
        consumer_key=API_KEY, 
        consumer_secret=API_SECRET_KEY, 
        access_token=ACCESS_TOKEN, 
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        new_entries = fetch_new_entries(cursor, last_entry_id)
        
        for entry in new_entries:
            cleaned_comment = clean_text(entry['comment'])
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

                if post_tweet(client, chunk):
                    tweet_count += 1
                    logging.info(f"Tweet count: {tweet_count}")  # Print tweet count
                    time.sleep(1)  # To avoid hitting rate limits

            last_entry_id = entry['id']
        
        cursor.close()
        db_conn.close()
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    run_bot()

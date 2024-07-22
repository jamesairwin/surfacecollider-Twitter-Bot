import mysql.connector
import tweepy
import os
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.DEBUG)

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
    'charset': 'latin1'  # Set charset to latin1
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

# Fetch all new entries from the database since the last processed ID
def fetch_new_entries(cursor, last_entry_id):
    query = "SELECT * FROM comments WHERE id > %s ORDER BY id ASC"
    cursor.execute(query, (last_entry_id,))
    return cursor.fetchall()

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
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            client.create_tweet(text=chunk)
            logging.info(f"Posted tweet: {chunk[:30]}...")
            return True
        except tweepy.errors.TweepyException as e:
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
        last_entry_id = latest_entry['id'] if latest_entry else 0
        logging.debug(f"Last entry ID: {last_entry_id}")
        cursor.close()
        db_conn.close()
    except Exception as e:
        logging.error(f"Error initializing the bot: {e}")
        return

    client = tweepy.Client(
        consumer_key=API_KEY, 
        consumer_secret=API_SECRET_KEY, 
        access_token=ACCESS_TOKEN, 
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        new_entries = fetch_new_entries(cursor, last_entry_id)
        logging.debug(f"New entries fetched: {new_entries}")
        
        for entry in new_entries:
            tweet_content = f"New entry added: {entry['comment']}"
            logging.debug(f"Tweet content: {tweet_content}")
            chunks = split_text_into_chunks(tweet_content)
            logging.debug(f"Tweet chunks: {chunks}")

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

                logging.debug(f"Attempting to post tweet: {chunk[:30]}...")
                if post_tweet(client, chunk):
                    tweet_count += 1
                    logging.info(f"Tweet count: {tweet_count}")
                    logging.debug(f"Tweet posted successfully: {chunk[:30]}...")
                    time.sleep(1)  # To avoid hitting rate limits
                else:
                    logging.debug(f"Tweet failed: {chunk[:30]}...")

            last_entry_id = entry['id']
        
        cursor.close()
        db_conn.close()
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    run_bot()

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

def get_db_connection():
    return mysql.connector.connect(**db_config)

def fetch_last_tweeted_id(cursor):
    cursor.execute("SELECT last_tweeted_id FROM tweet_tracker ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    return result['last_tweeted_id'] if result else 0

def update_last_tweeted_id(cursor, last_tweeted_id):
    cursor.execute(
        "INSERT INTO tweet_tracker (last_tweeted_id) VALUES (%s) ON DUPLICATE KEY UPDATE last_tweeted_id = VALUES(last_tweeted_id)",
        (last_tweeted_id,)
    )

def fetch_new_entries(cursor, last_tweeted_id):
    query = f"SELECT id, comment FROM comments WHERE id > {last_tweeted_id} ORDER BY id ASC"
    cursor.execute(query)
    return cursor.fetchall()

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
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            client.create_tweet(text=chunk)
            logging.info(f"Posted tweet: {chunk[:30]}...")
            return True
        except tweepy.errors.TweepyException as e:
            if '429' in str(e):
                logging.warning("Rate limit exceeded. Exiting...")
                exit(1)  # Exit the script immediately
            else:
                logging.error(f"An error occurred: {e}")
                return False
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return False

    logging.error("Max retries reached. Unable to post tweet.")
    return False

def run_bot():
    tweet_limit = 50
    tweet_count = 0
    reset_time = datetime.now() + timedelta(days=1)  # Reset in 24 hours

    logging.info(f"Starting bot. Tweet limit is {tweet_limit} per 24 hours.")
    logging.info(f"Current reset time: {reset_time}")

    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        last_tweeted_id = fetch_last_tweeted_id(cursor)
        entries = fetch_new_entries(cursor, last_tweeted_id)
    except Exception as e:
        logging.error(f"Error initializing the bot: {e}")
        return

    if not entries:
        logging.info("No new entries found in the database.")
        return

    client = tweepy.Client(
        consumer_key=API_KEY, 
        consumer_secret=API_SECRET_KEY, 
        access_token=ACCESS_TOKEN, 
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    try:
        for entry in entries:
            entry_id, comment = entry
            tweet_content = f"New entry added: {comment}"
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

            # Update the last_tweeted_id in the database
            update_last_tweeted_id(cursor, entry_id)
            db_conn.commit()
            logging.debug(f"Updated last_tweeted_id to: {entry_id}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        cursor.close()
        db_conn.close()

if __name__ == "__main__":
    run_bot()

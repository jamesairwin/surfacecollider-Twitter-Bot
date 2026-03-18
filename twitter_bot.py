import mysql.connector
import tweepy
import os
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# Load environment variables
load_dotenv()

# Twitter credentials
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
    'charset': 'latin1'
}

def get_db_connection():
    return mysql.connector.connect(**db_config)

def fetch_last_tweeted_id(cursor):
    cursor.execute("SELECT last_tweeted_id FROM tweet_tracker ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    return int(result['last_tweeted_id']) if result and result['last_tweeted_id'] is not None else 0

def update_last_tweeted_id(cursor, last_tweeted_id):
    cursor.execute(
        "INSERT INTO tweet_tracker (last_tweeted_id) VALUES (%s) "
        "ON DUPLICATE KEY UPDATE last_tweeted_id = VALUES(last_tweeted_id)",
        (last_tweeted_id,)
    )

def fetch_new_entries(cursor, last_tweeted_id):
    query = f"SELECT id, comment FROM comments WHERE id > {last_tweeted_id} ORDER BY id ASC"
    cursor.execute(query)
    return cursor.fetchall()

def split_text_into_chunks(text, chunk_size=280):
    if not text:
        return []
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
    """
    Returns:
      True  -> Tweet posted successfully
      False -> Tweet failed (non-503/530 error, e.g., rate limit)
      None  -> Tweet failed due to 503/530 (do not update last_tweeted_id)
    """
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            response = client.create_tweet(text=chunk)
            logging.info(f"✅ Tweet posted: {chunk[:50]}...")
            logging.debug(f"Full response: {response}")
            return True

        except tweepy.errors.TweepyException as e:
            retries += 1
            status_code = getattr(e, 'response', None)
            code = status_code.status_code if status_code else None

            logging.error(f"❌ Tweet failed (TweepyException) Attempt {retries}/{max_retries}")
            logging.error(f"Error: {e}")
            if code:
                logging.error(f"Status code: {code}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response text: {e.response.text}")

            # Handle rate limit
            if '429' in str(e):
                logging.warning("Rate limit hit. Exiting this run.")
                return False

            # Handle server errors (503/530)
            if code in [503, 530]:
                logging.warning(f"Server error {code}. Will NOT update last_tweeted_id for this tweet.")
                time.sleep(30)  # Wait a bit before retrying
                return None

            time.sleep(5)

        except Exception as e:
            retries += 1
            logging.error(f"❌ Unexpected error Attempt {retries}/{max_retries}: {e}")
            time.sleep(5)

    logging.error("❌ Max retries reached. Skipping this tweet.")
    return False

def run_bot():
    tweet_limit = 10
    tweet_count = 0
    starting_id = 6310

    logging.info(f"Starting bot. Tweet limit: {tweet_limit} per run.")

    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)

        last_tweeted_id = fetch_last_tweeted_id(cursor)
        logging.info(f"Last tweeted ID from tracker: {last_tweeted_id}")

        if last_tweeted_id < starting_id - 1:
            logging.info(f"Overriding last_tweeted_id to {starting_id - 1} for catch-up")
            last_tweeted_id = starting_id - 1

        entries = fetch_new_entries(cursor, last_tweeted_id)
        logging.info(f"Fetched {len(entries)} new entries from DB")

    except Exception as e:
        logging.error(f"Error initializing bot: {e}")
        return

    if not entries:
        logging.info("No new entries to tweet.")
        cursor.close()
        db_conn.close()
        return

    client = tweepy.Client(
        consumer_key=API_KEY,
        consumer_secret=API_SECRET_KEY,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    try:
        for entry in entries:
            entry_id = entry['id']
            comment = entry['comment']
            logging.info(f"Processing DB entry ID: {entry_id}")
            chunks = split_text_into_chunks(comment)

            all_chunks_posted = True  # Track if all chunks succeed

            for chunk in chunks:
                if tweet_count >= tweet_limit:
                    logging.info(f"Tweet limit reached ({tweet_limit}). Ending this run.")
                    cursor.close()
                    db_conn.close()
                    return

                logging.info(f"Attempting to tweet: {chunk[:50]}...")
                result = post_tweet(client, chunk)

                if result is True:
                    tweet_count += 1
                    time.sleep(20)
                elif result is None:
                    logging.warning("Server error 503/530 encountered. Skipping update of last_tweeted_id.")
                    all_chunks_posted = False
                    break
                else:
                    logging.warning(f"Failed to tweet chunk: {chunk[:50]}...")
                    all_chunks_posted = False

            # Only update last_tweeted_id if all chunks were successfully posted
            if all_chunks_posted:
                update_last_tweeted_id(cursor, entry_id)
                db_conn.commit()
                logging.info(f"Updated last_tweeted_id to: {entry_id}")
            else:
                logging.info(f"Did NOT update last_tweeted_id for entry ID {entry_id} due to tweet failures.")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")

    finally:
        cursor.close()
        db_conn.close()
        logging.info("Bot run completed.")

if __name__ == "__main__":
    run_bot()

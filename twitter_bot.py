import time
import os
import mysql.connector
import tweepy
import unicodedata
import re
from tweepy.errors import TweepyException
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

LAST_ENTRY_ID_FILE = "last_entry_id.txt"

# Read the last processed entry ID from the file
def read_last_entry_id():
    if os.path.exists(LAST_ENTRY_ID_FILE):
        with open(LAST_ENTRY_ID_FILE, 'r') as file:
            return int(file.read().strip())
    return None

# Write the last processed entry ID to the file
def write_last_entry_id(last_entry_id):
    with open(LAST_ENTRY_ID_FILE, 'w') as file:
        file.write(str(last_entry_id))

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
    try:
        client.create_tweet(text=chunk)
        print(f"Posted tweet: {chunk[:30]}...")  # Print the beginning of the tweet for confirmation
        return True
    except TweepyException as e:
        if '429' in str(e):
            print("Rate limit exceeded. Stopping further tweets.")
        else:
            print(f"An error occurred: {e}")
        return False

# Main function to run the bot
def run_bot():
    tweet_limit = 50
    tweet_count = 0

    print(f"Starting bot. Tweet limit is {tweet_limit} per 24 hours.")

    try:
        last_entry_id = read_last_entry_id()
        if last_entry_id is None:
            db_conn = get_db_connection()
            cursor = db_conn.cursor(dictionary=True)
            latest_entry = fetch_latest_entry(cursor)
            last_entry_id = latest_entry['id'] if latest_entry else 0
            cursor.close()
            db_conn.close()

        client = tweepy.Client(
            bearer_token=API_KEY, 
            consumer_key=API_KEY, 
            consumer_secret=API_SECRET_KEY, 
            access_token=ACCESS_TOKEN, 
            access_token_secret=ACCESS_TOKEN_SECRET
        )
    except Exception as e:
        print(f"Error initializing the bot: {e}")
        return

    while True:
        try:
            db_conn = get_db_connection()
            cursor = db_conn.cursor(dictionary=True)
            new_entries = fetch_new_entries(cursor, last_entry_id)

            for entry in new_entries:
                if tweet_count >= tweet_limit:
                    print(f"Tweet limit of {tweet_limit} reached. Stopping further tweets.")
                    return  # Stop the script if the tweet limit is reached

                cleaned_comment = clean_text(entry['comment'])
                tweet_content = f"New entry added: {cleaned_comment}"
                chunks = split_text_into_chunks(tweet_content)

                for chunk in chunks:
                    if tweet_count >= tweet_limit:
                        print(f"Tweet limit of {tweet_limit} reached. Stopping further tweets.")
                        return  # Stop the script if the tweet limit is reached

                    if post_tweet(client, chunk):
                        tweet_count += 1
                        print(f"Tweet count: {tweet_count}")  # Print tweet count
                        time.sleep(1)  # To avoid hitting rate limits

                last_entry_id = entry['id']
                write_last_entry_id(last_entry_id)

            cursor.close()
            db_conn.close()

        except Exception as e:
            print(f"An error occurred: {e}")

        time.sleep(300)  # Check for new entries every 5 minutes

if __name__ == "__main__":
    run_bot()

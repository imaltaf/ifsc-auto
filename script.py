import csv
import os
import requests
from io import StringIO
from appwrite.client import Client
from appwrite.services.databases import Databases
from telegram import Bot

# Initialize Appwrite client
client = Client()
client.set_endpoint(os.getenv('APPWRITE_ENDPOINT'))
client.set_project(os.getenv('APPWRITE_PROJECT_ID'))
client.set_key(os.getenv('APPWRITE_API_KEY'))

# Initialize Appwrite database service
databases = Databases(client)

# Initialize Telegram bot
bot = Bot(token=os.getenv('TELEGRAM_BOT_TOKEN'))

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    return StringIO(response.text)

def import_csv_to_appwrite(csv_file, database_id, collection_id):
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Create a document in Appwrite for each row
        result = databases.create_document(
            database_id=database_id,
            collection_id=collection_id,
            data=row
        )
        print(f"Imported document: {result['$id']}")

    # Send Telegram notification
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    message = "CSV import to Appwrite completed successfully!"
    bot.send_message(chat_id=chat_id, text=message)

if __name__ == "__main__":
    csv_url = input("Enter the URL of the CSV file to download: ")
    database_id = os.getenv('APPWRITE_DATABASE_ID')
    collection_id = os.getenv('APPWRITE_COLLECTION_ID')
    
    try:
        csv_file = download_csv(csv_url)
        import_csv_to_appwrite(csv_file, database_id, collection_id)
    except requests.RequestException as e:
        print(f"Error downloading CSV file: {e}")
        # Send error notification via Telegram
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        error_message = f"Error occurred while downloading or processing CSV: {e}"
        bot.send_message(chat_id=chat_id, text=error_message)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # Send error notification via Telegram
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        error_message = f"An unexpected error occurred: {e}"
        bot.send_message(chat_id=chat_id, text=error_message)
import csv
import os
import requests
import asyncio
import uuid
from io import StringIO
from appwrite.client import Client
from appwrite.services.databases import Databases
from telegram.ext import Application
from appwrite.exception import AppwriteException

# Initialize Appwrite client
client = Client()
client.set_endpoint(os.getenv('APPWRITE_ENDPOINT'))
client.set_project(os.getenv('APPWRITE_PROJECT_ID'))
client.set_key(os.getenv('APPWRITE_API_KEY'))

# Initialize Appwrite database service
databases = Databases(client)

# Initialize Telegram bot
telegram_bot = Application.builder().token(os.getenv('TELEGRAM_BOT_TOKEN')).build()

# Define the expected attributes
EXPECTED_ATTRIBUTES = [
    "BANK", "IFSC", "BRANCH", "CENTRE", "DISTRICT", "STATE", "ADDRESS", "CONTACT",
    "IMPS", "RTGS", "CITY", "ISO3166", "NEFT", "MICR", "UPI"
]

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    return StringIO(response.text)

async def send_telegram_message(message):
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    await telegram_bot.bot.send_message(chat_id=chat_id, text=message)

def convert_to_boolean(value):
    return str(value).lower() in ('true', 'yes', '1', 't', 'y')

def process_row(row):
    processed_row = {}
    for key, value in row.items():
        if key not in EXPECTED_ATTRIBUTES:
            continue  # Skip unexpected attributes
        if key in ['IMPS', 'RTGS', 'NEFT', 'UPI']:
            processed_row[key] = convert_to_boolean(value)
        elif key == 'ISO3166':
            processed_row[key] = str(value)[:2]  # Truncate to 2 characters
        else:
            processed_row[key] = str(value)  # Ensure all values are strings
    return processed_row

def import_csv_to_appwrite(csv_file, database_id, collection_id):
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        try:
            # Process the row to convert boolean fields, handle ISO3166, and filter attributes
            processed_row = process_row(row)
            
            # Generate a unique document ID
            document_id = str(uuid.uuid4())
            
            # Create a document in Appwrite for each row
            result = databases.create_document(
                database_id=database_id,
                collection_id=collection_id,
                document_id=document_id,
                data=processed_row
            )
            print(f"Imported document: {result['$id']}")
        except AppwriteException as e:
            print(f"Error importing row: {row.get('IFSC', 'Unknown IFSC')}")
            print(f"Error details: {str(e)}")
            # You might want to log this error or handle it in some way
            continue  # Skip this row and continue with the next one

async def main():
    csv_url = os.getenv('CSV_URL')
    if not csv_url:
        raise ValueError("CSV_URL not found in environment variables")
    
    database_id = os.getenv('APPWRITE_DATABASE_ID')
    collection_id = os.getenv('APPWRITE_COLLECTION_ID')
    
    try:
        csv_file = download_csv(csv_url)
        import_csv_to_appwrite(csv_file, database_id, collection_id)
        await send_telegram_message("CSV import to Appwrite completed successfully!")
    except requests.RequestException as e:
        error_message = f"Error occurred while downloading or processing CSV: {e}"
        print(error_message)
        await send_telegram_message(error_message)
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        await send_telegram_message(error_message)

if __name__ == "__main__":
    asyncio.run(main())
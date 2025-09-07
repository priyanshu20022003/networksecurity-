import os
import sys
import json
from dotenv import load_dotenv
import certifi
import pandas as pd
import numpy as np
import pymongo

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# Load environment variables
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
if not MONGO_DB_URL:
    raise RuntimeError("MONGO_DB_URL not found in .env file. Please set it first.")
print(f"MongoDB URL: {MONGO_DB_URL}")

ca = certifi.where()

class NetworkDataExtract:
    def __init__(self):
        try:
            logging.info("✅ NetworkDataExtract object created successfully")
        except Exception as e:
            raise NetworkSecurityException(str(e), "Error in __init__ of NetworkDataExtract")

    def csv_to_json_converter(self, file_path):
        """
        Reads CSV file and converts it into a list of JSON-like records.
        """
        try:
            logging.info(f"📂 Reading CSV file: {file_path}")
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            logging.info(f"✅ Successfully converted CSV to JSON. Total records: {len(records)}")
            return records
        except Exception as e:
            logging.error(f"❌ Failed to read or convert CSV: {file_path}")
            raise NetworkSecurityException(str(e), f"File path: {file_path}")

    def insert_data_mongodb(self, records, database, collection):
        """
        Inserts given records into MongoDB.
        """
        try:
            logging.info(f"🔗 Connecting to MongoDB database: {database}, collection: {collection}")
            mongo_client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)
            db = mongo_client[database]
            col = db[collection]
            result = col.insert_many(records)
            logging.info(f"✅ Inserted {len(result.inserted_ids)} records into {database}.{collection}")
            return len(result.inserted_ids)
        except Exception as e:
            logging.error("❌ Failed to insert data into MongoDB")
            raise NetworkSecurityException(str(e), f"Database: {database}, Collection: {collection}")


if __name__ == '__main__':
    file_path = './Network_data/phisingData.csv'  
    database = 'priyanshu'
    collection = 'network_data'

    try:
        network_obj = NetworkDataExtract()
        records = network_obj.csv_to_json_converter(file_path)
        number_of_records = network_obj.insert_data_mongodb(records, database, collection)
        print(f'Number of records inserted: {number_of_records}')
    except NetworkSecurityException as e:
        logging.error(e)
        print(e)
    except Exception as ex:
        logging.error(f"Unexpected error occurred: {ex}")
        print(f"Unexpected error: {ex}")

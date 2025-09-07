import pymongo
import pandas as pd

MONGO_DB_URL = "mongodb+srv://bhardwaj05priyanshu_db_user:Admin123@cluster0.a6wswrk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "priyanshu"
COLLECTION_NAME = "Network_data"

client = pymongo.MongoClient(MONGO_DB_URL)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Count documents
count = collection.count_documents({})
print(f"Total documents in collection: {count}")

# Fetch a few sample records
sample_data = list(collection.find().limit(5))
print("Sample records:")
for record in sample_data:
    print(record)

# Convert to DataFrame and check shape
df = pd.DataFrame(sample_data)
print(f"DataFrame shape: {df.shape}")
print(df.head())

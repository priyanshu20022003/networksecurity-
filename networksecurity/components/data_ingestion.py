import os
import sys
import numpy as np
import pandas as pd
import pymongo
from sklearn.model_selection import train_test_split

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact
from dotenv import load_dotenv

load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_collection_as_dataframe(self):
        """
        Read data from MongoDB collection and return as a pandas DataFrame.
        """
        try:
            logging.info(f"Connecting to MongoDB at {MONGO_DB_URL}")
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[self.data_ingestion_config.database_name][
                self.data_ingestion_config.collection_name
            ]

            logging.info(
                f"Reading data from collection: {self.data_ingestion_config.database_name}.{self.data_ingestion_config.collection_name}"
            )
            df = pd.DataFrame(list(collection.find()))

            if df.empty:
                raise ValueError(
                    f"No data found in MongoDB collection: {self.data_ingestion_config.database_name}.{self.data_ingestion_config.collection_name}"
                )

            logging.info(f"Number of rows fetched from MongoDB: {len(df)}")

            if "_id" in df.columns:
                df.drop(columns=["_id"], inplace=True)
                logging.info("Dropped '_id' column from DataFrame.")

            df.replace({"na": np.nan}, inplace=True)
            return df

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        """
        Split the given dataframe into train and test sets and save them as CSV files.
        """
        try:
            if dataframe.empty:
                raise ValueError("Dataframe is empty. Cannot perform train-test split.")

            logging.info("Performing train-test split.")
            train_set, test_set = train_test_split(
                dataframe,
                test_size=self.data_ingestion_config.train_test_split_ratio,
                random_state=42,
            )

            logging.info(
                f"Train set size: {train_set.shape[0]}, Test set size: {test_set.shape[0]}"
            )

            # Ensure directory exists
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            # Save datasets
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False)

            logging.info(
                f"Train and test files saved at {self.data_ingestion_config.training_file_path} and {self.data_ingestion_config.testing_file_path}"
            )

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Orchestrate data ingestion: fetch data from MongoDB, split into train-test, and save.
        """
        try:
            logging.info("Starting data ingestion process.")
            df = self.export_collection_as_dataframe()

            logging.info("Splitting data into train and test sets.")
            self.split_data_as_train_test(df)

            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path,
            )

            logging.info(f"Data Ingestion Artifact: {data_ingestion_artifact}")
            logging.info("Data ingestion process completed successfully.")
            return data_ingestion_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)

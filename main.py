from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
import sys

if __name__ == '__main__':
    try:
        # Create Training Pipeline Configuration
        training_pipeline_config = TrainingPipelineConfig()

        # Create Data Ingestion Config
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)

        # Initialize Data Ingestion Component
        data_ingestion = DataIngestion(data_ingestion_config)

        logging.info("Initiating the data ingestion process...")

        # Run Data Ingestion
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()

        logging.info("Data Ingestion Completed Successfully")
        print(data_ingestion_artifact)

    except Exception as e:
        raise NetworkSecurityException(e, sys)

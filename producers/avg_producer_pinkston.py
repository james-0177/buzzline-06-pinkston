"""
avg_producer_pinkston.py

Stream numeric data to a Kafka topic.

It is common to transfer csv data as JSON so 
each field is clearly labeled. 
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time  # control message intervals
import pathlib  # work with file paths
import csv  # handle CSV data
import json  # work with JSON data
from datetime import datetime  # work with timestamps

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("AVG_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("AVG_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE = DATA_FOLDER.joinpath("avg_le.csv")
FEMALE_FILE = DATA_FOLDER.joinpath("female_le.csv")
MALE_FILE = DATA_FOLDER.joinpath("male_le.csv")
logger.info(f"Total Average Data file: {DATA_FILE}")
logger.info(f"Female Data file: {FEMALE_FILE}")
logger.info(f"Male Data file: {MALE_FILE}")

#####################################
# Message Generator
#####################################


def generate_messages():
    """
    Read from three csv files and yield records one by one, until the file is read.

    Args:
        total_file (pathlib.Path): Path to avg_le.csv
        female_file (pathlib.Path): Path to female_le.csv
        male_file (pathlib.Path): Path to male_le.csv

    Yields:
        dict: JSON-ready message with year, total, female, and male ages.
    """
    try:
        logger.info(f"Opening total data file: {DATA_FILE}")
        logger.info(f"Opening female date file: {FEMALE_FILE}")
        logger.info(f"Opening male data file: {MALE_FILE}")

        with open(DATA_FILE, "r") as t_file, \
            open(FEMALE_FILE, "r") as f_file, \
            open(MALE_FILE, "r") as m_file:
        
            total_reader = csv.DictReader(t_file)
            female_reader = csv.DictReader(f_file)
            male_reader = csv.DictReader(m_file)

            for total_row, female_row, male_row in zip(total_reader, female_reader, male_reader):
                if "year" not in total_row or "age" not in total_row:
                    logger.error(f"Missing 'year' or 'age' in total row: {total_row}")
                    continue
                if "year" not in female_row or "age" not in female_row:
                    logger.error(f"Missing 'year' or 'age' in female row: {female_row}")
                    continue
                if "year" not in male_row or "age" not in male_row:
                    logger.error(f"Missing 'year' or 'age' in male row: {male_row}")
                    continue

                yield {
                    "year": int(total_row["year"]),
                    "total": float(total_row["age"]),
                    "female": float(female_row["age"]),
                    "male": float(male_row["age"])
                }
                    
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e.filename}. Exiting.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in message generation: {e}")
        sys.exit(3)

#####################################
# Define main function for this module.
#####################################


def main():
    """
    Main entry point for the producer.

    - Reads the Kafka topic name from an environment variable.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams messages to the Kafka topic.
    """

    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data files exists
    for file_path in [DATA_FILE, FEMALE_FILE, MALE_FILE]:
        if not file_path.exists():
            logger.error(f"Data file not found: {file_path}. Exiting.")
            sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for csv_message in generate_messages():
            producer.send(topic, value=csv_message)
            logger.info(f"Sent message to topic '{topic}': {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
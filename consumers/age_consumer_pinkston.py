"""
age_consumer_pinkston.py

Consume json messages from a Kafka topic and visualize average life expectancy in real-time.

Example Kafka message format:
{"year": "1951", "age": 57.6}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing

# Use a deque ("deck") - a double-ended queue data structure
# A deque is a good way to monitor a certain number of "most recent" messages
# A deque is a great data structure for time windows (e.g. the last 5 messages)
from collections import deque

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
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
    topic = os.getenv("AGE_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("AGE_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures (empty lists)
#####################################

years = []  # To store year for the x-axis
ages = []  # To store age readings for the y-axis

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()


#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """
    Update age vs. year chart.
    Args:
        rolling_window (deque): Rolling window of age readings.
        window_size (int): Size of the rolling window.
    """
    # Clear the previous chart
    ax.clear()

    # Create a bar chart using the plot() method
    # Use the year for the x-axis and age for the y-axis
    # Use the label parameter to add a legend entry
    # Use the color parameter to set the bar color
    ax.bar(years, ages, color="skyblue", label="Average Life Expectancy")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Year")
    ax.set_ylabel("Age")
    ax.set_title("Average Life Expectancy: 1900 - 2018 by James Pinkston")

    # Rotate x-axis label for readability
    plt.xticks(rotation=45)
    
    # Set y-axis limits and ticks
    ax.set_ylim(30, 100)
    ax.set_yticks(range(30, 101, 5))

    # Use the legend() method to display the legend
    ax.legend()

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)  


#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a JSON-transferred CSV message.

    Args:
        message (str): JSON message received from Kafka.
        
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        age = data.get("age")
        year = data.get("year")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if age is None or year is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the year and age to the chart data
        years.append(year)
        ages.append(age)

        # Update chart after processing this message
        update_chart()

        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # Clear previous run's data
    years.clear()
    ages.clear()

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    
    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")
        


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
    plt.ioff()  # Turn off interactive mode after completion
    plt.show()
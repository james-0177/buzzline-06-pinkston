"""
avg_consumer_pinkston.py

Consume json messages from a Kafka topic and visualize male and female life expectancy
compared to total average life expectancy from 1900 - 2018.

Example Kafka message format:
{"year": 1900, "total": 47.3, "female": 48.3, "male": 46.3}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing

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
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from statistics import mean

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


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("AVG_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures (empty lists)
#####################################

years = []  # To store year for the x-axis
total_ages = []  # To store age readings for the y-axis
female_ages = [] # To store female age readings for the y-axis
male_ages = [] # To store male age readings for the y-axis


#########################################
# Variables for Periodic Analytics Report
#########################################

DECADE_BLOCK = 10
decade_data = []
current_block_start = 1900


#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots(figsize=(12, 6))
fig.patch.set_facecolor('lightgray')
ax.set_facecolor('lightgray')

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
    
    """
    # Clear the previous chart
    ax.clear()

    # Create a bar chart using the plot() method
    # Use the year for the x-axis and age for the y-axis
    # Use the label parameter to add a legend entry
    # Determine the bar color for each bar
    colors = []
    for i, age in enumerate(total_ages):
        if i == 0:
            colors.append("steelblue")
        else:
            if age < total_ages[i - 1]:
                colors.append("darkred")
            else:
                colors.append("steelblue")

    # Draw bars with conditional colors
    ax.bar(years, total_ages, color=colors)

    # Draw lines with gender-based colors
    ax.plot(years, female_ages, color='#8A2BE2', label = 'Female Avg. Life Expectancy', linewidth=2)
    ax.plot(years, male_ages, color='navy', label = 'Male Avg. Life Expectancy', linewidth=2)

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Year")
    ax.set_ylabel("Age")
    ax.set_title("Average U.S. Life Expectancy: 1900 - 2018 by James Pinkston")

    # Rotate x-axis label for readability
    plt.xticks(rotation=45)

    # Set x-axis ticks
    ax.set_xticks(range(1900, 2025, 5))
    
    # Set y-axis limits and ticks
    ax.set_ylim(30, 90)
    ax.set_yticks(range(30, 91, 5))

    # Add grid lines to chart
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    # Use the legend() method to display the legend
    legend_handles = [
        Patch(color="steelblue", label="Increase/No Change in Avg. Life Expectancy"),
        Patch(color="darkred", label="Decrease in Avg. Life Expectancy"),
        Line2D([0], [0], color="#8A2BE2", label="Female Avg. Life Expectancy", linewidth=2),
        Line2D([0], [0], color="navy", label="Male Avg. Life Expectancy", linewidth=2)
    ]

    ax.legend(handles=legend_handles)

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)  


#######################################
# Periodic Decade Analytics Report
#######################################


def periodic_report(block, start_year, end_year):
    """
    Prints analytics for one full decade block.
    """
    totals = [d['total'] for d in block]
    females = [d['female'] for d in block]
    males = [d['male'] for d in block]

    avg_total = mean(totals)
    avg_female = mean(females)
    avg_male = mean(males)
    gender_gap = avg_female - avg_male

    lowest = min(block, key=lambda d: d['total'])
    highest = max(block, key=lambda d: d['total'])

    drops = []
    for prev, curr in zip(block, block[1:]):
        if prev['total'] - curr['total'] > 2:
            drops.append(curr['year'])

    RED = "\033[91m"
    RESET = "\033[0m"

    # print(f"\n=== Period: {start_year}-{end_year} ===")
    print("\n" + "=" * 50)
    print(f"📊 DECADE REPORT: {start_year}-{end_year}")
    print("-" * 50)
    print(f"Avg. Life Expectancy: Total = {avg_total:.1f}, "
          f"Female = {avg_female:.1f}, Male = {avg_male:.1f}")
    print(f"Lowest Life Expectancy: {lowest['year']} "
          f"(Total = {lowest['total']:.1f}, "
          f"Female = {lowest['female']:.1f}, "
          f"Male = {lowest['male']:.1f})"
          f"{RED + '⚠️' + RESET if lowest['year'] in drops else ''}")
    print(f"Highest Life Expectancy: {highest['year']} "
          f"(Total = {highest['total']:.1f}, "
          f"Female = {highest['female']:.1f}, "
          f"Male = {highest['male']:.1f})")
    print(f"Gender Gap (Avg.): {gender_gap:.1f} years")

    if drops:
        print(RED + f"⚠️  Significant drop(s) detected in year(s): {', '.join(map(str, drops))}" + RESET)

    # print("========================================\n")
    print("=" * 50 + "\n")


#######################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    global decade_data, current_block_start
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
        year = data.get("year")
        total = data.get("total")
        female = data.get("female")
        male = data.get("male")
        
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if total is None or year is None or female is None or male is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the year and ages to the chart based on gender
        years.append(year)
        total_ages.append(total)
        female_ages.append(female)
        male_ages.append(male)
        
        # Update chart after processing this message
        update_chart()

        # Decade report tracking
        decade_data.append({
            'year': year,
            'total': total,
            'female': female,
            'male': male
        })

        # If we've reached the end of a decade (e.g., 1910, 1920, 1930...)
        if (year - current_block_start) == DECADE_BLOCK:
            # Print the report for this finished decade
            periodic_report(decade_data, current_block_start, year - 1)

            # Reset for the next decade block
            decade_data.clear()
            current_block_start = year
            
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
    total_ages.clear()
    female_ages.clear()
    male_ages.clear()
    
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
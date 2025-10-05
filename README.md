# buzzline-06-pinkston

The goal of this project is to analyze Average U.S. Life Expectancy from 1900 - 2018 and visualize that data on a live chart. It will also breakdown the average life expectancy by gender and compare those breakdowns to the total average.

The project uses a producer script to load data from three CSV files, converts the data into JSON messages, and then stores them in a Kafka topic. A consumer script reads the topic, extracts the messages, and then renders the data on a live-updating chart.

This project requires the use of GitHub, Python 3.11, VS Code, Apache Kafka, and WSL (if using a Windows machine). It is assumed that the user already has a GitHub account, and that their machine is setup to run and process Python projects/code through VS Code and that Apache Kafka is also setup to run.

The expected outcome is to have a combo bar/line chart that updates live as data is read into it.

## Copy this Project

1. Copy/fork this project into your GitHub account.
2. Rename the project as you desire.
3. Clone the project down to your machine and open in VS Code.

## Task 1. Start Kafka (using WSL if on a Windows machine)

If Windows, open a PowerShell terminal in VS Code:

```powershell
wsl
```

At the bash prompt, enter the following commands:
```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```
** KEEP THIS TERMINAL OPEN!** Kafka is running and needs to stay active.

For detailed instructions, see [SETUP_KAFKA](SETUP_KAFKA.md)

## Task 2. Setup/Manage Local Project Virtual Environment

Open a new terminal in VS Code:

### Windows
```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip wheel setuptools
python -m pip install --upgrade -r requirements.txt
```

### Mac/Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

## Task 3. Start the Kafka JSON Producer

Open a new terminal in VS Code (this can be the same terminal as Task 2):

### Windows
```powershell
.venv\Scripts\Activate.ps1
python -m producers.avg_producer_pinkston
```

### Mac/Linux
```bash
source .venv/bin/activate
python3 -m producers.avg_producer_pinkston
```

## Task 4. Start the Kafka JSON Consumer

Open a new terminal in VS Code (for a total of three terminal windows):

### Windows
```powershell
.venv\Scripts\Activate.ps1
python -m consumers.avg_consumer_pinkston
```

### Mac/Linux
```bash
source .venv/bin/activate
python3 -m consumers.avg_consumer_pinkston
```

## Stop the Continuous Process

To kill the terminal, press CTRL + C. This works in the powershell/bash terminals as well as the WSL terminal.
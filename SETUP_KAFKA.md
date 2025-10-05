# Apache Kafka Set Up

> Run **Apache Kafka** locally free and open source.

---

## Step 0. If Windows, Start WSL

Windows users - be sure you have completed the installation of WSL as shown in [https://github.com/denisecase/pro-analytics-01/blob/main/01-machine-setup/03c-windows-install-python-git-vscode.md](https://github.com/denisecase/pro-analytics-01/blob/main/01-machine-setup/03c-windows-install-python-git-vscode.md).

- We can keep working in **Windows VS Code** for editing, Git, and GitHub.  
- When you need to run Kafka or Python commands, just open a **WSL terminal** from VS Code.  

Launch WSL. Open a PowerShell terminal in VS Code. Run the following command:

````powershell
wsl
````

You should now be in a Linux shell (prompt shows something like `username@DESKTOP:.../buzzline-02-case$`). 

Do **all** steps below in this WSL shell.

---

## Step 1. Install Java (Required for Kafka)

Apache Kafka requires Java to run. Install Java 17 for best compatibility.

### Step 1A: Windows (WSL) / Ubuntu / Debian Users

1. Update your package manager
2. Install Java 17 OpenJDK
3. Verify Java installation
4. Install additional tools we'll need

````bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-17-jdk curl wget
java --version
curl --version
wget --version
````

### Step 1B: macOS Users

````bash
# Install Java via Homebrew
brew install openjdk@17

# Add Java to PATH
echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Verify installation
java --version
````

### Step 1C: Red Hat/CentOS/Fedora Users

````bash
sudo dnf install -y java-17-openjdk curl wget
java --version
````

**Expected output**: You should see something like `openjdk 17.x.x` in the version output.

**If you have Java 21 or higher**, configure to use Java 17:
````bash
sudo update-alternatives --config java
# Select option for Java 17
````

---

## Step 2. Download Apache Kafka

1. Navigate to home directory
2. Download the latest Kafka binary distribution (Scala 2.13 version)
3. Verify the download was successful

````bash
cd ~
curl -O https://downloads.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz
ls -la kafka_2.13-3.9.1.tgz
````

**Expected output**: You should see the downloaded file with size around 100MB.

**If download fails**, try the backup mirror:
````bash
curl -O https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
````

---

## Step 3. Extract and Set Up Kafka

1. Extract the downloaded archive
2. Rename the folder to simply "kafka" for easier access
3. Navigate to the Kafka directory
4. Make all scripts executable
5. Verify the setup

````bash
tar -xzf kafka_2.13-3.9.1.tgz
mv kafka_2.13-3.9.1 kafka
cd ~/kafka
chmod +x bin/*.sh
ls -la bin/ | head -10
````

**Expected output**: You should see executable script files like `kafka-server-start.sh`, `kafka-topics.sh`, etc.

---

## Step 4. Configure Kafka for Development

1. Generate a unique cluster ID for your Kafka instance
2. Format the log directories using KRaft mode (no Zookeeper needed)
3. Verify the configuration

````bash
cd ~/kafka

# Generate cluster UUID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo "Generated Cluster ID: $KAFKA_CLUSTER_ID"

# Format storage with the cluster ID
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# Verify configuration file exists
ls -la config/kraft/server.properties
````

**Expected output**: You should see a UUID generated and "Formatting..." messages, followed by the config file listing.

---

## Step 5. Start Kafka Broker

1. Start the Kafka server in the foreground
2. Keep this terminal open - Kafka will run here
3. Watch for "started (kafka.server.KafkaServer)" message

````bash
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
````

**Expected output**: You should see many log messages ending with:
```
[2024-xx-xx xx:xx:xx,xxx] INFO [KafkaServer id=1] started (kafka.server.KafkaServer)
```

**Keep this terminal open!** Kafka is running and needs to stay active.

---

## Step 6. Test Kafka in New Terminal

Open a **second terminal** (WSL for Windows, regular terminal for Mac/Linux) and test Kafka functionality.

**Windows users**: Open a new WSL terminal in VS Code  
**Mac/Linux users**: Open a new terminal tab/window

1. Change directory to ~/kafka.
2. Create a topic named `test-topic`
3. List all topics to verify creation
4. Describe the topic to get topic details

````bash
cd ~/kafka

bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


bin/kafka-topics.sh --list --bootstrap-server localhost:9092


bin/kafka-topics.sh --describe --topic test-topic --bootstrap-server localhost:9092
````

**Expected output**: 
- Topic creation should show "Created topic test-topic."
- Topic list should show "test-topic"
- Topic description should show partition and replication details

---

## Step 7. Test Message Production

1. Change directory to ~/kafka.
2. Start the Kafka console producer.
3. Type several test messages
4. Each message will be sent when you press Enter
5. Exit with Ctrl+C when done

````bash
cd ~/kafka

bin/kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092
````

**What to do**: After running this command, type messages like:
```
Hello Kafka
This is message 2
Testing Apache Kafka
Final test message
```

Then press **Ctrl+C** to exit the producer.

**Expected output**: You should see a `>` prompt where you can type messages.

---

## Step 8. Test Message Consumption

1. Change directory to ~/kafka.
2. Start the Kafka console consumer
3. Read messages from the beginning of the topic
4. You should see the messages you just produced
5. Exit with Ctrl+C when done

````bash
cd ~/kafka

bin/kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092
````

**Expected output**: You should see all the messages you typed in Step 7:
```
Hello Kafka
This is message 2
Testing Apache Kafka
Final test message
```

Press **Ctrl+C** to exit the consumer.

Congratulations! You just created a topic (a place for ordered, streaming, reliable messages).

You produced messages to that topic. 

You consumed messages from that topic. 

Now let's switch to Python rather than the command line. 


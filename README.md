# 🧠 Spark Kafka Message Reader

## 📋 Overview
This project demonstrates how to use **Apache Spark Structured Streaming** to read and display messages from an **Apache Kafka** topic in real-time.

It connects to Kafka, subscribes to a given topic, and prints all incoming messages to the console — ideal for learning, debugging, or quick streaming tests.

---

## ⚙️ Prerequisites

Before running the script, make sure you have the following installed:

- **Java 17 (JDK 17)**
- **Python 3.8+**
- **Apache Spark (with PySpark)**
- **Kafka (running locally or remotely)**

---

## 🧩 1. Install Java JDK 17

### On Ubuntu / Debian:
```bash
sudo apt update
sudo apt install openjdk-17-jdk -y

## 🧩 2. Install pyspark
`pip3 install pyspark`

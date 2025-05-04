#!/bin/bash

# Install Java (required for Elasticsearch)
apt-get install -y openjdk-11-jdk

# Install Elasticsearch
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add -
echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-7.x.list
apt-get update || { echo "APT update failed"; exit 1; }
apt-get install -y elasticsearch=7.17.0

# Start Elasticsearch service
systemctl enable elasticsearch
systemctl start elasticsearch
# Wait for Elasticsearch to be ready
until curl -s http://localhost:9200; do sleep 1; done

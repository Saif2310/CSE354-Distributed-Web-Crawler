#!/bin/bash

# Install Java (required for Elasticsearch)
apt-get install -y openjdk-11-jdk

# Install Elasticsearch
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add -
echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-7.x.list
apt-get update || { echo "APT update failed"; exit 1; }
apt-get install -y elasticsearch=7.17.0

# Configure JVM heap size (adjust based on RAM)
echo "-Xms512m" | sudo tee -a /etc/elasticsearch/jvm.options
echo "-Xmx512m" | sudo tee -a /etc/elasticsearch/jvm.options

# Fix permissions for plugin installation
chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/plugins
chmod -R 755 /usr/share/elasticsearch/plugins

# Install repository-gcs plugin non-interactively
sudo -u elasticsearch /usr/share/elasticsearch/bin/elasticsearch-plugin install repository-gcs --batch

# Start Elasticsearch service with extended timeout
systemctl enable elasticsearch
systemctl start elasticsearch
# Wait for Elasticsearch to be ready
until curl -s http://localhost:9200; do sleep 1; done

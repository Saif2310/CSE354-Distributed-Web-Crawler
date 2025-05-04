#!/bin/bash

# Install Java (required for Elasticsearch)
apt-get install -y openjdk-11-jdk

# Install Elasticsearch
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add -
echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-7.x.list
apt-get update || { echo "APT update failed"; exit 1; }
apt-get install -y elasticsearch=7.17.0

# Configure JVM heap size (increased to 1GB)
echo "-Xms1g" | sudo tee -a /etc/elasticsearch/jvm.options
echo "-Xmx1g" | sudo tee -a /etc/elasticsearch/jvm.options

# Fix permissions for plugin installation
chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/plugins
chmod -R 755 /usr/share/elasticsearch/plugins

# Install repository-gcs plugin non-interactively
sudo -u elasticsearch /usr/share/elasticsearch/bin/elasticsearch-plugin install repository-gcs --batch

# Automatically set TimeoutStartSec to 300 seconds for elasticsearch.service
mkdir -p /etc/systemd/system/elasticsearch.service.d
cat > /etc/systemd/system/elasticsearch.service.d/override.conf << EOF
[Service]
TimeoutStartSec=300
EOF

# Reload systemd to apply the override
systemctl daemon-reload

# Start Elasticsearch service
systemctl enable elasticsearch
systemctl start elasticsearch

# Wait for Elasticsearch to be ready (up to 10 minutes)
timeout=600
elapsed=0
interval=10
while ! curl -s http://localhost:9200; do
    sleep $interval
    elapsed=$((elapsed + interval))
    if [ $elapsed -ge $timeout ]; then
        echo "Elasticsearch failed to start within $timeout seconds"
        systemctl status elasticsearch.service
        exit 1
    fi
    echo "Waiting for Elasticsearch to start... ($elapsed/$timeout seconds)"
done

echo "Elasticsearch started successfully"

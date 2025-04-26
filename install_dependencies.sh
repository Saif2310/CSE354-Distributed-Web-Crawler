#!/bin/bash

# Install system dependencies
apt-get install -y python3-dev build-essential libxml2-dev libxslt1-dev libssl-dev libffi-dev

# Install Python dependencies
pip3 install --upgrade pip
pip3 install google-cloud-pubsub==2.13.0
pip3 install google-cloud-storage==2.5.0
pip3 install scrapy==2.6.2
pip3 install elasticsearch==7.17.0
pip3 install google-cloud-monitoring==2.12.0

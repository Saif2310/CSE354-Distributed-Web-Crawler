#!/bin/bash

# Install system dependencies
apt-get install -y python3-dev build-essential libxml2-dev libxslt1-dev libssl-dev libffi-dev

# Install Python dependencies
pip3 install --upgrade pip
pip3 install google-cloud-pubsub==2.13.0
pip3 install google-cloud-storage==2.5.0
pip3 install scrapy==2.6.2
pip3 install Twisted==22.10.0
pip3 install pyOpenSSL==22.0.0  
pip3 install cryptography==36.0.0  
pip3 install attrs==22.2.0
pip3 install elasticsearch==7.17.0
pip3 install google-cloud-monitoring==2.12.0

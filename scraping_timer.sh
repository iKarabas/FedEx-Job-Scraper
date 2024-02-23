#!/bin/sh

while true; do
  scrapy crawl job_spider && python ../query.py
  sleep 120  # sleep for 2 minutes
done

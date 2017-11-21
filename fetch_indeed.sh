#!/bin/bash

dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "indeed fetching starts at $dt ..." >> /home/ubuntu/indeed_scraper/fetching_log.txt
python indeed.py >> /home/ubuntu/indeed_scraper/fetching_log.txt 2>&1
dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "indeed fetching ends at $dt ..." >> /home/ubuntu/indeed_scraper/fetching_log.txt

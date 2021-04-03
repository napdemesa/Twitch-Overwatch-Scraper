#!/bin/bash
cd /Users/nap/overwatch-scraper/Twitch-Overwatch-Scraper
#echo Starting up virtual environment... >> /Users/nap/overwatch-scraper/Twitch-Overwatch-Scraper/twitch_scrape.log
source venv/bin/activate
#echo Starting scrape... >> /Users/nap/overwatch-scraper/Twitch-Overwatch-Scraper/twitch_scrape.log
python twitch_scrape.py #>> /Users/nap/overwatch-scraper/Twitch-Overwatch-Scraper/twitch_scrape.log
#echo Done! >> /Users/nap/overwatch-scraper/Twitch-Overwatch-Scraper/twitch_scrape.log
deactivate
#echo Deactivating Virtual environment... >> /Users/nap/overwatch-scraper/Twitch-Overwatch-Scraper/twitch_scrape.log

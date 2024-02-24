SLEEP_TIME=120  # Sleep time in seconds

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'  # No color

while true; do
  echo -e "${GREEN}Running scraper at $(date)${NC}"
  scrapy crawl job_spider && python ../query.py
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}Scraping successful at $(date)${NC}"
  else
    echo -e "${RED}Scraping failed at $(date)${NC}"
  fi
  echo -e "${YELLOW}Sleeping for $SLEEP_TIME seconds...${NC}"
  sleep "$SLEEP_TIME"
done

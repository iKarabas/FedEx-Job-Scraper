FROM python:3.8-slim

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

ENV TWISTED_REACTOR twisted.internet.asyncioreactor.AsyncioSelectorReactor

CMD ["scrapy", "crawl", "job_spider"]

FROM python:3.8-slim

WORKDIR /app

# Copy only the requirements file first
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

ENV TWISTED_REACTOR twisted.internet.asyncioreactor.AsyncioSelectorReactor

CMD ["scrapy", "crawl", "job_spider"]

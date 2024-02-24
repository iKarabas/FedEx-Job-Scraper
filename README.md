
# Project 
This project is designed to scrape job postings from the FedEx career page. The scraper runs at 5-minute intervals, collecting and storing data in both PostgreSQL and MongoDB databases. The implementation includes automatic duplicate checking and removal of job postings that are no longer available, leveraging Redis for efficient tracking.

Key Features:

Scheduled scraping of job postings at 5-minute intervals.
Storage of cleaned data in PostgreSQL and MongoDB databases.
Automatic duplicate detection and removal using Redis.
Efficient algorithms to manage job availability tracking.


I got inspired by this file project_definition.pdf; It might be useful for you, too, as it has clear explanations. 

A video of the previous version of the project implemented according to project_definition.pdf: 
[Video link](https://youtu.be/YthLCzunf-E)

# Project Setup Guide

To run this project, you must install Docker Desktop, pgAdmin 4, and MongoDB Compass. Follow the steps below to set up the environment:

## Install Dependencies

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop).
2. Install [pgAdmin 4](https://www.pgadmin.org/download/).
3. Install [MongoDB Compass](https://www.mongodb.com/try/download/compass).

## Docker Setup

Open the Docker application.


## PostgreSQL Setup

In pgAdmin, create a database. You can use the naming conventions defined in the environment variables in the `docker-compose.yaml` file. Alternatively, you need to customize the variables such as port, name, and table name based on your database rules.

If you will just use the existing namings, then just create a database named jobs_database in the pgadmin4 application

## MongoDB Setup

Open the MongoDB Compass application.

## Run the Project

Navigate to the project directory containing the Dockerfile.

### Build Docker Environment

Run the following command to build the Docker environment:

docker-compose build

### Start the Process

Once the building is complete, start the process by running:

docker-compose up


# How does it work?

Every part of the application has its container and works together. The FedEx career website is easy to scrape because it responds to HTTP requests and gives all the job posting data for the current page inside the response, like a JSON file, even if it's messy. We can clean it up quickly.

The scraper runs every 5 minutes (you can change this according to your needs). Now, the challenge is saving the data in the database without getting duplicates. If we don't check for duplicates, the scraper will quickly fill our database with the same data. Some suggest deleting all data in the database and refilling it every time we scrape the website. But for big websites with tens of thousands of job postings, this is inefficient and will be slow.

My solution is simple. Create a unique field for each job posting in the database, like a primary key but designed for our algorithm. I called it "job_identifier," a mix of request_id, name, and job location. At the start of each scraping cycle, the scraper collects all job_identifiers in the database and puts them in Redis. This requires a minimal amount of resources and storage because each job_identifier is a small string, unlike the whole job ad. When parsing a job ad, the scraper checks if it's in the Redis structure. If not, it stores it. This way, it only adds new job postings after the first scrape.

But there's another problem. The above method only prevents duplicates. Imagine you saved a job in the first scrape, but in the second scrape, the job is filled, and the career website doesn't have it anymore. We must fix this, or our database will fill with closed job postings.

The solution is similar. At the start of each scrape, collect all job_identifiers and put them in another Redis structure with a different key_prefix. Each job_identifier is a key, and their value is initially false. As the parser reads job postings, it checks this Redis structure. If found, it changes the value to "true." Before ending the scrape, delete closed job postings (entries with "false") from all databases—Redis, PostgreSQL, and MongoDB. This is quite fast because we have our own primary_key named job_identifier to find and delete the entries.

Keeping the database management files separate is practical. If you need to scrape another website, just create a new job_spider. This way, you adapt to new websites without significant changes in the existing files.

To schedule the scraper, alternative methods such as utilizing Cron or Celery can be implemented. Although I attempted to employ a cronjob, it still requires debugging. Consequently, I decided to use a straightforward script that operates within the scraper container.

### Item.py Modifications

The content of `item.py` is open to modification. Fields in each job data item were selected based on importance and minimized for more efficient database storage. Feel free to modify as needed.

### Pipeline.py

After itemizing the JSON data, `pipeline.py` puts the data into databases.

### Query.py

After the parsing process is complete, `query.py` extracts all the data from the databases into corresponding CSV files.


# Testing
The current crawling speed averages 120 pages per minute. This speed improves after the initial scrape, thanks to a checking mechanism that utilizes Redis.

Verifying the duplication process is straightforward. After the second scrape, you'll notice that entries are only stored in the databases if there are new updates on the website job postings.

To test the deletion process, you have two options. The first is to modify the job_identifier field of an existing entry or create a new one in the PostgreSQL database using the pgadmin application. The second option is to be patient enough for the job postings on the website to undergo updates. You can observe the number and job_identifier of the deleted closed job postings on the terminal at the end of the scraping process.


# TODO List:

Improve Code Quality

Build a better Scheduler for scraping timings.

Create a control structure for a multi-device scraping program. 

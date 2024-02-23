
To-Do List:

Improve Code Quality

build a better Scheduler for scraping timings


Provide comprehensive documentation in the README.md file.



# Project 
This project is designed to scrape job postings from the FedEx career page. The scraper runs at 10-minute intervals, collecting data and storing it in both PostgreSQL and MongoDB databases. The implementation includes automatic duplicate checking and removal of job postings that are no longer available, leveraging Redis for efficient tracking.

Key Features:

Scheduled scraping of job postings at 10-minute intervals.
Storage of cleaned data in PostgreSQL and MongoDB databases.
Automatic duplicate detection and removal using Redis.
Efficient algorithms to manage job availability tracking.


For a comprehensive overview and detailed specifications of the project, please refer to project_definition.pdf


A video of the project: 
[Video link](https://youtu.be/YthLCzunf-E)

# Project Setup Guide

To run this project, you need to install Docker Desktop, pgAdmin 4, and MongoDB Compass. Follow the steps below to set up the environment:

## Install Dependencies

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop).
2. Install [pgAdmin 4](https://www.pgadmin.org/download/).
3. Install [MongoDB Compass](https://www.mongodb.com/try/download/compass).

## Docker Setup

Open Docker application.


## PostgreSQL Setup

In pgAdmin, create a database. You can use the naming conventions defined in the environment variables in the `docker-compose.yaml` file. Alternatively, you need to customize the variables such as port, name, and table name based on your database rules.

If you will just use the existing namings, then just creata a database named jobs_database in the pgadmin4 server

## MongoDB Setup

Just open MongoDB Compass application.

## Run the Project

Navigate to the project directory containing the Dockerfile.

### Build Docker Environment

Run the following command to build the Docker environment:

docker-compose build

### Start the Process

Once the building is complete, start the process by running:

docker-compose up

### Item.py Modifications

The content of `item.py` is open to modification. Fields in each job data item were selected based on importance and minimized for more efficient database storage. Feel free to modify as needed.

### Pipeline.py

After itemizing the JSON data, `pipeline.py` puts the data into databases.

### Query.py

After the parsing process is complete, `query.py` extracts all the data from the databases into corresponding CSV files.

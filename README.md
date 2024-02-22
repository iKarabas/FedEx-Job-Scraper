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

The process involves parsing JSON files. After parsing each job-data item, it checks whether it exists in the Redis cache. If it does, it skips the pipeline phase. The program processes files in the order `s01.json`, `s02.json`, and `s01.json`, each containing 100 job-data items. If the cache mechanism is successful, the databases will contain two hundred items. Even if it processed 300 job-data items, the number of stored job-data items is 200 because only 200 of them are unique .

Duplicate checking is based on the rule that two jobs with the same title and exact location are considered duplicates. This checking-rule is open for improvement.

### Item.py Modifications

The content of `item.py` is open to modification. Fields in each job data item were selected based on importance and minimized for more efficient database storage. Feel free to modify as needed.

### Pipeline.py

After itemizing the JSON data, `pipeline.py` puts the data into databases.

### Query.py

After the parsing process is complete, `query.py` extracts all the data from the databases into corresponding CSV files.

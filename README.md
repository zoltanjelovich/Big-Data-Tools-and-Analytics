# Weather Data ETL Pipeline with Apache Airflow

## Overview
This repository contains an Apache Airflow DAG that automates an ETL pipeline for weather data. The DAG extracts weather data for multiple cities from the OpenWeather API, transforms the data into a structured format, and loads it into both an AWS S3 bucket and a PostgreSQL database.

## Features
- Extracts real-time weather data for predefined cities using the OpenWeather API.
- Transforms temperature values from Kelvin to Celsius and organizes weather attributes.
- Loads the processed data into an AWS S3 bucket and a PostgreSQL database.
- Runs multiple times a day (at 08:00, 12:00, and 20:00 UTC) to keep weather data updated.
- Implements task dependencies using Airflow.

## DAG Workflow
1. **Start Pipeline**: Initiates the ETL process.
2. **API Availability Check**: Ensures the OpenWeather API is responsive.
3. **Extract Weather Data**: Fetches raw weather data from the API for each city.
4. **Transform Data**: Converts temperature values and formats the data into a structured table.
5. **Load Data to PostgreSQL**: Stores the transformed data in an RDS PostgreSQL database.
6. **Load Data to S3**: Saves the extracted weather data as CSV files in an AWS S3 bucket.
7. **End Pipeline**: Marks the completion of the ETL workflow.

## Setup Instructions
### Prerequisites
- Apache Airflow installed and running.
- PostgreSQL database configured with a valid connection in Airflow (`postgres_conn`).
- OpenWeather API key configured in Airflow (`openweather_api`).
- AWS S3 bucket with appropriate permissions.

### Steps
1. **Set Up Airflow Connections**
   - Add a new Airflow HTTP connection (`openweather_api`) with the OpenWeather API base URL (`https://api.openweathermap.org`).
   - Add a new PostgreSQL connection (`postgres_conn`) with credentials for the target database.

2. **Deploy the DAG**
   - Copy `weather_dag_final.py` to the Airflow DAGs folder (e.g., `/airflow/dags/`).
   - Restart the Airflow scheduler to detect the new DAG.

3. **Trigger the DAG**
   - Open the Airflow UI and enable the DAG named `weather_dag_final`.
   - Manually trigger a run or wait for the scheduled execution.

## Configuration
- Modify the `cities` list in `weather_dag_final.py` to specify the cities for which weather data should be extracted.
- Update the `postgres_conn` and `openweather_api` connection IDs in Airflow if necessary.
- Change the `s3` bucket path in the `load_to_s3` function if a different storage location is required.

## Author
Zoltan Jelovich

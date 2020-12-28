# Data ingestion to Bigquery using Dataflow and Bigquery Data Transfer

## Data Generation Notebock

This notebook is to generate data data will be manually ingested as a CSV periodically into Bigquery. The Notebook generates lattitudes and longitudes based on an extraction of Bigquery Chicago dataset. The CSV output is "chicago-manual.txt"

## Data Exploration Notebook

This notebook contains the data exploration for a future improvement of the data generation

## Prototype Data Pubslihing Notebook

This notebook publishes data to an existing topic in GCP to simulate a "near real time" integration with a source

## Setting up locally

This requires a GCP Key in the same folder as the notebooks. Conda environment already contains all the pip installations and the name as well.

Remember to set up the GOOGLE_APPLICATION_CREDENTIALS to the location of the GCP credentials locations using :
'''conda env config vars set GOOGLE_APPLICATION_CREDENTIALS=C:\Path to folder\GCP-keys.json'''

Set up the PROJECT_ID as an environment variable as well using a .env file in the same folder as the notebooks

## GCP set up

A table named chicago is a prerequisit. The creation of the table is in "chicago table.txt"
The service user must be able to publish messages to the topic in GCP
An ingestion pipeline using Dataflow which is subscribed to a topic in GCP
A The view of chicago_locations.sql can be used to set up a view on top of both tables

## Set up airflow

Airflow in this project is set up to use a local executioner instead of Celery since this is for demo purposes

First create a docker volume for postgres ''docker volume create postgresql''
To run airflow use ''docker-compose up''

## TODO

Add a BI engine instead of a view on top of the tables
Add an ingestion timestamp to improve the selection of distinct rows
Improve the data generation to fill the fares as well
Move the Data transfer to a Data Flow to create complex transformations
Finish Airflow publishing
Convert timestamp to datetime for dashboards

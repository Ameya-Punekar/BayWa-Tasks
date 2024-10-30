# Data Pipeline Monitoring for Public Power Data

This project contains a data ingestion and transformation pipeline monitoring system for public power data, developed in a Jupyter Notebook. The purpose of this notebook is to monitor data ingestion and transformation scripts, saving data into `.csv` files that can be used for BI/ML applications.


## Introduction

This notebook provides scripts to:
1. Ingest data.
2. Monitor data ingestion and transformation processes, capturing and logging errors, ingestion intervals, and other relevant metrics.

> **Note:** This notebook is optimized for Google Colab.

## Data Ingestion

The ingestion pipeline fetches data from an API based on specified parameters:

## Data Transformation

The pipeline also includes transformation scripts to prepare ingested data for analysis, converting the data into formats suitable for BI and ML applications.

## Logging

Logging captures all ingestion details, including errors and duration of each ingestion interval. Logs are saved to a file making it easy to track issues and review ingestion times.

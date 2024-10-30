# Ingestion Data Pipeline for Energy-Charts API

This repository contains scripts to build a data ingestion pipeline for fetching data from the Energy-Charts API. The pipeline is designed for efficient and reliable retrieval of public power, price, and installed power data, providing both historical backfill and real-time ingestion options. Each script saves data locally in JSON format, ready for analysis or further processing.

## Source Data

- **Energy-Charts API (Swagger UI)**
  - **Public Power Data**: Ingested every 15 minutes
  - **Price Data**: Ingested daily
  - **Installed Power Data**: Ingested monthly

## Structure

The folder `data_ingestion_scripts` contains separate scripts for each data source. You can run these scripts locally or in Jupyter Notebooks to save the ingested data to a specified local path.

## Scripts Overview

### 1. Public Power Data Ingestion Pipeline

This script retrieves public power data from the Energy-Charts API at 15-minute intervals and saves it locally as JSON. It provides two modes of operation:

- **Backfill Mode**: Fetches historical data over a specified date range.
- **Real-time Mode**: Retrieves data from midnight up to the current time, then updates every 15 minutes.

#### Features

- **Time Intervals**: Data is fetched at 15-minute intervals, adjustable to Central European Time (CET/CEST).
- **Time Zone Handling**: Automatically converts timestamps to CET or CEST.
- **Error Handling**: Captures and logs errors if API requests fail.

### 2. Price Data Ingestion Script

This script ingests daily electricity price data from the Energy-Charts API. It supports both backfill and daily ingestion modes, saving the data as JSON.

#### Features

- **Backfill Mode**: Retrieves historical price data within a specified date range.
- **Default Mode**: If no date range is specified, it defaults to fetching the current day's data.
- **Time Zone Consistency**: Automatically handles CET/CEST for consistent date alignment.
- **Data Validation**: Ensures JSON integrity after ingestion.

### 3. Installed Power Data Ingestion Script

This script retrieves monthly or yearly data on installed power in Germany, with options for including or excluding decommissioned installations. Data is saved in JSON format, providing an accessible structure for analysis.

#### Features

- **Customizable Frequency**: Fetches data at monthly or yearly intervals.
- **Data Format**: Outputs data in JSON array format for straightforward loading and analysis.

## Usage

To use any of these scripts, navigate to the `data_ingestion_scripts` folder and run the script for the desired data type. Data will be saved locally in JSON format at the specified path.


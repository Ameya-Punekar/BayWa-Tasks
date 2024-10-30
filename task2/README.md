# Task 2: Data Modeling and Analysis for BI/ML Use Cases

This folder provides a solution for Task 2, involving the creation and documentation of a data model to support the following Business Intelligence (BI) and Machine Learning (ML) use cases:

1. **Trend of Daily Public Net Electricity Production**: Track daily production in Germany for each production type.
2. **Price vs. Net Power Analysis**: Analyze daily electricity price against net power production for offshore and onshore wind.

To ensure data flows from ingestion to analysis, each script transforms and saves the data in a structured format ready for loading into a SQL database. The SQL queries for each use case provide a foundation for analysis and reporting.

## Folder Structure

The task is organized into three main subfolders:

### 1. `data_transformation_scripts`

This folder contains scripts to clean and transform raw JSON files into a structured CSV format, using PySpark. Each dataset is handled by a dedicated notebook:

- **`public_power_data_cleaning`**: 
  - Transforms raw JSON for public power data using PySpark.
  - Saves the cleaned data in CSV format due to challenges with saving Delta tables.

- **`price_data_cleaning`**:
  - Processes raw JSON for price data using PySpark.
  - Saves the cleaned data as CSV, as Delta table saving was unsuccessful.

- **`installed_power_cleaning`**:
  - Cleans and formats installed power data from raw JSON using PySpark.
  - Saves data in CSV format, as saving Delta tables was not feasible.

**Note**: Each cleaning script processes JSON data into CSV format due to Delta table save issues.

### 2. `cleaned_csv_data`

The cleaned data files are saved in this folder after running the transformation scripts. These CSV files can then be loaded into a SQL database, enabling analysis and reporting for BI/ML use cases.

### 3. `sql_queries_bi_ml_usecase`

This folder contains SQL queries that support the use cases. Each query is designed to deliver results for the following reports:

- **Daily Net Electricity Production Trend**:
  - Tracks daily net electricity production across various production types in Germany.

- **Daily Price vs. Net Power Analysis**:
  - Compares daily electricity prices with net power production specifically for offshore and onshore wind.

## Workflow

1. **Run the Data Transformation Scripts**:
   - Navigate to `data_transformation_scripts`.
   - Execute the transformation notebooks (`public_power_data_cleaning`, `price_data_cleaning`, and `installed_power_cleaning`).
   - Cleaned data will be saved as CSV files in the `cleaned_csv_data` folder.

2. **Load CSV Data into SQL Database**:
   - Import the CSV files from `cleaned_csv_data` into your SQL database.
   - Create tables according to your data model to store transformed data for reporting and analysis.

3. **Execute SQL Queries for BI/ML Use Cases**:
   - Open the SQL scripts in `sql_queries_bi_ml_usecase`.
   - Run the queries in your SQL database to generate results for each use case.

# Pinterest Data Pipeline Project

## Outline
This is a project which involved creating a system for extracting, storing, transforming and analysing emulated Pinterest post data through the creation of two data pipelines, with one for **batch processing** and one for **real-time processing** of streaming data. The aim of this project was to understand the process of building a fully functional data pipeline and improve my knowledge of various key data engineering software and tools, namely:
- **Python** - For running the Pinterest posts emulation via AWS RDS queries and interacting with Kafka and AWS Kinesis through API requests
- **Kafka (Using Amazon MSK)** - For ingesting the raw Pinterest data and writing it to topics in an S3 bucket for batch processing in Databricks
- **Amazon EC2** - For setting up a Kafka client machine
- **Apache Airflow (Using Amazon MWAA)** - For scheduling batch processing tasks
- **Amazon Kinesis** - For ingesting the raw Pinterest data as data streams for real-time processing in Databricks
- **Amazon API Gateway** - For deploying a REST API to interact with MSK and Kinesis
- **Databricks** - For data cleaning and transformation (batch and real-time processing) and SQL queries for identifying key metrics

## Installation and Initialisation
The code for this project was created and tested using Python 3.12.0 with the *json*, *random*, *sqlalchemy* and *time* libraries. Running the **user_posting_emulation.py** script effectively initiates the data pipeline for batch processing via Kafka and Databricks while the **user_posting_emulation_streaming.py** script can be used for the streaming data pipeline via Kinesis. The data can then be transformed using Dabatabricks with the relevant notebooks that are included in this project directory. Please note that the detailed setup of the relevant AWS cloud infrastructure mentioned in the outline is outwith the scope of this readme.

## Project Structure
### Batch Processing Pipeline

## File structure
```
├── __main__.py
├── database_utils.py
├── data_extraction.py
├── data_cleaning.py
├── sql_queries
|    ├── creating_database_schema.sql
|    └── querying_data_for_metrics.sql
└── extracted_data
    ├── card_details.csv
    ├── card_details.pdf
    ├── event_details.json
    ├── order_details.csv
    ├── product_details.csv
    ├── product_details_weights_converted.csv
    ├── store_details.csv
    └── user_details.csv

```
### Python Files (.py)
- *\__main\__.py* - Main script for the complete data pipeline
- *database_utils.py* - Code for establishing connection interface with local and cloud-based SQL databases
- *data_extraction.py* - Code for extracting relevant datasets from a range of sources online (Amazon RDS instance, S3 bucket, pdf table, AWS API endpoint)
- *data_cleaning.py* - Code for cleaning each dataset with a variety of techniques within the Pandas library

### SQL Files (.sql)
- *creating_database_schema.sql* - SQL queries for creating database schema, including setting data types and primary and foreign key constraints
- *querying_data_for_metrics.sql* - SQL queries for obtaining a range of business metrics surrounding sales through the usage of aggregate functions, joins, subqueries etc
  
### Extracted Data Files
These are the files containing the raw data from the numerous sources mentioned above, which are then used by the functions in the **data_extraction.py** module. They are downloaded automatically into this location upon running the **__main__.py** script but are included here for clarity, as some of the extraction methods require credentials that are not included in this remote directory.

## License information
This is free and unencumbered software released into the public domain.





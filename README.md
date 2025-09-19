Car Sales ETL Pipeline

This project demonstrates an end-to-end ETL pipeline designed to process and transform car sales data using Azure Data Factory (ADF) and Azure Databricks. The pipeline ingests raw data, applies transformations, and stores the refined data in a structured format suitable for analytics.

Project Structure

databricks/: Contains Databricks notebooks for data transformation and processing.

data/: Sample raw datasets used for testing and development.

factory/: Azure Data Factory pipeline configurations and related files.

linkedService/: Definitions for linked services in ADF.

pipeline/: ADF pipeline JSON files for orchestrating data workflows.

DDL/: Data Definition Language scripts for creating necessary database structures.

Technologies Used

Azure Data Factory (ADF): For orchestrating data workflows and managing data movement.

Azure Databricks: For scalable data processing and transformation using Spark.

Python: For scripting and automation tasks within Databricks notebooks.

SQL: For querying and transforming data within Databricks.

Azure Data Lake Storage (ADLS): For storing raw and processed data.

Getting Started
Prerequisites

An active Azure subscription with access to Azure Data Factory and Azure Databricks.

Basic knowledge of Azure services and data engineering concepts.

Setup

Clone this repository to your local machine:

git clone https://github.com/Kiranc9901/car_sales_project.git


Navigate to the factory/ directory and deploy the ADF pipeline configurations to your Azure Data Factory instance.

In the databricks/ directory, upload the Databricks notebooks to your Azure Databricks workspace.

Configure the linked services in ADF to connect to your data sources and destinations.

Trigger the pipeline in ADF to start the data ingestion and transformation process.

Usage

Once the pipeline is set up and running, it will:

Ingest raw car sales data from specified sources.

Apply necessary transformations using Databricks notebooks.

Store the processed data in Azure Data Lake Storage in a structured format.

License

This project is licensed under the MIT License.

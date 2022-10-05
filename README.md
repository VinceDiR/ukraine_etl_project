# **Diamond Price Predictor App using Streamlit** #

## **Statement of Purpose** ##

This project's purpose is to collect and aggregate event-level data from the ongoing conflict in Ukraine on a daily basis. 

I have created a basic Python script using the Requests package and PySpark to ingest, clean, and upload the data to AWS S3. I have also created an AWS Glue Crawler that is triggered on the uploaded parquet files to read them into an AWS Athena table. I've created an Airflow DAG to execute this script and crawler daily at midnight.

Finally, a frontend application has been built using Streamlit and Pyathena to make the Athena database accessible to end users.

### **Data Description** ###

The data we will be using for this project is the [ACLED Conflict Data](https://www.acleddata.com), which is freely available to use (with attribution) via API and Export Tool. It contains event-level data for all incidents within Ukraine since the beginning of the conflict on 24 February 2022. The data is updated weekly and contains information on the actors, location, type of event, and number of fatalities. The data is retrieved in JSON format for the date 8 days prior to a given day.

### **Tools** ###

- **Requests** for accessing the ACLED API Endpoint

- **PySpark** for data munging and cleaning. Also Writing data to AWS S3.

- **AWS S3** for storing parquet files.

- **AWS Glue** for reading parquet files into AWS Athena.

- **Airflow** for orchestrating the ETL process on a daily basis.

- **PyAthena** for querying the Athena data in Python.

- **Streamlit** for building a frontend application.

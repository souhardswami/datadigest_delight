# Data Digest and Report Generation Application

This application is designed to ingest data from CSV files, generate reports, and provide access to those reports through a Flask web application. It uses Celery for background task processing.

## Running the Application

### To Run the Application

To start the application, use the following command:

```bash
sh server.sh

celery -A celery_app.celery_app worker --loglevel=info --concurrency 4
```

## Ingesting Data from CSV
### To ingest data from a CSV file, follow these steps:


Navigate to the digest_csv_file_into_db directory.
Paste your data file into the /data_files directory.
Run the Python script to ingest the data:

```
cd digest_csv_file_into_db
python script.py

```
# Crypto Data Pipeline

A Python application that queries the CoinGecko API to fetch daily cryptocurrency data, stores the raw data in BigQuery, and calculates metrics such as moving averages using SQL in BigQuery. The application is automatically deployed to Cloud Run and executed daily with Google Cloud Scheduler.

## Architecture

1. Cloud Run executes the Python app daily.
2. The app queries the CoinGecko API and retrieves cryptocurrency information.
3. Raw data is inserted into BigQuery (`crypto_raw.market_history`)
4. Metrics such as moving averages are calculated in BigQuery using SQL.
5. Final metric tables are stored in the dataset `crypto_analytics.market_history_analytics`.
6. Google Cloud Scheduler triggers the daily execution.
7. API keys and project ID are securely stored in Secret Manager.

## Technologies

- Python 3.x
- Google Cloud Platform:
  - BigQuery
  - Cloud Run
  - Cloud Scheduler
  - Secret Manager
- Python Libraries:
  - `requests`
  - `Flask`
  - `google-cloud-bigquery`
  - `google-auth`

## Installation & Setup

1. Clone the repository:

```bash
git clone https://github.com/username/crypto-data-pipeline.git
cd crypto-data-pipeline

## Environment variables

API_KEY=xxx
PROJECT_ID=xxx

## Local Development with Docker

This project is designed to run inside a Docker container. To run it locally:

1. Build the Docker image:

```bash
docker build -t crypto-data-pipeline .

docker run -e API_KEY="your_api_key" -e PROJECT_ID="your_project_id" crypto-data-pipeline

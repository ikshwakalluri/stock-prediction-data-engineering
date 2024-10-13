
# Stock Data Pipeline with Airflow and AWS S3

## Project Overview
This project automates the extraction of stock market data using Yahoo Finance (`yfinance` library) and uploads the data to AWS S3. The data is partitioned and stored in Parquet format for efficient storage and further analysis. The pipeline is orchestrated using Apache Airflow, running locally in Docker containers, ensuring smooth and automated data extraction and uploading.

## Features
- **Automated fetching of historical stock data** for over 1,064 companies spanning 20 years (from 2000-01-01 to 2024-10-01).
- **Efficient data storage**: Data is stored in AWS S3 using S3Hook in Parquet format, partitioned by company and year for future analysis.
- **Scalability**: The project can be extended to include daily stock data updates. For now, it's running locally to minimize costs but could be shifted to cloud infrastructure for future scalability.

## Getting Started
Ensure you have the following tools installed:
- **Docker**
- **Docker Compose**
- **AWS S3 Bucket** (properly configured)

### Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-repo/stock-data-pipeline.git
   cd stock-data-pipeline
   ```

2. **Configure AWS environment variables**:
   - Ensure your AWS credentials and region are set up in the `.env` file.
   - Ensure the AWS account has the necessary permissions for AWS S3.

3. **Build and run Docker containers**:
   - The Docker Compose file is based on the official Apache Airflow setup, modified for this project.
   - Start Airflow using:
     ```bash
     docker compose up
     ```

4. **Access Airflow UI**:
   - Once the containers are up, access the Airflow web interface at `http://localhost:8080` with the default credentials (username: `airflow`, password: `airflow`).

5. **Trigger the DAG**:
   - Unpause and trigger the DAG `stock_data_airflow_local_aws_s3` to fetch and upload historical stock data.

## Usage
- The DAG fetches stock market data for companies listed in the `company_tick_symbols_processed.csv` file.
- Data is fetched in batches and uploaded to AWS S3 in Parquet format.
- Further analysis can be performed using AWS Glue or AWS SageMaker.

## Future Improvements
- **Daily stock data updates**: Implement real-time daily stock data updates and transition the pipeline to cloud infrastructure.
- **Data validation and error handling**: Add more comprehensive data validation and error-handling mechanisms.
- **Monitoring and alerting**: Implement monitoring and alerting systems using AWS CloudWatch or similar services.

## Key Learnings and Insights

These learnings and insights come from working on this project and may benefit the community:

### Understanding APIs and Rate Limits
- Initially, Alpha Vantage API was considered, but its 25-request-per-day limit made it unsuitable for large-scale development.
- The Yahoo Finance API offers more flexibility, but managing timeouts and rate limits is essential when scaling for larger datasets.

### Efficient Data Handling
- Parquet format was chosen for its efficiency with structured data, reducing storage costs and improving read performance.
- Partitioning data by company and year optimized storage and querying, handling over 20 years of stock data effectively.

### Parallel Processing and Batch Uploads
- While Python's `concurrent.futures` was explored to speed up the fetching process, parallel fetching wasn’t ideal for small files and API constraints. It may be more effective with larger datasets in future iterations.
- Dividing companies into batches added resilience. If a process fails, only the affected batch needs retrying. This approach also prepares the pipeline for future daily updates while controlling costs.

### Cloud Cost Optimization
- Running the project locally minimized cloud costs during early development, allowing for refinement of the pipeline without incurring high AWS costs. The project can scale to the cloud when necessary.

## Contribution
Feel free to raise issues, submit PRs, or reach out if you want to collaborate or suggest improvements.

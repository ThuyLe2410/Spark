# Sales Data Analysis with Apache Spark
## Project Overview
This project leverages Apache Spark to perform data analysis on a sales dataset. The dataset used for analysis is SalesJan2009.csv, which contains sales transactions from January 2009. The primary goal of this project is to demonstrate the capabilities of Apache Spark for data processing, transformation and analysis.

## Key Features:
- Data Loading and Cleaning: Load and clean the sales data, including handling missing values and filtering invalid entries.
- Data Transformation: Perform various data transformations using PySpark, including filtering, grouping, aggregation, and pivoting.
Data Analysis: Use PySpark SQL and DataFrame API for querying and analyzing the data.
- Performance Optimization: Utilize Spark's in-memory processing and caching features to optimize performance for large datasets.
## Installation

### Install Apache Spark

```bash
brew update
brew install apache-spark
```


### Run Apache Spark cluster locally

```bash
$SPARK_HOME/sbin/start-master.sh # This will start Spark master process at localhost:8080
$SPARK_HOME/sbin/start-worker.sh <master_address> 
```

### Install packages

```bash
# Create/activate virtual env (if needed)
python3 -m venv <env_name>
source ./<env_name>/bin/activate

# Install packages
pip install -r requirements.txt
```

## Run the project

```bash
python3 main.py
```

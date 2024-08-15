# Data Analysis with Apache Spark

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
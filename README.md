# sparkify-pyspark

## Content
* [Summary](#Summary)
* [ETL](#ETL)
* [Project structure](#Project-structure)
* [Installation](#Installation)

### Summary
This project involves the use of [S3](https://aws.amazon.com/en/s3/) (Data storage) and PySpark.

Data sources are provided by the public ``S3 buckets``. The first bucket contains information about songs and artists, while the second bucket contains simulated app activity logs by users. The objects contained in both buckets <br> are JSON files. 

#### Fact Table
`songplays`

#### Dimensions Table
`users`, `songs`, `artists`, `time`

### ETL
The ETL (Extract-Transform-Load) process is relatively straightforward, with 2 main steps.
1. Create `column_names.py` to allow easy access to column names, especially if changes are required.
2. Run `etl.py` to load data from the public S3 buckets into PySpark SparkSession.

### Project Structure
1. column_names.py
* Contains column name mappings
2. etl.py
* Contains helper function which connects to S3 buckets and runs ETL through PySpark before inserting back into an output S3 bucket
3. etl.ipynb
* Allows step-by-step running of PySpark ETL process to better see the input and output when running each step
4. dwh-example.cfg
* Example format for configurations required

### Installation
Clone this repository:
```
git clone https://github.com/terryyylim/sparkify-pyspark.git
```

Change to sparkify-pyspark directory
```
cd sparkify-pyspark
```

To prevent complications to the global environment variables, I suggest creating a virtual environment for tracking and using the libraries required for the project.

1. Create and activate a virtual environment (run `pip3 install virtualenv` first if you don't have Python virtualenv installed):
```
virtualenv -p python3 <desired-path>
source <desired-path>/bin/activate
```

2. Install the requirements:
```
pip install -r requirements.txt
```
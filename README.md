## Sparkify Data Lake

### Overview

This project builds an ETL pipeline relying on Apache Spark for a data lake hosted on Amazon S3 for Sparkify's analytics purposes.

The ETL loads song, artist and log data in JSON format from S3, processes the data into analytics tables in a star schema using Spark, and writes those tables into partitioned parquet formatted files on S3. 

At the end, a star schema has been used to allow the Sparkify team to readily run queries to analyse user activity on their app. Spark was used because it's powerful, can be run from a Python shell, and can be deployed in the cloud, among other reasons.

### Structure
The project contains the following components:

etl.py reads data from S3, processes it into analytics tables, and then writes them to new S3 bucket
dl.cfg contains your AWS credentials **(which should not be included in finish push or add it in .gitignore)**

### Analytics Schema
The analytics tables centers on the following fact table:

songplays - which contains a log of user song plays
songplays has foreign keys to the following dimension tables:

users_table - which contains users data
songs - which contains songs data
artists - which contains artists data
time - which contains start

### Instructions
You will need to create a configuration file dl.cfg with the following structure, with S3 read and write policy.



[AWS]
AWS_ACCESS_KEY_ID=<your_aws_access_key_id> <br/>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>

To execute the ETL pipeline from the command line, enter the following:

python etl.py --bucket <your-output-s3-bucket-name>
eg: if your output bucket is called "destination", then in command line, enter:
    `python etl.py --bucket "destination"`

Where <your-output-s3-bucket-name> is the name of a new S3 bucket where the final analytics tables will be written to. Make sure your working directory is at the top-level of the project.
    
You can work on your project with a smaller dataset found in the workspace, and then move on to the bigger dataset on AWS.

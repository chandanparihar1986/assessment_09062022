# Assessment

The aim of this assessment is to built an ETL pipeline that will read the incoming events, apply some
transformation using Spark and produce a partitioned table in Hive.
Just to extend the scope, this also supports writing the transformed data to a json file, however, this is implemented just for a demonstration purpose.


## Prepare Environment
- JDK 1.8
- Scala 1.12
- Spark 3.1.1
- Editor - Intellij or Eclipse
- for all reference libraries, please go through the pom.xml file

## Config file
- project configuration file is given under "config/config.properties"

## Build & Run
- One can build the project jar using "mvn install" if required
- To run the application, use the following command for reference
  `spark-submit --master local --class sse.assignment.assignment <application jar path> <configuration file>`
- Application jar file -> Use the assembled jar
- Configuration file -> config/config.properties 
- Please provide the absolute path for each file path given in the config file

## Test Data
- Please refer to data/user_events.csv or data/user_events_with_null_key.csv


## Unit Test
- Test cases are written under src/test/scala/assignment_test

## Airflow job
- this can be found under airflow directory
  - Due to time constraint, I couldn't run/test this in the local environment


## Performance consideration 

While aggregating the data
- For performance on the window function, this spark Jira was reffered - https://issues.apache.org/jira/browse/SPARK-8638
- Good case -> when the data is partitoned on both id and name, we can expect relatively low shuffling in spark
- Bad case ->  when the data is partitioned on some other columns or no partions, please expect high data shuffling (IO/Network overhead) on spark.
- The input files must be stored in the distributed file or no-sql system and ensure the right partitioning strategy in place in the source system.

While saving to hive table
- ToDo: Beside the date column, we can consider adding other columns to the partition key depending on the downstream use cases (this is to ensure we understand how the data is going to be consumed by the end user and to reduce the data scan while read)
- ToDo: If the above partitioning is not enough, buckting would be another option to optimize it further
- Ultimately our goal would be to strike a right balance between the optimum read & write while deciding on the partitioning

Caching
- there wasn't any real need of caching the data in this program, however, if we add more sinks to this program, we should consider caching the data e.g. output of the aggregate dataframe


## Design proposal
- Please refer to the proposed design architecture under architecture directory
- Assuming the on-prem cluster is in place, 
  - we can collect & ingest the data from both mobile events and backend databases into the distributed file system i.e. HDFS
  - I propose we should consider a good partitioning startegy with a right balance between read and write before ingesting data into HDFS with full understanding of the use case in terms of how much data will be consumed by the end user (Hot store) and how of of the data can be purged to a cold storage.
  - In this design I am storing both hot and cold data on HDFS but ideally we should consider storing the hot data (processed data) on a No-SQL system 
  - The partitioning strategy can very for each dataset except for the small dataset for which I will try to create view in the backend databases and expose just one single dataset to the Hadoop system. This is to ensure we are not ingesting too many small datasets to the big data system.
  - The data is processed using a spark batch process which reads partitioned data from the HDFS and produces a curated dataset that can be stored as a hive table
  - The data is stored in Parquet format so we get all the benefits of Parquet, thereby reducing the IO/network load for the downstream applications
  - The hive table(s) can be exposed to the stakeholders with an active directory based fine grain control
  - A JDBC connection can be provided to the Data Scientist or Data Analyst to query the data
  - Or they will also have an option to get it from the Tableau report that points to the same curated dataset
  - Just to clarify one thing on the design, the two HDFS images that I have added to the architecture, they are essentially the same Hadoop cluster. It was done just for a better readability.
  - For access control, it's important that we set up different AD groups for each stakeholder (one for one group of people) so that we can ensure a fine grain control over the curated datasets
  - This pipeline can be scheduled using Airflow with a trigger that sets it run once in a day
- Please note this design is put together with keeping low cost in mind. Few things to consider-
  - We might experience sporadic access failures when the underlying HDFS files are overwritten by the upstream ingestion job
  - Hive doesn't provide all capabilities of No-Sql system
  - So exposing curated dataset through a columaner based NoSQL database to the end user may perform better over time with low maintenance.


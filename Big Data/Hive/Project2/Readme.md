# Assignment

* Downloaded Dataset 1 - https://data.cityofnewyork.us/api/views/2bnn-yakx/rows.csv?accessType=DOWNLOAD

Data set is downloaded as parking_violations.csv
Change config.txt to customize the local, hadoop directories, the db name & the hive connection string

## Task
1. Create a schema based on the given dataset
2. Dump the data inside the hdfs in the given schema location.

### Solution
* Move data csv file from local to HDFS:  **sh moveData.sh**
* Create Database, Hive tables & Load data: **sh ingestToHive.sh**

## Assignment
* Part I: Examine the Data
1. Total Tickets for the year
2. Total unique states the cars which got parking tickets came from!!
3. Total parking tickets having empty addresses
* Part II: Aggregation Queries
1. Top 5 Violations
2. 
	- Top 5 vehicle body type which got a parking ticket
	- Top 5 vehicle make which got a parking ticket
3. 
	- Top 5 Violating Precincts
	- Top 5 Issuer Precincts
4. Violation code frequency across 3 precincts which have issued the most number of tickets
5. Create view to divide discrete bins of violation time
6. 3 most commonly occurring violations across 6 equal discrete bins of violation time
7. The most common time bin for each of the 3 most commonly occuring violation codes
8. 
	- Creating view which divides issue date into different seasons
	- 3 most commonly occurring violations for each of season in NYC

### Solution
* Run Analysis: **sh assignment.sh**
Reporting tool for mongoDB
===========================
dba-mongo-report

Uses metrics from collStats and indexstats at present and generate a simple report.

### Usage

 You can run dba-mongo-report for either;
 
  * The mongo instance as a whole
  ```python
    dba-mongo-report.py 
```
  * A single db, along with all its collections
  ```python
    dba-mongo-report.py -d test 
```
  * A specific collection within a db
  ```python
    dba-mongo-report.py -d test -c restaurants
```
   * For help
   ```python
   dba-mongodb-reporting.py -h 
   
usage: dba-mongo-report.py [-h] [-db DB_NAME] [-coll COLL_NAME]

dba-mongodb-reporting

optional arguments:
  -h, --help       show this help message and exit
  -db DB_NAME      db name
  -coll COLL_NAME  collection name
```
    
The report covers:
  
  * Overview of the collection/s within the db/s
  * Wiredtiger stats for that collection
  * Index stats for all the indexes for that collection/s

### Required packages

pip install pymongo
pip install argparse
  
### Requirments

 Mongodb running wired tiger storage engine
  

### Sample

Running:

 ```python
    main.py -d test -c restaurants
```
Would yeild:

![alt tag](screenshots/db_report.PNG)




<!-- Headings -->
# Data Engineering: week 2 Homework Assignment (HIVE-1)
## 1.Hive
### (1) Distinguish HDFS (Hadoop Distributed File System) and Linux File System. Get familiar with HDFS commands. Practice following commands:
- List directories / filesin HDFS
- Copy files from LinuxFile System to HDFS
- Copy files from HDFS to Linux File System

Answer:

connect to jumpbox and edge node:
```
ssh jackie@54.86.193.122
ssh jackie@ip-172-31-92-98.ec2.internal
```

1. List directories / filesin HDFS;
```
hdfs dfs -ls
```
2. Copy files from LinuxFile System to HDFS
```
hdfs dfs -put *.parquet /user/jackie
```
``notice: the file I  chose is arbitrary``

``copy *.parquet from Linux file system to HDFS's /user/jackie``

3. Copy files from HDFS to Linux File System

```
hdfs dfs -get /user/jackie/*.parquet
```
<br />
<br />

### (2) In your hdfs home folder (/user/<your-id>), create a folder named banklist, and copy /data/banklist/banklist.csv to the banklist folder you just created

Answer:

```
hdfs dfs -mkdir /user/jackie/banklist
hdfs dfs -cp /data/banklist/banklist.csv /user/jackie/banklist
```
<br />
<br />

### (3) In your database, create a Hive managed table using the CSV files.  The CSV file has a header (first line) which gives the name of the columns.  Column ‘CERT’has type ‘int’, other columns have type ‘string’.  After create the table, run some queries to verify the table is created correctly.
- `desc formatted <your table>;`
- `select * from <your table> limit 5;`
- `select count(*) from <your table>;`
### Hint: Use org.apache.hadoop.hive.serde2.OpenCSVSerde. Study the banklist.csv file (hint: some columns have quoted text). Do some research using google to see which SERDEPROPERTIES you need to specify.

Answer:
```sql
CREATE TABLE if not exists jackie_db.banklist_managed(
   `Bank Name` string,
    City string,
    ST string,
    CERT int,
    `Acquiring Institution` string,
    `Closing Date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES ( 
    "separatorChar"=",",
    "quoteChar"="\"",
    "escapeChar"="\\"
)
STORED AS TEXTFILE tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/jackie/banklist/*.csv' INTO TABLE jackie_db.banklist_managed;
/* csv file vanishes after this command */
```

1. `desc formatted <your table>;`:

```sql
desc formatted jackie_db.banklist_managed;
```
2. `select * from <your table> limit 5;`:
```sql
select * from jackie_db.banklist_managed limit 5;
```

3. `select count(*) from <your table>;`:
```sql
select count(*) from jackie_db.banklist_managed;
```
<br />
<br />

### (4) In your database, create a Hive external table using the CSV files. After create the table, run some queries to verify the table is created correctly.
- `desc formatted <your table>;`
- `select * from <your table> limit 5;`
- `select count(*) from <your table>;`
- `drop table <your table>; verify the data folder is not deleted by Hive.`

Answer:

```sql
CREATE external TABLE if not exists jackie_db.banklist_external( 
    `Bank Name` string,
    City string,
    ST string,
    CERT int,
    `Acquiring Institution` string,
    `Closing Date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES ( 
    "separatorChar"=",",
    "quoteChar"="\"",
    "escapeChar"="\\"
)
STORED AS TEXTFILE
location '/user/jackie/banklist/'
tblproperties ("skip.header.line.count"="1");
```

```
# do following in terminal 
hdfs dfs -cp /data/banklist/banklist.csv /user/jackie/banklist
```

```sql
load data inpath '/user/jackie/banklist/*.csv' into table jackie_db.banklist_external;
```

1. `desc formatted <your table>;`:

```sql
desc formatted jackie_db.banklist_external;
```
2. `select * from <your table> limit 5;`:
```sql
select * from jackie_db.banklist_external limit 5;
```

3. `select count(*) from <your table>;`:
```sql
select count(*) from jackie_db.banklist_external;
```

4. `drop table <your table>; verify the data folder is not deleted by Hive.`:
```sql
drop table jackie_db.banklist_external;
```
<br />
<br />

### (5) Create a Hive table using AVRO file where you get from the SQOOP homework.

Answer:
```sql
create table if not exists jackie_db.banklist_avro(
    `Bank Name` string,
    City string,
    ST string,
    CERT int,
    `Acquiring Institution` string,
    `Closing Date` string
)
stored as avro;
```

```
# do following in terminal 
hdfs dfs -cp /data/banklist/banklist.csv /user/jackie/banklist
```

```sql
load data inpath '/user/jackie/banklist/*.csv' into table jackie_db.banklist_avro;
```
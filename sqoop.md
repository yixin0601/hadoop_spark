<!-- Headings -->
# Data Engineering: week 2 Homework Assignment(SQOOP)
## 1. SQOOP
<br >

### (1) Get familiar with HUE file browser
### HUE URL: https://hue.ascendingdc.com/hue
<br />

### (2) In hdfs, create a folder named retail_db under your home folder (/user/<your-id>), and 3 sub-folder named parquet, avro and text


ssh to jumpbox and edge node: 

```
ssh jackie@54.86.193.122
ssh jackie@ip-172-31-92-98.ec2.internal
```
Answer:
```
hdfs dfs -mkdir -p retail_db/parquet
hdfs dfs -mkdir  retail_db/avro
hdfs dfs -mkdir  retail_db/text
```
My Answer:
```
hdfs dfs -mkdir /user/jackie/retail_db
hdfs dfs -mkdir /user/jackie/retail_db/parquet
hdfs dfs -mkdir /user/jackie/retail_db/avro
hdfs dfs -mkdir /user/jackie/retail_db/text
```
<br />

### (3) Import all tables from MariaDBretail_db to hdfs, in parquet format, save in hdfs folder  /user/<your_id>/retail_db/parquet, each table is stored in a sub-folder, for example, products table should be imported to /user/<your-id>/retail_db/parquet/products/

Answer:

```
sqoop import-all-tables \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--compression-codec=snappy \
--as-parquetfile \
--warehouse-dir /user/jackie/retail_db/parquet
```
<br />

### (4) Import all tables from MariaDB retail_db to hdfs, in text format, use ‘#’ as field delimiter.  Save in hdfs folder  /user/<your_id>/retail_db/text, each table is stored in a sub-folder, for example, products table should be imported to /user/<your_id>/retail_db/text/products/

Answer:

```
sqoop import-all-tables \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--compression-codec=snappy \
--fields-terminated-by ‘#’ \
--warehouse-dir /user/jackie/retail_db/text
```
<br />

### (5) Import table order_items to hdfs, in avro format, save in hdfs folder /user/<your_id>/retail_db/avro


 * First run with one map task, then with two map tasks, compare the console output. List the sqoop behavior difference between one map task and two map tasks.

 Answer:
 1. run with one map task:
 ```
 sqoop import \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--compression-codec=snappy \
--as-avrodatafile \
--table order_items \
--target-dir /user/jackie/retail_db/avro
 ```
2. run with two map tasks:

```
sqoop import \
-m 2 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--compression-codec=snappy \
--as-avrodatafile \
--table order_items \
--target-dir /user/jackie/retail_db/avro2
```

``notice: when “target”, creating a new folder same time``

3. compare

1 file (large MB) and 2 files (small KB)
the sum of two m2 files' size equals the size of one m1 file

When specify multiplemap tasks: Sqoopqueries the range of primary key and use it to divide workload among map tasks.

Logs with 2 map tasks:

INFO db.DBInputFormat: Using read commited transaction isolationINFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`order_item_id`), MAX(`order_item_id`) FROM `order_items` 

INFO db.IntegerSplitter: Split size: 86098; Num splits: 2 from: 1 to: 172198

INFO mapreduce.JobSubmitter: number of splits:2

Logs with 1 map task:

INFO db.DBInputFormat: Using read commited transaction isolation

INFO mapreduce.JobSubmitter: number of splits:1
<br />
<br />

### (6) In edge node, in your home folder,Create a folder named “order_items_files” in Linux file system.

Answer:

```
mkdir order_items_files
```

``notice:``

``The “home folder” in “Linux file system” is /home/jackie, which is not /user/jackie``

``managed table saved in /user/hive, external table saved in /user/jackie``
<br />
<br />

### (7) Copy order_itemstable files generated in step 3, 4, 5 from HDFS to Linux file system, name them as 
- order_items.parquet,
- order_items.avro,
- order_items.txt

Answer:

1. order_items.parquet:

```
hdfs dfs -get /user/jackie/retail_db/parquet/order_items/*.parquet
mv *.parquet order_items.parquet
```
2. order_items.avro:

```
hdfs dfs -get /user/jackie/retail_db/avro/*.avro
mv *.avro order_items.avro
```

3. order_items.txt:

```
hdfs dfs -get /user/jackie/retail_db/text/order_items/part-m-00000
mv part-m-00000 order_items.txt
```

``notice: 'get' for copy file from HDFS to Linus; 'mv' for change name``
<br />
<br />

### (8) Use parquet-tools toshow following information from order_items.parquet

- schema
- rowcount
- first 5 records
- metadata

Answer:

```
cd order_item_files                                     # change working directory
parquet-tools --help
parquet-tools schema order_items.parquet                # schema
parquet-tools rowcount order_items.parquet              # rowcount
parquet-tools head -n 5 order_items.parquet             # first 5 records
parquet-tools meta order_items.parquet                  # metadata
```
<br />
<br />

### (9) Examine text file order_items.txt, and calculate rowcount, compare it with the rowcount in step (8)

My Answer:

```
wc -l order_items.txt
```

Answer:

```
cat order_items.txt
cat order_items.txt | wc –l
```

``notice: wc: word count; l: lines``
<br />
<br />

### (10) Use avro-tools to show following information from order_items.avro

- schema
- metadata
- convert to json files
- rowcount



Answer:

```
avro-tools --help
avro-tools getschema order_items.avro                   # schema
avro-tools getmeta order_items.avro                     # metadata
avro-tools tojson order_items.avro  > order_items.json  # convert to json files
avro-tools tojson order_items.avro | wc -l              # rowcount
```
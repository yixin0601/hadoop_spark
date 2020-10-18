<!-- Headings -->
# Data Engineering: week 6 Homework Assignment(Spark DataFrame)
## 1.Employee dataset
<br >
In HDFS /data/spark/employee, there are 4 files:

- dept.txt
- dept-with-header.txt
- emp.txt
- emp-with-header.txt

give employee and dept. information.

Answer following questions by:

(1) Spark SQL
(2) Spark DataFrame API

### Questions:


```
# connect to jumpbox:
ssh jackie@54.86.193.122
# connect to edge node:
ssh jackie@ip-172-31-92-98.ec2.internal
# connect to spark shell:
spark-shell
# set partition from 200 to 2:
spark.conf.set("spark.sql.shuffle.partition",2)
```

### 1.list total salary for each dept.

Method 1: Spark DataFrame API

```
# loda data:
val path1 = "/data/spark/employee/dept-with-header.txt"
val dept = spark.read.format("csv").option("header","true").option("inferSchema","true").load(path1)
val path2 = "/data/spark/employee/emp-with-header.txt"
val emp = spark.read.format("csv").option("header","true").option("inferSchema","true").load(path2)

# inner join and create a dataframe:
val joinExpression = dept.col("DEPT_NO") === emp.col("DEPTNO")
val df = emp.join(dept,joinExpression).drop(emp.col("DEPTNO"))

# show result:
df.groupBy("DEPT_NAME").sum("SAL").show()
+----------+--------+                                                           
| DEPT_NAME|sum(SAL)|
+----------+--------+
|     SALES|    9400|
|  RESEARCH|    6775|
|ACCOUNTING|    8750|
+----------+--------+
```

Method 2: Spark SQL

```
# need do Method 1 first
# create a temp view
df.createOrReplaceTempView("employee_table")
# show result:
spark.sql("select DEPT_NAME,sum(SAL) as total_salary from employee_table group by DEPT_NAME").show()
+----------+------------+                                                       
| DEPT_NAME|total_salary|
+----------+------------+
|     SALES|        9400|
|  RESEARCH|        6775|
|ACCOUNTING|        8750|
+----------+------------+
```


### 2.list total number of employee and average salary for each dept.

Method 1: Spark DataFrame API

```
# total number of employee:
df.count()
res6: Long = 12 

# average salary for each dept:
df.groupBy("DEPT_NAME").avg("SAL").show()
+----------+------------------+                                                 
| DEPT_NAME|          avg(SAL)|
+----------+------------------+
|     SALES|1566.6666666666667|
|  RESEARCH|2258.3333333333335|
|ACCOUNTING|2916.6666666666665|
+----------+------------------+
```

Method 2: Spark SQL

```
# total number  of employee:
spark.sql("select count(*) from employee_table").show()
+--------+
|count(1)|
+--------+
|      12|
+--------+

# average salary for each dept:
spark.sql("select DEPT_NAME,avg(SAL) as avg_salary from employee_table group by DEPT_NAME").show()
+----------+------------------+                                                 
| DEPT_NAME|        avg_salary|
+----------+------------------+
|     SALES|1566.6666666666667|
|  RESEARCH|2258.3333333333335|
|ACCOUNTING|2916.6666666666665|
+----------+------------------+
```

### 3.list the first hired employee's name for each dept.

Method 1: Spark DataFrame API



Method 2: Spark SQL

```
# create a temp view
dept.createOrReplaceTempView("dept")
emp.createOrReplaceTempView("emp")

spark.sql("select DEPTNO,NAME,HIREDATE from emp where (DEPTNO,HIREDATE) in (select DEPTNO,min(HIREDATE) from dept join emp on dept.DEPT_NO = emp.DEPTNO group by DEPTNO)").show()
+------+-----+-------------------+                                              
|DEPTNO| NAME|           HIREDATE|
+------+-----+-------------------+
|    20|SMITH|1980-12-17 00:00:00|
|    30|ALLEN|1981-02-20 00:00:00|
|    10|CLARK|1981-06-09 00:00:00|
+------+-----+-------------------+
```

### 4.list total employee salary for each city.

Method 1: Spark DataFrame API

```
df.groupBy("LOC").sum("SAL").show()
+-------+--------+                                                              
|    LOC|sum(SAL)|
+-------+--------+
| DALLAS|    6775|
|CHICAGO|    9400|
|NEW YOR|    8750|
+-------+--------+
```

Method 2: Spark SQL

```
spark.sql("select sum(SAL) as total_salary, LOC from employee_table group by LOC").show()
+------------+-------+                                                          
|total_salary|    LOC|
+------------+-------+
|        6775| DALLAS|
|        9400|CHICAGO|
|        8750|NEW YOR|
+------------+-------+
```

### 5.list employee's name and salary whose salary is higher than their manager

Method 1: Spark DataFrame API


Method 2: Spark SQL

```
spark.sql("select t1.NAME, t2.SAL from employee_table t1 INNER JOIN employee_table t2 on t1.MGR==t2.EMPNO where t1.SAL>t2SAL").show()
```

##### 6.list employee's name and salary whose salary is higher than average salary of whole company

Method 1: Spark DataFrame API



Method 2: Spark SQL

```
spark.sql("select NAME, SAL from employee_table where SAL > (select AVG(SAL) from employee_table)").show()
+-----+----+                                                                    
| NAME| SAL|
+-----+----+
|JONES|2975|
|BLAKE|2850|
|CLARK|2450|
| KING|5000|
| FORD|3000|
+-----+----+
```

### 7.list employee's name and dept name whose name start with "J"

Method 1: Spark DataFrame API

```
df.filter(col("NAME").startsWith("J")).select("NAME","DEPT_NAME").show()
+-----+---------+
| NAME|DEPT_NAME|
+-----+---------+
|JONES| RESEARCH|
|JAMES|    SALES|
+-----+---------+
```

Method 2: Spark SQL

```
spark.sql("select NAME,DEPT_NAME from employee_table where NAME LIKE 'J%' ").show()
+-----+---------+
| NAME|DEPT_NAME|
+-----+---------+
|JONES| RESEARCH|
|JAMES|    SALES|
+-----+---------+
```

### 8.list 3 employee's name and salary with highest salary

Method 1: Spark DataFrame API

```
df.orderBy(col("SAL").desc).select("NAME","SAL").show(3)
+-----+----+                                                                    
| NAME| SAL|
+-----+----+
| KING|5000|
| FORD|3000|
|JONES|2975|
+-----+----+
```

Method 2: Spark SQL

```
spark.sql("select NAME, SAL from emp order by SAL DESC limit 3").show()
+-----+----+                                                                    
| NAME| SAL|
+-----+----+
| KING|5000|
| FORD|3000|
|JONES|2975|
+-----+----+
```

### 9.sort employee by total income (salary+commission), list name and total income.

Method 1: Spark DataFrame API

```
df.withColumn("total_income",col("SAL")+col("COMM")).sort(desc("total_income")).select("NAME","total_income").show()
+------+------------+                                                           
|  NAME|total_income|
+------+------------+
|  KING|        5000|
|  FORD|        3000|
| JONES|        2975|
| BLAKE|        2850|
|MARTIN|        2650|
| CLARK|        2450|
| ALLEN|        1900|
|  WARD|        1750|
|TURNER|        1500|
|MILLER|        1300|
| JAMES|         950|
| SMITH|         800|
+------+------------+
```

Method 2: Spark SQL

```
spark.sql("select NAME, SAL+COMM as total_income from employee_table order by total_income DESC").show(false)
+------+------------+
|NAME  |total_income|
+------+------------+
|KING  |5000        |
|FORD  |3000        |
|JONES |2975        |
|BLAKE |2850        |
|MARTIN|2650        |
|CLARK |2450        |
|ALLEN |1900        |
|WARD  |1750        |
|TURNER|1500        |
|MILLER|1300        |
|JAMES |950         |
|SMITH |800         |
+------+------------+
```


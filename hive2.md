<!-- Headings -->
# Data Engineering: week 3Homework Assignment (Hive-2)
## 1.Banklist dataset
### (1)Write queries on banklist table:
- Findtop 5states with most banks. The result must show the state name and number of banks in descending order.


Answer:

```sql
SELECT state, count(*) as count from banklist
group by state
ORDER BY count DESC
LIMIT 5;
```

- Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.

Answer:

```sql
select substr(closing_date, -2, 2) as yr, count(*) as count
from banklist
group by substr(closing_date, -2, 2)
order by yr;
```
<br />
<br />

## 2.Chicago crime dataset:

https://data.cityofchicago.org/Public-Safety/Crimes-2020/qzdf-xmn8

### Check hive database named “chicago”, there is one table named:

- crime_parquet: include data from 2001-present

### (1) In your own database, create a partitioned table(partitionedby year)to store data, store in parquet format. Name the table “crime_parquet_16_20”;

Answer:

```sql
create table if not exists crime_parquet_16_20 (
    id  bigint,
    case_number string,
    `date`  bigint,       --seconds since 1970/01/01
    block   string,
    iucr    string,
    primary_type    string,
    description     string,
    loc_desc    string,
    arrest      boolean,
    domestic    boolean,
    beat        string,
    district    string,
    ward        int,
    community_area  string,
    fbi_code        string,
    x_coordinate    int,
    y_coordinate    int,
    updated_on      string,
    latitude        float,
    longitude       float,
    loc             string
)
partitioned by (yr int)
stored as parquet;
```

### (2) Import 2016 to 2020 data into the partitioned table from table chicago.crime_parquet.

```sql
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;

insert into crime_parquet_16_20partition (yr)
select
    id, 
    case_number, 
    `date`, 
    block, 
    iucr,
    primary_type, 
    description,
    loc_desc,
    arrest,
    domestic,
    beat,
    district,
    ward,
    community_area,
    fbi_code,
    x_coordinate,
    y_coordinate,
    updated_on,
    latitude,
    longitude,
    loc,
    yr
from crime_parquet
where yr>=2016 and yr<=2020;
```

### (3) Write queries to answer following questions:
- Which type of crime is most occurring for each year?  List top 10 crimes for each year.
- Which locations are most likely for a crime to happen?  List top 10 locations.
- Are there certain high crime rate locations for certain crime types?

Answer:
```sql
select yr, primary_type, count from
(
    select yr, primary_type, count(*) as count from crime_parquet
    where yr = 2016
    group by yr, primary_type
    order by count desc
    limit 5
) a
union
select yr, primary_type, count from 
(
    select yr, primary_type, count(*) as count from crime_parquet
    where yr = 2017
    group by yr, primary_type
    order by count desc
    limit 5
) b
union
select yr, primary_type, count from 
(
    select yr, primary_type, count(*) as count from crime_parquet
    where yr = 2018
    group by yr, primary_type
    orderby count desc
    limit 5
) c
union
select yr, primary_type, count from 
(
    select yr, primary_type, count(*) as count from crime_parquet
    where yr = 2019
    group by yr, primary_type
    order by count desc
    limit 5
) d
union
select yr, primary_type, count from 
(
    select yr, primary_type, count(*) as count from crime_parquet
    where yr = 2020
    group by yr, primary_type
    order by count desc
    limit 5
) e
order by yr, count desc
;
```

Impala:

```sql
select yr, primary_type, count
from
(
    select yr, primary_type, count(*) as count
    from crime_parquet 
    where yr = 2016
    group by yr, primary_type
    order by count desc
    limit 5 
    union 
    select yr, primary_type, count(*) as count
    from crime_parquet 
    where yr = 2017
    group by yr, primary_type
    order by count desc
    limit 5 
    union 
    select yr, primary_type, count(*) as count
    from crime_parquet 
    where yr = 2018
    group by yr, primary_type
    order by count desc
    limit 5 
    union 
    select yr, primary_type, count(*) as count
    from crime_parquet 
    where yr = 2019
    group by yr, primary_type
    order by count desc
    limit 5 
    union 
    select yr, primary_type, count(*) as count
    from crime_parquet 
    where yr = 2020
    group by yr, primary_type
    order by count desc
    limit 5 
) a
order by yr, count desc;
```

```sql
SELECT loc_desc, count(*)as count
FROM crime_parquet
GROUP BY loc_desc
order by count desc
limit 10;
```

```sql
SELECT loc_desc, primary_type, count(*) as count
FROM crime_parquet
GROUP BY loc_desc, primary_type
order by count desc
limit 10;
```

## 3.Retail_dbdataset:
### In retail_db, there are 6 tables.  Get yourself familiar with their schemas.
- categories
- customers
- departments
- orders
- order_items
- products

### Write queries to answer following questions:
### (1) List all orders with total order_items = 5.
### (2) List customer_id, order_id, order item_count with total order_items = 5
### (3) List customer_fname，customer_id, order_id, order item_count with total order_items = 5(join orders, order_items, customers table)
### (4) List top 10 most popular product categories. (join products, categories,order_items table)
### (5) List top 10 revenue generating products. (join products, orders, order_items table)

Answer:
```sql
--list all orders with total order_items = 5
select order_item_order_id, count(order_item_id) as item_counts
from order_items
group by order_item_order_id
having count(order_item_id) = 5   --OR hive: having item_counts = 5
order by order_item_order_id;
```

```sql
--list customer_id, order_id, order item_count with total order_items = 5
select o.order_customer_id, oi.order_item_order_id, 
count(oi.order_item_id) as item_counts
from order_items oi
inner join orders o
on o.order_id = oi.order_item_order_id
group by o.order_customer_id, oi.order_item_order_id
having count(oi.order_item_id) = 5   --OR hive: having item_counts = 5
order by oi.order_item_order_id;
```
```sql
--list customer_fname，customer_id, order_id, order item_count with total order_items = 5
select c.customer_fname, o.order_customer_id, oi.order_item_order_id, count(oi.order_item_id) as item_counts
from order_items oi
inner join orders o
on o.order_id = oi.order_item_order_id
inner join customers c
on c.customer_id = o.order_customer_id
group by o.order_customer_id, c.customer_fname, oi.order_item_order_id
having count(oi.order_item_id) = 5   --OR hive: having item_counts = 5
order by oi.order_item_order_id;
```
```sql
--Most popular product categories
--Also try this with impala, it will get rejected if the table stats are missing.
/*
Rejected query from pool root.default: 
request memory needed 4.35 GB per node is greater than memory available for admission 4.00 GB of ip-172-31-89-11.ec2.internal:22000. Use the MEM_LIMIT query option to indicate how much memory is required per node.
*/

select c.category_name, count(order_item_quantity) as count
from order_items oi
inner join products p on oi.order_item_product_id = p.product_id
inner join categories c on c.category_id = p.product_category_id
group by c.category_name
order by count desc
limit 10;
```
```sql
--top 10 revenue generating products
select p.product_id, p.product_name, r.revenue
from products p inner join
(
    select oi.order_item_product_id, sum(oi.order_item_subtotal) as revenue
    from order_items oi inner join orders o
    on oi.order_item_order_id = o.order_id
    where o.order_status <> 'CANCELED' and o.order_status <> 'SUSPECTED_FRAUD'
    group by order_item_product_id
) r
on p.product_id = r.order_item_product_id
order by r.revenue desc
limit 10;
```
# StorageIndex_Analyzer

This tool is designed to improve your Oracle Big Data SQL queries with your data in hive. If your queries are slow, it will be very helpful to understand how your data is distributed. The Storage Index Analyzer provides information about how sorted is the data. If the columns that you are querying are unsorted, most likely the storage indexes will have to read more HDFS blocks, therefore it will take more time to resolve the query.
This tool analyzes the table that you want to analyze, and it will tell us how sorted is each column. There is another tool under the order_data folder, which will help to order the data and improve the performance of our queries.

## Requirements


The tool is written in python for Spark 1.6. The current limitations for this tool are the following:
1. Data must be TEXT
2. Hive tables must not be partitioned
3. Table must have maximum 34 columns
4. Current data types supported are: int, double and bigint
## How it works

You must execute the run_report.sh script, and you are required to provide two parameters:
1.  Database.table to analyze
1.	Percentage of data to analyze. If you have a big data set, you can decide to analyze a subset of the data.

#### Example:
``./run_report.sh csv.web_sales 10``

In this example, we are going to analyze the table web_sales, which is under the csv database. We have decided to analyze just the 10% of the data.

## Output

The script creates a table under the database default which it is called result. This table has two columns: column name and value for sorted data. Closer the value is to 100, more sorted the data is. If the value is -1, it means that the value is a constant.
This table will help you to understand if the column that you are querying is a good one.

## Order the data

If you are frequenly quering colums which the value is 0 or close to 0, you must consider ordering the data. Under the order_data folder there is a script which you can customize to reorder the data efficiently. The script is a Spark job which creates a new table using a select all with a order by statement. 

## Example

In the following example you will see how to use the scripts to get performance improvement. 

We have a few TBs table in Hadoop over the table web_sales_csv and a small table in the oracle database called date_dim_orcl. We want to run some join sql over these tables. 

#### First Query
```sql 
SELECT 
       s.ws_ship_date_sk,
       d.d_date as ship_date,
       s.ws_item_sk,
       s.ws_list_price,
       s.ws_sales_price,
       s.ws_ext_discount_amt
FROM web_sales_csv s,  date_dim_orcl d 
WHERE s.ws_sold_date_sk = d.d_date_sk
and ws_sold_date_sk>36930 
and ws_bill_customer_sk =1252144;
```


#### Second Query

```sql
SELECT 
       s.ws_ship_date_sk,
       d.d_date as ship_date,
       s.ws_item_sk,
       s.ws_list_price,
       s.ws_sales_price,
       s.ws_ext_discount_amt
FROM web_sales_csv s,  date_dim_orcl d 
WHERE s.ws_sold_date_sk = d.d_date_sk
and ws_sold_date_sk>36930 
and ws_bill_customer_sk <1252144;
```

#### Third Query
```sql
SELECT s.ws_ship_date_sk,
       d.d_date as ship_date,
       s.ws_item_sk,
       i.i_class,
       i.i_category,
       i.i_color,
       s.ws_list_price,
       s.ws_sales_price,
       s.ws_ext_discount_amt
FROM web_sales_csv s, bds.item_orcl i, bds.date_dim_orcl d 
WHERE ws_ship_date_sk = d.d_date_sk
  AND ws_item_sk = i.i_item_sk
  AND s.ws_item_sk in (114783, 34603); 
  ```


Timing of the queries:

1. First query: 234 seconds
1. Second query: 5 seconds
1. Third query: 244 seconds

These results indicates that the data could be unsorted. Now we are going to run the script to understand our data:
``./run_report.sh csv.web_sales 10``

Once it is over, we have a look into the default.result table and we look for the columns in the queries:

1. ws_ship_date_sk: 94.44 %
1. ws_item_sk: 0 %

We can see that the ws_item_sk has a very bad value, which most likely is making my queries slow. We will use the order_data script to improve the performance. We are going to create a new table with the columns ordered for our queries. We modify the reorder.py with our needs:

```sql
sqlContext.sql("create table csv.web_sales_spark as select * from csv.web_sales order by ws_sold_date_sk,ws_item_sk")
```
Then we can run the script:

``./run_order.sh``

Now we run the queries pointing to the new table with the following results:

1. First query: 2.2 seconds (106x improvement)
1. Sedond query: 1 second (5x improvement)
1. Third query: 3 seconds (81 x improvement)

rom pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext

if __name__ == "__main__":
	
	conf=SparkConf()
	
	
	conf.set("spark.yarn.executor.memoryOverhead","5000")	
	sc=SparkContext(conf=conf)
	sqlContext = HiveContext(sc)
	sqlContext.sql(" create table web_sales_spark as select * from csv.web_sales order by ws_sold_date_sk,ws_item_sk")

	sc.stop()

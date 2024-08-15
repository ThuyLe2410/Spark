from pyspark import SparkConf, SparkContext, SparkFiles, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, trim, lower, when, isnull


# 1. Initialize SparkConf and SparkContext with custom serializer
conf = SparkConf().setAppName("Sales Analysis Project").setMaster("local")
sc = SparkContext(conf=conf)

# 2. Add the csv file to SparkFiles
sc.addFile("/Users/moon/workspace/na/spark/SalesJan2009.csv")
file_path = SparkFiles.get("SalesJan2009.csv")

# 3. Initialize SparkSession for DataFrame operations
spark = SparkSession.builder.appName("Sales Analysis with DataFrames").getOrCreate()

# 4. Load csv data into df
sales_df = spark.read.csv(file_path, header=True, inferSchema=True)
print(sales_df.show(5))

# 5. Preprocessing data
print(sales_df.printSchema())
## Convert price from string to numeric type
sales_df = sales_df.withColumn("Price", col("Price").cast("double"))
sales_df = sales_df.filter((col("Price")>=0))
sales_df.describe().show()
sales_df = sales_df.withColumn("Product", trim(lower(col("Product"))))
sales_df.show()
## Handle missing data: check for nulls and fill them with 0
sales_df = sales_df.withColumn("Price", when(isnull(col("Price")),0).otherwise(col("Price")))

## Aggregation: calculate average price and total revenue per product
agg_df = sales_df.groupBy("Product").agg(
    avg("Price").alias("Avg Price"),
    sum(col("Price")).alias("Total Revenue")
).orderBy(col("Product"))
agg_df.show()

## Pivot data to see the total revenue for each product by country
pivot_df = sales_df.groupBy("Country").pivot("Product").sum("Price")
pivot_df.show(5)

# 6. SQL Queries using spark.sql
sales_df.createOrReplaceTempView("sales")

## Query Avg Price, Total Sales, Total Revenue,  by product using SQL
sql_query = """
            SELECT Product, avg(Price) as AvgPrice,
            count(*) as TotalSales,
            sum(Price) as TotalRevenue
            FROM sales 
            GROUP BY Product
            ORDER BY TotalRevenue DESC"""
result_df = spark.sql(sql_query)
result_df.show()

## Query Avg Price, Total Sales, Total Revenue,  by Country using SQL
sql_query = """
            SELECT Country, avg(Price) as AvgPrice,
            count(*) as TotalSales,
            sum(Price) as TotalRevenue
            FROM sales 
            GROUP BY Country
            ORDER BY TotalRevenue DESC"""
result_df = spark.sql(sql_query)
result_df.show()

## Query Avg Price, Total Sales, Total Revenue,  by Payment_type using SQL
sql_query = """
            SELECT Payment_Type,
            count(*) as TotalSales,
            sum(Price) as TotalRevenue
            FROM sales 
            GROUP BY Payment_Type
            ORDER BY TotalRevenue DESC"""
result_df = spark.sql(sql_query)
result_df.show()

spark.stop()











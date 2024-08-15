import pyspark

session = pyspark.sql.SparkSession.builder.appName("SalesJan2009").getOrCreate()

df = session.read.options(header=True).csv("SalesJan2009.csv")

# Display first 5 rows
df.show(5)

sales_table = "sales"

df.createOrReplaceTempView(sales_table)

query = f"SELECT DISTINCT Product FROM {sales_table}"
print(f"Result for query '{query}'")
session.sql(query).show()

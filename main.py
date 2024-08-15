import pyspark

spark = pyspark.sql.SparkSession.builder.appName("SalesJan2009").getOrCreate()

# Read csv data into a Dataframe
df = spark.read.options(header=True).csv("SalesJan2009.csv")

# Display the first 5 rows
df.show(5)

SALES_TABLE = "sales"
df.createOrReplaceTempView(SALES_TABLE)

query = f"SELECT DISTINCT Product FROM {SALES_TABLE}"
print(f"Result for query '{query}'")
spark.sql(query).show()

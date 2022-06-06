from pyspark.sql import SparkSession
from pyspark.sql import types as t


spark = SparkSession.builder.master("local[*]").appName("lab9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


schema = t.StructType() \
      .add("step", t.IntegerType(), True) \
      .add("type", t.StringType(), True) \
      .add("amount", t.DoubleType(), True) \
      .add("nameOrig", t.StringType(), True) \
      .add("oldbalanceOrg",t.DoubleType(), True) \
      .add("newbalanceOrig",t.DoubleType(), True) \
      .add("nameDest", t.StringType(), True) \
      .add("oldbalanceDest",t.DoubleType(), True) \
      .add("newbalanceDest",t.DoubleType(), True) \
      .add("isFraud",t.IntegerType(), False) \
      .add("isFlaggedFraud",t.IntegerType(), False) \
      
fraud_df = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("./data.csv")

fraud_df.printSchema()
fraud_df.show(truncate=False)

print("Number rows: ", fraud_df.count())

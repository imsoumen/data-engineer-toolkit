from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when, lag, lead, coalesce, lit

spark = SparkSession.builder.appName("SwapSeats").getOrCreate()

# Sample data
data = [
    (1, 'Amit'),
    (2, 'Deepa'),
    (3, 'Rohit'),
    (4, 'Anjali'),
    (5, 'Neha'),
    (6, 'Sanjay'),
    (7, 'Priya')
]

df = spark.createDataFrame(data, ["id", "student"])

df = df.withColumn("rn", row_number().over(Window.orderBy("id")))

df_output = df.withColumn("new_id",
                          when(
                                col("rn") % 2 == 0,
                                    lag(col("id")).over(Window.orderBy("rn"))  
                            ).otherwise(
                                coalesce(lead(col("id")).over(Window.orderBy("rn")), col("id"))
                            )
                          )

df_output.select("id","student","new_id").show()